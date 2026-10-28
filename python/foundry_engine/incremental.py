"""Correctness-first incremental execution over an SQLite transaction log.

Workers read immutable input versions, return a complete snapshot on first run
or a keyed OutputDelta later, and publish output, audit record, semantic version,
and watermarks in one ACID transaction. Joins and aggregates must emit every
output key they affect. The local SQLite store is a correctness reference, not
a distributed production-scale compute backend.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Iterable, Mapping, Sequence


class IncrementalError(Exception):
    """Typed failure; output and checkpoints are not partially advanced."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class InputSlice:
    dataset: str
    tx_id: int
    transaction_type: str
    current: tuple[Mapping[str, Any], ...]
    previous: tuple[Mapping[str, Any], ...]
    added: tuple[Mapping[str, Any], ...]
    removed: tuple[Mapping[str, Any], ...]
    modified: tuple[Mapping[str, Any], ...]

    def dataframe(self, mode: str) -> tuple[Mapping[str, Any], ...]:
        if mode not in {"added", "removed", "modified", "current", "previous"}:
            raise IncrementalError("BAD_READ_MODE", f"unsupported dataframe read mode {mode!r}")
        return getattr(self, mode)


@dataclass(frozen=True)
class OutputDelta:
    """Incremental output changes, keyed by the transform's declared output key."""

    upserts: Iterable[Mapping[str, Any]] = ()
    deletes: Iterable[Any] = ()


@dataclass(frozen=True)
class TransformSpec:
    name: str
    inputs: tuple[str, ...]
    output: str
    key: str
    output_schema: Mapping[str, str]
    semantic_version: int = 1
    require_incremental: bool = False
    strict_append: bool = False
    snapshot_inputs: tuple[str, ...] = ()
    allow_retention: bool = False
    input_keys: tuple[tuple[str, str], ...] = ()

    def validate(self) -> dict[str, str]:
        if any(not isinstance(value, str) or not value.strip() for value in (self.name, self.output, self.key)):
            raise IncrementalError("BAD_TRANSFORM", "transform name, output and stable key are required")
        if not isinstance(self.inputs, tuple) or not self.inputs or any(
            not isinstance(name, str) or not name.strip() for name in self.inputs
        ):
            raise IncrementalError("BAD_TRANSFORM", "inputs must be a tuple of nonempty dataset names")
        if len(set(self.inputs)) != len(self.inputs) or self.output in self.inputs:
            raise IncrementalError("BAD_TRANSFORM", "inputs must be unique and exclude the output")
        if not isinstance(self.snapshot_inputs, tuple) or any(
            not isinstance(name, str) or not name.strip() for name in self.snapshot_inputs
        ):
            raise IncrementalError("BAD_TRANSFORM", "snapshot_inputs must be a tuple of nonempty dataset names")
        if len(set(self.snapshot_inputs)) != len(self.snapshot_inputs) or not set(self.snapshot_inputs).issubset(self.inputs):
            raise IncrementalError("BAD_TRANSFORM", "snapshot inputs must be unique declared inputs")
        if (
            not isinstance(self.semantic_version, int)
            or isinstance(self.semantic_version, bool)
            or not 1 <= self.semantic_version <= 2**63 - 1
        ):
            raise IncrementalError("BAD_TRANSFORM", "semantic version must be a positive SQLite integer")
        if not isinstance(self.require_incremental, bool) or not isinstance(self.strict_append, bool) or not isinstance(self.allow_retention, bool):
            raise IncrementalError("BAD_TRANSFORM", "require_incremental, strict_append and allow_retention must be booleans")
        schema = _validate_schema(self.output_schema, self.output)
        if not schema or self.key not in schema or schema[self.key] not in {"string", "integer", "number"}:
            raise IncrementalError("BAD_TRANSFORM_SCHEMA", "output schema must contain a scalar primary key")
        if not isinstance(self.input_keys, tuple) or any(
            not isinstance(entry, tuple)
            or len(entry) != 2
            or any(not isinstance(value, str) or not value.strip() for value in entry)
            for entry in self.input_keys
        ):
            raise IncrementalError("BAD_TRANSFORM", "input_keys must be tuples of nonempty dataset and key names")
        if self.input_keys:
            names = [name for name, _ in self.input_keys]
            if len(names) != len(set(names)) or set(names) != set(self.inputs):
                raise IncrementalError("BAD_TRANSFORM", "input_keys must map every declared input exactly once")
        return schema

    def input_key(self, name: str) -> str:
        return dict(self.input_keys).get(name, self.key)

    def fingerprint(
        self,
        schema: Mapping[str, str],
        input_schemas: Mapping[str, Mapping[str, str]],
    ) -> str:
        contract = {
            "name": self.name,
            "inputs": list(self.inputs),
            "input_keys": {name: self.input_key(name) for name in self.inputs},
            "input_schemas": {name: dict(input_schemas[name]) for name in self.inputs},
            "output": self.output,
            "key": self.key,
            "schema": dict(schema),
            "snapshot_inputs": list(self.snapshot_inputs),
            "require_incremental": self.require_incremental,
            "strict_append": self.strict_append,
            "allow_retention": self.allow_retention,
        }
        return hashlib.sha256(_json(contract).encode()).hexdigest()


@dataclass(frozen=True)
class _Dataset:
    name: str
    tx_id: int
    operation: str
    rows: tuple[dict[str, Any], ...]
    schema: dict[str, str]
    restricted_view: bool
    writeback: bool


@dataclass(frozen=True)
class _Snapshot:
    inputs: Mapping[str, _Dataset]
    previous: Mapping[str, tuple[dict[str, Any], ...]]
    output: _Dataset | None
    state: Mapping[str, Any] | None
    watermarks: Mapping[str, int]


class DatasetStore:
    """SQLite local snapshot store used for correctness tests and prototypes."""

    def __init__(self, path: str | Path):
        raw = str(path)
        self._anchor: sqlite3.Connection | None = None
        if raw == ":memory:":
            self.path = f"file:foundry-{uuid.uuid4().hex}?mode=memory&cache=shared"
            self._uri = True
            self._anchor = sqlite3.connect(self.path, uri=True, isolation_level=None)
        else:
            self.path = raw
            self._uri = raw.startswith("file:")
            Path(raw).parent.mkdir(parents=True, exist_ok=True)
        db = self.connect()
        try:
            if not self._uri:
                db.execute("PRAGMA journal_mode = WAL")
            db.executescript(
                """
                PRAGMA foreign_keys=ON;
                CREATE TABLE IF NOT EXISTS datasets(name TEXT PRIMARY KEY, schema_json TEXT NOT NULL, restricted_view INTEGER NOT NULL DEFAULT 0, writeback INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS transactions(id INTEGER PRIMARY KEY AUTOINCREMENT, dataset TEXT NOT NULL REFERENCES datasets(name), operation TEXT NOT NULL CHECK(operation IN ('SNAPSHOT','APPEND','UPDATE')), rows_json TEXT NOT NULL, committed_at REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS transactions_tip ON transactions(dataset,id DESC);
                CREATE TRIGGER IF NOT EXISTS transactions_immutable_update BEFORE UPDATE ON transactions BEGIN SELECT RAISE(ABORT,'transaction versions are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS transactions_immutable_delete BEFORE DELETE ON transactions BEGIN SELECT RAISE(ABORT,'transaction versions are immutable'); END;
                CREATE TABLE IF NOT EXISTS transform_state(name TEXT PRIMARY KEY, semantic_version INTEGER NOT NULL, contract_hash TEXT NOT NULL, output_tx INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS transform_watermarks(transform TEXT NOT NULL, input TEXT NOT NULL, tx INTEGER NOT NULL, PRIMARY KEY(transform,input));
                CREATE TABLE IF NOT EXISTS transform_runs(id INTEGER PRIMARY KEY AUTOINCREMENT, transform TEXT NOT NULL, mode TEXT NOT NULL, status TEXT NOT NULL, semantic_version INTEGER NOT NULL, slices_json TEXT NOT NULL, watermarks_json TEXT NOT NULL, output_tx INTEGER, rows_out INTEGER NOT NULL, error_code TEXT, error_message TEXT, committed_at REAL NOT NULL);
                CREATE TRIGGER IF NOT EXISTS transform_runs_immutable_update BEFORE UPDATE ON transform_runs BEGIN SELECT RAISE(ABORT,'transform run history is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS transform_runs_immutable_delete BEFORE DELETE ON transform_runs BEGIN SELECT RAISE(ABORT,'transform run history is immutable'); END;
                """
            )
        finally:
            db.close()

    def connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None, uri=self._uri)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=30000")
        db.execute("PRAGMA synchronous=FULL")
        return db

    def close(self) -> None:
        if self._anchor is not None:
            self._anchor.close()
            self._anchor = None

    def register_dataset(self, name: str, rows: Iterable[Mapping[str, Any]], schema: Mapping[str, str], *, restricted_view: bool = False, writeback: bool = False) -> int:
        if not isinstance(name, str) or not name.strip():
            raise IncrementalError("BAD_DATASET", "dataset name is required")
        if not isinstance(restricted_view, bool) or not isinstance(writeback, bool):
            raise IncrementalError("BAD_DATASET", "dataset access flags must be booleans")
        schema = _validate_schema(schema, name)
        materialized = _validate_rows(rows, schema, name)
        db = self.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM datasets WHERE name=?", (name,)).fetchone():
                raise IncrementalError("ALREADY_EXISTS", f"dataset {name!r} already exists")
            db.execute("INSERT INTO datasets(name,schema_json,restricted_view,writeback) VALUES(?,?,?,?)", (name, _json(schema), int(restricted_view), int(writeback)))
            tx = db.execute("INSERT INTO transactions(dataset,operation,rows_json,committed_at) VALUES(?,'SNAPSHOT',?,?)", (name, _json(materialized), time.time())).lastrowid
            db.commit()
            return int(tx)
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def commit_dataset(self, name: str, rows: Iterable[Mapping[str, Any]], *, operation: str = "SNAPSHOT") -> int:
        if not isinstance(name, str) or not name.strip():
            raise IncrementalError("BAD_DATASET", "dataset name is required")
        if not isinstance(operation, str) or operation not in {"SNAPSHOT", "APPEND", "UPDATE"}:
            raise IncrementalError("BAD_TRANSACTION", f"unsupported transaction {operation!r}")
        db = self.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            current = _read_dataset(db, name)
            materialized = _validate_rows(rows, current.schema, name)
            if operation == "APPEND":
                materialized = [*current.rows, *materialized]
            tx = db.execute("INSERT INTO transactions(dataset,operation,rows_json,committed_at) VALUES(?,?,?,?)", (name, operation, _json(materialized), time.time())).lastrowid
            db.commit()
            return int(tx)
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def read_dataset(self, name: str) -> list[dict[str, Any]]:
        db = self.connect()
        try:
            db.execute("BEGIN")
            result = list(_read_dataset(db, name).rows)
            db.commit()
            return result
        finally:
            db.close()

    def watermarks(self, transform: str) -> dict[str, int]:
        db = self.connect()
        try:
            return {row["input"]: int(row["tx"]) for row in db.execute("SELECT input,tx FROM transform_watermarks WHERE transform=?", (transform,))}
        finally:
            db.close()

    def output_history(self, name: str) -> list[dict[str, Any]]:
        db = self.connect()
        try:
            return [{"tx_id": int(r["id"]), "operation": r["operation"], "rows": json.loads(r["rows_json"])} for r in db.execute("SELECT id,operation,rows_json FROM transactions WHERE dataset=? ORDER BY id", (name,))]
        finally:
            db.close()

    def run_history(self, transform: str) -> list[dict[str, Any]]:
        db = self.connect()
        try:
            return [{**dict(r), "read_slices": json.loads(r["slices_json"]), "input_watermarks": json.loads(r["watermarks_json"])} for r in db.execute("SELECT * FROM transform_runs WHERE transform=? ORDER BY id", (transform,))]
        finally:
            db.close()


class IncrementalRunner:
    def __init__(self, store: DatasetStore):
        self.store = store

    def run(self, spec: TransformSpec, transform: Callable[[Mapping[str, InputSlice], str], Iterable[Mapping[str, Any]] | OutputDelta], *, force_snapshot: bool = False) -> dict[str, Any]:
        try:
            if not isinstance(spec, TransformSpec):
                raise IncrementalError("BAD_TRANSFORM", "spec must be a TransformSpec")
            if not isinstance(force_snapshot, bool):
                raise IncrementalError("BAD_TRANSFORM", "force_snapshot must be a boolean")
            return self._run(spec, transform, force_snapshot)
        except IncrementalError as error:
            self._record_failure(spec, error.code)
            raise
        except Exception:
            self._record_failure(spec, "TRANSFORM_EXECUTION_FAILED")
            raise

    def _run(self, spec: TransformSpec, transform: Callable, force_snapshot: bool) -> dict[str, Any]:
        schema = spec.validate()
        if not callable(transform):
            raise IncrementalError("BAD_TRANSFORM", "transform must be callable")
        snap = self._read_snapshot(spec)
        fingerprint = spec.fingerprint(schema, {name: dataset.schema for name, dataset in snap.inputs.items()})
        state = snap.state
        initial = state is None
        version_changed = bool(state and int(state["semantic_version"]) != spec.semantic_version)
        if state and snap.output is None:
            raise IncrementalError("OUTPUT_MISSING", f"owned output {spec.output!r} is missing")
        if state and snap.output.tx_id != int(state["output_tx"]):
            raise IncrementalError("OUTPUT_CONFLICT", "output advanced outside this transform; use an explicit versioned rebuild after resolving ownership")
        if state and spec.semantic_version < int(state["semantic_version"]):
            raise IncrementalError("SEMANTIC_VERSION_ROLLBACK", "semantic version cannot move backwards")
        if state and state["contract_hash"] != fingerprint and not version_changed:
            raise IncrementalError("INCREMENTAL_CONTRACT_CHANGED", "transform contract changed without semantic-version bump")
        if state and snap.output is not None and snap.output.schema != schema and not version_changed:
            raise IncrementalError("OUTPUT_SCHEMA_CONFLICT", "output schema changes require a semantic-version snapshot rebuild")
        if initial and snap.output is not None:
            raise IncrementalError("OUTPUT_CONFLICT", "refusing to adopt an existing output")

        full = initial or version_changed or force_snapshot
        if spec.require_incremental and not initial and force_snapshot and not version_changed:
            raise IncrementalError("REQUIRE_INCREMENTAL", "transform requires incremental execution")
        for name, dataset in snap.inputs.items():
            _require_key(dataset, spec.input_key(name))

        slices = MappingProxyType(_make_slices(spec, snap, include_deltas=initial or not full))
        advanced = {name for name, item in snap.inputs.items() if snap.watermarks.get(name) != item.tx_id}
        changed = {name: item for name, item in slices.items() if name not in spec.snapshot_inputs and (item.added or item.modified or item.removed)}
        snapshot_changed = sorted(name for name in spec.snapshot_inputs if name in advanced)
        if not full and snapshot_changed:
            raise IncrementalError("SNAPSHOT_INPUT_CHANGED", f"snapshot-only inputs advanced: {', '.join(snapshot_changed)}")
        if not full and not advanced:
            if snap.output is None:
                raise IncrementalError("BAD_TRANSFORM", "unchanged inputs require an existing output")
            counts = _slice_counts(slices)
            watermarks = {name: item.tx_id for name, item in snap.inputs.items()}
            run_id = self._commit_noop(spec, snap, fingerprint, watermarks, counts, len(snap.output.rows))
            return {"ok": True, "status": "skipped", "mode": "incremental", "reason": "inputs unchanged", "output_operation": "NONE", "output_tx_id": snap.output.tx_id, "rows_out": len(snap.output.rows), "run_id": run_id, "read_slices": counts, "input_watermarks": watermarks}
        if not full and not spec.allow_retention and any(item.removed for item in changed.values()):
            raise IncrementalError("RETENTION_REQUIRED", "input removals require allow_retention")
        if not full and spec.strict_append:
            for name in advanced:
                item = slices[name]
                if item.transaction_type != "APPEND" or item.modified or item.removed:
                    raise IncrementalError("STRICT_APPEND_VIOLATION", f"input {name!r} was not a pure append")

        mode = "snapshot" if full else "incremental"
        proposed = transform(slices, mode)
        if full:
            if isinstance(proposed, OutputDelta):
                raise IncrementalError("BAD_TRANSFORM_RESULT", "snapshot transform must return complete rows")
            output = _validate_rows(proposed, schema, spec.output)
            output_map = _row_map(output, spec.key, spec.output)
            output = list(output_map.values())
        else:
            if not isinstance(proposed, OutputDelta):
                raise IncrementalError("BAD_TRANSFORM_RESULT", "incremental transform must return OutputDelta")
            upserts = _validate_rows(proposed.upserts, schema, spec.output)
            upsert_map = _row_map(upserts, spec.key, spec.output)
            if isinstance(proposed.deletes, (str, bytes, Mapping)):
                raise IncrementalError("BAD_TRANSFORM_RESULT", "output deletes must be an iterable of scalar keys")
            delete_values = list(proposed.deletes)
            for value in delete_values:
                _validate_value(value, schema[spec.key], spec.output + "." + spec.key, 0)
            delete_keys = [_key(value) for value in delete_values]
            if len(delete_keys) != len(set(delete_keys)):
                raise IncrementalError("INCREMENTAL_DUPLICATE_KEY", "duplicate output delete key")
            if set(delete_keys) & set(upsert_map):
                raise IncrementalError("BAD_TRANSFORM_RESULT", "output key is both deleted and upserted")
            if not changed and (upsert_map or delete_keys):
                raise IncrementalError("BAD_TRANSFORM_RESULT", "transform emitted output changes for unchanged input rows")
            output_map = _row_map(snap.output.rows if snap.output else (), spec.key, spec.output)
            if spec.strict_append and (delete_keys or set(upsert_map) & set(output_map)):
                raise IncrementalError("STRICT_APPEND_VIOLATION", "strict append cannot delete or modify output keys")
            if set(delete_keys) - set(output_map):
                raise IncrementalError("BAD_TRANSFORM_RESULT", "output delta deletes a key that is not currently present")
            output_changed = bool(delete_keys) or any(
                key not in output_map or _json(output_map[key]) != _json(row)
                for key, row in upsert_map.items()
            )
            for key in delete_keys:
                output_map.pop(key)
            output_map.update(upsert_map)
            output = list(output_map.values())

        output = _validate_rows(output, schema, spec.output)
        counts = _slice_counts(slices)
        watermarks = {name: item.tx_id for name, item in snap.inputs.items()}
        if not full and (not changed or not output_changed):
            if snap.output is None:
                raise IncrementalError("BAD_TRANSFORM", "no-change incremental run requires existing output")
            run_id = self._commit_noop(spec, snap, fingerprint, watermarks, counts, len(output))
            reason = "input transaction advanced without output changes" if changed else "input transaction changed but row set did not"
            return {"ok": True, "status": "skipped", "mode": "incremental", "reason": reason, "output_operation": "NONE", "output_tx_id": snap.output.tx_id, "rows_out": len(output), "run_id": run_id, "read_slices": counts, "input_watermarks": watermarks}
        operation = "SNAPSHOT" if full else ("APPEND" if spec.strict_append else "UPDATE")
        return self._commit(spec, snap, fingerprint, schema, watermarks, counts, output, operation, mode)

    def _read_snapshot(self, spec: TransformSpec) -> _Snapshot:
        db = self.store.connect()
        try:
            db.execute("BEGIN")
            inputs = {name: _read_dataset(db, name) for name in spec.inputs}
            output_exists = db.execute("SELECT 1 FROM datasets WHERE name=?", (spec.output,)).fetchone() is not None
            output = _read_dataset(db, spec.output) if output_exists else None
            row = db.execute("SELECT * FROM transform_state WHERE name=?", (spec.name,)).fetchone()
            state = dict(row) if row else None
            marks = {r["input"]: int(r["tx"]) for r in db.execute("SELECT input,tx FROM transform_watermarks WHERE transform=?", (spec.name,))}
            if state is not None and set(marks) != set(spec.inputs):
                raise IncrementalError("CHECKPOINT_CORRUPTION", "stored watermarks do not match the transform input contract")
            previous = {name: (() if name not in marks else _rows_at(db, name, marks[name])) for name in spec.inputs}
            for name, item in inputs.items():
                if item.restricted_view:
                    raise IncrementalError("RESTRICTED_VIEW_BATCH", f"restricted view {name!r} cannot feed batch transform")
            if output and (output.restricted_view or output.writeback):
                raise IncrementalError("BAD_TRANSFORM_OUTPUT", "cannot write to restricted view or writeback dataset")
            db.commit()
            return _Snapshot(inputs, previous, output, state, marks)
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def _record_failure(self, spec: TransformSpec, code: str) -> None:
        name = getattr(spec, "name", None)
        if not isinstance(name, str) or not name:
            return
        semantic_version = getattr(spec, "semantic_version", 0)
        version = semantic_version if isinstance(semantic_version, int) and not isinstance(semantic_version, bool) else 0
        try:
            db = self.store.connect()
            try:
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    "INSERT INTO transform_runs(transform,mode,status,semantic_version,slices_json,watermarks_json,output_tx,rows_out,error_code,error_message,committed_at) VALUES(?,'unknown','failed',?,'{}','{}',NULL,0,?,NULL,?)",
                    (name, version, code, time.time()),
                )
                db.commit()
            finally:
                db.close()
        except (sqlite3.Error, TypeError, ValueError):
            return

    def _assert_unchanged(self, db: sqlite3.Connection, spec: TransformSpec, snap: _Snapshot) -> None:
        for name, item in snap.inputs.items():
            catalog = db.execute(
                "SELECT schema_json,restricted_view,writeback FROM datasets WHERE name=?",
                (name,),
            ).fetchone()
            try:
                current_schema = json.loads(catalog["schema_json"]) if catalog is not None else None
            except json.JSONDecodeError as error:
                raise IncrementalError("INPUT_CONFLICT", f"input {name!r} schema metadata is corrupt") from error
            if catalog is None or (
                current_schema != item.schema
                or bool(catalog["restricted_view"]) != item.restricted_view
                or bool(catalog["writeback"]) != item.writeback
            ):
                raise IncrementalError("INPUT_CONFLICT", f"input {name!r} metadata changed during compute")
            if _latest_tx(db, name) != item.tx_id:
                raise IncrementalError("INPUT_CONFLICT", f"input {name!r} advanced during compute")
        exists = db.execute("SELECT 1 FROM datasets WHERE name=?", (spec.output,)).fetchone() is not None
        if exists != (snap.output is not None):
            raise IncrementalError("OUTPUT_CONFLICT", "output catalog changed during compute")
        if snap.output:
            catalog = db.execute(
                "SELECT schema_json,restricted_view,writeback FROM datasets WHERE name=?",
                (spec.output,),
            ).fetchone()
            try:
                output_schema = json.loads(catalog["schema_json"]) if catalog is not None else None
            except json.JSONDecodeError as error:
                raise IncrementalError("OUTPUT_CONFLICT", "output schema metadata is corrupt") from error
            if catalog is None or (
                output_schema != snap.output.schema
                or bool(catalog["restricted_view"]) != snap.output.restricted_view
                or bool(catalog["writeback"]) != snap.output.writeback
            ):
                raise IncrementalError("OUTPUT_CONFLICT", "output metadata changed during compute")
            if _latest_tx(db, spec.output) != snap.output.tx_id:
                raise IncrementalError("OUTPUT_CONFLICT", "output advanced during compute")
        row = db.execute("SELECT * FROM transform_state WHERE name=?", (spec.name,)).fetchone()
        if (dict(row) if row else None) != snap.state:
            raise IncrementalError("RUN_STATE_CONFLICT", "transform concurrently committed")
        marks = {r["input"]: int(r["tx"]) for r in db.execute("SELECT input,tx FROM transform_watermarks WHERE transform=?", (spec.name,))}
        if marks != dict(snap.watermarks):
            raise IncrementalError("RUN_STATE_CONFLICT", "transform checkpoints changed during compute")

    def _commit_noop(self, spec: TransformSpec, snap: _Snapshot, fingerprint: str, marks: Mapping[str, int], counts: Mapping[str, Any], rows_out: int) -> int:
        db = self.store.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            self._assert_unchanged(db, spec, snap)
            if snap.output is None:
                raise IncrementalError("BAD_TRANSFORM", "no-op requires existing output")
            _write_state(db, spec, fingerprint, snap.output.tx_id, marks)
            run_id = _write_success(db, spec, "incremental", counts, marks, snap.output.tx_id, rows_out)
            db.commit()
            return run_id
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def _commit(self, spec: TransformSpec, snap: _Snapshot, fingerprint: str, schema: Mapping[str, str], marks: Mapping[str, int], counts: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], operation: str, mode: str) -> dict[str, Any]:
        db = self.store.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            self._assert_unchanged(db, spec, snap)
            record = db.execute("SELECT schema_json FROM datasets WHERE name=?", (spec.output,)).fetchone()
            if record is None:
                db.execute("INSERT INTO datasets(name,schema_json) VALUES(?,?)", (spec.output, _json(schema)))
            elif json.loads(record["schema_json"]) != dict(schema):
                version_changed = snap.state is not None and int(snap.state["semantic_version"]) != spec.semantic_version
                if mode != "snapshot" or not version_changed:
                    raise IncrementalError("BAD_TRANSFORM_SCHEMA", f"output {spec.output!r} schema changes require a semantic-version snapshot rebuild")
                db.execute("UPDATE datasets SET schema_json=? WHERE name=?", (_json(schema), spec.output))
            output_tx = int(db.execute("INSERT INTO transactions(dataset,operation,rows_json,committed_at) VALUES(?,?,?,?)", (spec.output, operation, _json(rows), time.time())).lastrowid)
            _write_state(db, spec, fingerprint, output_tx, marks)
            run_id = _write_success(db, spec, mode, counts, marks, output_tx, len(rows))
            db.commit()
            reasons = []
            if snap.state is None:
                reasons.append("output has never been built")
            elif int(snap.state["semantic_version"]) != spec.semantic_version:
                reasons.append("semantic version changed")
            elif mode == "snapshot":
                reasons.append("snapshot explicitly requested")
            return {"ok": True, "status": "success", "mode": mode, "reasons": reasons, "output_operation": operation, "output_tx_id": output_tx, "rows_out": len(rows), "run_id": run_id, "read_slices": dict(counts), "input_watermarks": dict(marks)}
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()


def filter_map_transform(input_name: str, input_key: str, output_key: str, predicate: Callable[[Mapping[str, Any]], bool], project: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None):
    """Safe example transform for one input, including filter flips and deletes."""
    projection = project or (lambda row: dict(row))

    def run(slices: Mapping[str, InputSlice], mode: str):
        if set(slices) != {input_name}:
            raise IncrementalError("BAD_TRANSFORM", "filter-map transform expects exactly its declared input")
        source = slices[input_name]
        if mode == "snapshot":
            return [dict(projection(row)) for row in source.current if predicate(row)]
        previous = {_key(row[input_key]): row for row in source.previous}
        upserts, deletes = [], []
        for row in (*source.added, *source.modified):
            if predicate(row):
                upserts.append(dict(projection(row)))
        for row in source.removed:
            if predicate(row):
                deletes.append(projection(row)[output_key])
        for row in source.modified:
            before = previous[_key(row[input_key])]
            old = projection(before) if predicate(before) else None
            new = projection(row) if predicate(row) else None
            if old is not None and (new is None or _key(old[output_key]) != _key(new[output_key])):
                deletes.append(old[output_key])
        return OutputDelta(upserts, deletes)

    return run


def _latest_tx(db: sqlite3.Connection, dataset: str) -> int | None:
    row = db.execute("SELECT id FROM transactions WHERE dataset=? ORDER BY id DESC LIMIT 1", (dataset,)).fetchone()
    return int(row["id"]) if row else None


def _read_dataset(db: sqlite3.Connection, name: str) -> _Dataset:
    catalog = db.execute("SELECT * FROM datasets WHERE name=?", (name,)).fetchone()
    if catalog is None:
        raise IncrementalError("NOT_FOUND", f"dataset {name!r} not found")
    tx = db.execute("SELECT id,operation,rows_json FROM transactions WHERE dataset=? ORDER BY id DESC LIMIT 1", (name,)).fetchone()
    if tx is None:
        raise IncrementalError("BAD_DATASET", f"dataset {name!r} has no committed snapshot")
    try:
        schema = _validate_schema(json.loads(catalog["schema_json"]), name)
        rows = _validate_rows(json.loads(tx["rows_json"]), schema, name)
    except (IncrementalError, json.JSONDecodeError, TypeError, ValueError, RecursionError) as error:
        raise IncrementalError("CORRUPT_DATASET", f"stored dataset {name!r} failed schema or row validation") from error
    return _Dataset(
        name,
        int(tx["id"]),
        tx["operation"],
        tuple(rows),
        schema,
        bool(catalog["restricted_view"]),
        bool(catalog["writeback"]),
    )


def _rows_at(db: sqlite3.Connection, name: str, tx: int) -> tuple[dict[str, Any], ...]:
    row = db.execute("SELECT rows_json FROM transactions WHERE dataset=? AND id=?", (name, tx)).fetchone()
    if row is None:
        raise IncrementalError("WATERMARK_LOST", f"checkpoint transaction for {name!r} is unavailable")
    catalog = db.execute("SELECT schema_json FROM datasets WHERE name=?", (name,)).fetchone()
    if catalog is None:
        raise IncrementalError("WATERMARK_LOST", f"checkpoint dataset {name!r} is unavailable")
    try:
        schema = _validate_schema(json.loads(catalog["schema_json"]), name)
        rows = _validate_rows(json.loads(row["rows_json"]), schema, name)
    except (IncrementalError, json.JSONDecodeError, TypeError, ValueError, RecursionError) as error:
        raise IncrementalError("WATERMARK_CORRUPT", f"checkpoint rows for {name!r} failed schema or row validation") from error
    return tuple(rows)


def _freeze_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze_value(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_value(item) for item in value)
    return value


def _thaw_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_value(item) for item in value]
    return value


def _make_slices(
    spec: TransformSpec,
    snap: _Snapshot,
    *,
    include_deltas: bool,
) -> dict[str, InputSlice]:
    def freeze(rows: Sequence[Mapping[str, Any]]) -> tuple[Mapping[str, Any], ...]:
        return tuple(_freeze_value(row) for row in rows)

    result = {}
    for name, dataset in snap.inputs.items():
        previous = snap.previous[name]
        if include_deltas:
            added, removed, modified = _diff(previous, dataset.rows, spec.input_key(name))
        else:
            # A rebuild consumes the complete current snapshot; applying deltas
            # from an old key/schema contract would be undefined.
            added, removed, modified = (), (), ()
        result[name] = InputSlice(name, dataset.tx_id, dataset.operation, freeze(dataset.rows), freeze(previous), freeze(added), freeze(removed), freeze(modified))
    return result


def _diff(previous: Sequence[Mapping[str, Any]], current: Sequence[Mapping[str, Any]], key: str):
    old, new = _row_map(previous, key, "previous input"), _row_map(current, key, "current input")
    return (
        [row for identity, row in new.items() if identity not in old],
        [row for identity, row in old.items() if identity not in new],
        [row for identity, row in new.items() if identity in old and _json(row) != _json(old[identity])],
    )


def _row_map(rows: Sequence[Mapping[str, Any]], key: str, label: str) -> dict[str, dict[str, Any]]:
    result = {}
    for row in rows:
        value = row.get(key)
        identity = _key(value)
        if identity in result:
            raise IncrementalError("INCREMENTAL_DUPLICATE_KEY", f"duplicate key in {label}")
        result[identity] = dict(row)
    return result


def _require_key(dataset: _Dataset, key: str) -> None:
    if dataset.schema.get(key) not in {"string", "integer", "number"}:
        raise IncrementalError("BAD_TRANSFORM_SCHEMA", f"input key {dataset.name}.{key} is absent or non-scalar")


def _validate_schema(schema: Mapping[str, str], label: str) -> dict[str, str]:
    if not isinstance(schema, Mapping):
        raise IncrementalError("BAD_TRANSFORM_SCHEMA", f"schema for {label!r} must be a mapping")
    result = dict(schema)
    supported = {"string", "integer", "number", "boolean", "date", "timestamp", "json"}
    if any(not isinstance(name, str) or not name.strip() or not isinstance(kind, str) or kind not in supported for name, kind in result.items()):
        raise IncrementalError("BAD_TRANSFORM_SCHEMA", f"invalid column or data type in {label!r}")
    return result


def _validate_rows(rows: Iterable[Mapping[str, Any]], schema: Mapping[str, str], label: str) -> list[dict[str, Any]]:
    if isinstance(rows, (str, bytes, Mapping)):
        raise IncrementalError("BAD_ROW", f"rows for {label!r} must be an iterable of mappings")
    try:
        materialized = list(rows)
    except (TypeError, ValueError, OverflowError, RecursionError) as error:
        raise IncrementalError("BAD_ROW", f"rows for {label!r} are not iterable") from error
    result = []
    for index, row in enumerate(materialized):
        if not isinstance(row, Mapping) or set(row) != set(schema):
            raise IncrementalError("BAD_TRANSFORM_SCHEMA", f"row {index + 1} in {label!r} differs from declared schema")
        try:
            for name, kind in schema.items():
                _validate_value(row[name], kind, f"{label}.{name}", index)
            item = _thaw_value(row)
            for name, kind in schema.items():
                _validate_value(item[name], kind, f"{label}.{name}", index)
            _json(item)
        except RecursionError as error:
            raise IncrementalError("BAD_ROW", f"row {index + 1} in {label!r} exceeds supported nesting depth") from error
        result.append(item)
    return result


def _validate_value(value: Any, kind: str, column: str, index: int) -> None:
    if value is None:
        return
    if kind == "json":
        try:
            _validate_json_value(value)
        except IncrementalError as error:
            raise IncrementalError("TYPE_MISMATCH", f"row {index + 1} value for {column!r} is not json") from error
        return
    checks = {
        "string": lambda v: isinstance(v, str),
        "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
        "number": lambda v: (isinstance(v, int) and not isinstance(v, bool)) or (isinstance(v, float) and math.isfinite(v)),
        "boolean": lambda v: isinstance(v, bool),
        "date": _valid_date,
        "timestamp": _valid_timestamp,
    }
    if not checks[kind](value):
        raise IncrementalError("TYPE_MISMATCH", f"row {index + 1} value for {column!r} is not {kind}")


def _validate_json_value(value: Any, active: set[int] | None = None) -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if math.isfinite(value):
            return
        raise IncrementalError("BAD_ROW", "JSON numbers must be finite")
    active = active if active is not None else set()
    if isinstance(value, (list, tuple)):
        identity = id(value)
        if identity in active:
            raise IncrementalError("BAD_ROW", "JSON values cannot contain cycles")
        active.add(identity)
        try:
            for item in value:
                _validate_json_value(item, active)
        finally:
            active.remove(identity)
        return
    if isinstance(value, Mapping):
        identity = id(value)
        if identity in active:
            raise IncrementalError("BAD_ROW", "JSON values cannot contain cycles")
        if any(not isinstance(key, str) for key in value):
            raise IncrementalError("BAD_ROW", "JSON object keys must be strings")
        active.add(identity)
        try:
            for item in value.values():
                _validate_json_value(item, active)
        finally:
            active.remove(identity)
        return
    raise IncrementalError("BAD_ROW", "value contains a non-JSON Python type")


def _valid_date(value: Any) -> bool:
    if not isinstance(value, str) or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value) is None:
        return False
    try:
        date.fromisoformat(value)
        return True
    except ValueError:
        return False


def _valid_timestamp(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    match = re.fullmatch(
        r"([0-9]{4}-[0-9]{2}-[0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(?:[.]([0-9]+))?(Z|[+-][0-9]{2}:[0-9]{2})",
        value,
    )
    if match is None or not _valid_date(match.group(1)):
        return False
    hour, minute, second = (int(match.group(index)) for index in (2, 3, 4))
    if hour > 23 or minute > 59 or second > 60:
        return False
    zone = match.group(6)
    if zone != "Z":
        zone_hour, zone_minute = (int(part) for part in zone[1:].split(":"))
        if zone_hour > 23 or zone_minute > 59:
            return False
    return True


def _numeric_key(value: int | float) -> list[Any]:
    # Decimal(str(float)) compares the decimal spelling supplied by the
    # transform, avoiding Python's int-vs-float distinction in stable keys.
    from decimal import Decimal

    decimal = Decimal(value) if isinstance(value, int) else Decimal(str(value))
    sign, digits, exponent = decimal.as_tuple()
    digits = list(digits)
    if not any(digits):
        return [0, "0", 0]
    while len(digits) > 1 and digits[-1] == 0:
        digits.pop()
        exponent += 1
    return [sign, "".join(str(digit) for digit in digits), exponent]


def _key(value: Any) -> str:
    if value is None or value == "":
        raise IncrementalError("INCREMENTAL_KEY", "keys cannot be null or empty")
    if isinstance(value, bool):
        identity = ["boolean", value]
    elif isinstance(value, int):
        identity = ["number", _numeric_key(value)]
    elif isinstance(value, float) and math.isfinite(value):
        identity = ["number", _numeric_key(value)]
    elif isinstance(value, str):
        identity = ["string", value]
    else:
        raise IncrementalError("INCREMENTAL_KEY", "keys must be finite numbers or strings")
    return _json(identity)


def _slice_counts(slices: Mapping[str, InputSlice]) -> dict[str, dict[str, int]]:
    modes = ("added", "removed", "modified", "current", "previous")
    return {
        name: {mode: len(getattr(item, mode)) for mode in modes}
        for name, item in slices.items()
    }


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))


def _write_state(
    db: sqlite3.Connection,
    spec: TransformSpec,
    fingerprint: str,
    output_tx: int,
    marks: Mapping[str, int],
) -> None:
    db.execute(
        "INSERT INTO transform_state(name,semantic_version,contract_hash,output_tx) VALUES(?,?,?,?) "
        "ON CONFLICT(name) DO UPDATE SET semantic_version=excluded.semantic_version, "
        "contract_hash=excluded.contract_hash, output_tx=excluded.output_tx",
        (spec.name, spec.semantic_version, fingerprint, output_tx),
    )
    db.execute("DELETE FROM transform_watermarks WHERE transform=?", (spec.name,))
    db.executemany(
        "INSERT INTO transform_watermarks(transform,input,tx) VALUES(?,?,?)",
        [(spec.name, name, tx) for name, tx in marks.items()],
    )


def _write_success(
    db: sqlite3.Connection,
    spec: TransformSpec,
    mode: str,
    counts: Mapping[str, Any],
    marks: Mapping[str, int],
    output_tx: int,
    rows_out: int,
) -> int:
    cursor = db.execute(
        "INSERT INTO transform_runs(transform,mode,status,semantic_version,slices_json,watermarks_json,output_tx,rows_out,error_code,error_message,committed_at) "
        "VALUES(?,?,'success',?,?,?,?,?,NULL,NULL,?)",
        (
            spec.name,
            mode,
            spec.semantic_version,
            _json(counts),
            _json(dict(marks)),
            output_tx,
            rows_out,
            time.time(),
        ),
    )
    return int(cursor.lastrowid)
