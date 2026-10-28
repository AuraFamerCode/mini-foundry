from __future__ import annotations

import json
import unittest
from typing import Any, Mapping, Optional

from python.foundry_engine.incremental import (
    DatasetStore,
    IncrementalError,
    IncrementalRunner,
    InputSlice,
    OutputDelta,
    TransformSpec,
    filter_map_transform,
)


INPUT_SCHEMA = {"id": "string", "active": "boolean", "value": "integer"}
OUTPUT_SCHEMA = {"id": "string", "active": "boolean", "value": "integer"}


def spec(**overrides: Any) -> TransformSpec:
    values: dict[str, Any] = {
        "name": "active_items",
        "inputs": ("source",),
        "output": "published",
        "key": "id",
        "output_schema": OUTPUT_SCHEMA,
    }
    values.update(overrides)
    return TransformSpec(**values)


def rows(*items: tuple[str, bool, int]) -> list[dict[str, Any]]:
    return [{"id": identity, "active": active, "value": value} for identity, active, value in items]


class IncrementalRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = DatasetStore(":memory:")
        self.runner = IncrementalRunner(self.store)

    def tearDown(self) -> None:
        self.store.close()

    def add_source(self, initial: Optional[list[dict[str, Any]]] = None) -> None:
        self.store.register_dataset("source", initial or [], INPUT_SCHEMA)

    def transform(self, key: str = "id"):
        return filter_map_transform("source", key, key, lambda row: row["active"])

    def test_initial_build_is_full_snapshot_and_records_watermark(self) -> None:
        self.add_source(rows(("a", True, 1), ("b", False, 2)))
        result = self.runner.run(spec(), self.transform())

        self.assertEqual(result["mode"], "snapshot")
        self.assertEqual(result["output_operation"], "SNAPSHOT")
        self.assertEqual(self.store.read_dataset("published"), rows(("a", True, 1)))
        self.assertEqual(self.store.watermarks("active_items"), result["input_watermarks"])
        self.assertEqual(self.store.run_history("active_items")[-1]["status"], "success")

    def test_keyed_delta_updates_rows_and_removes_rows_that_stop_matching(self) -> None:
        self.add_source(rows(("a", True, 1), ("b", True, 2), ("c", False, 3)))
        self.runner.run(spec(), self.transform())
        self.store.commit_dataset("source", rows(("a", True, 10), ("b", False, 2), ("c", True, 3), ("d", True, 4)), operation="UPDATE")

        result = self.runner.run(spec(), self.transform())

        self.assertEqual(result["mode"], "incremental")
        self.assertEqual(result["output_operation"], "UPDATE")
        self.assertCountEqual(self.store.read_dataset("published"), rows(("a", True, 10), ("c", True, 3), ("d", True, 4)))
        self.assertEqual(result["read_slices"]["source"]["modified"], 3)
        self.assertEqual(result["read_slices"]["source"]["added"], 1)

    def test_deletion_requires_retention_and_failure_does_not_advance_checkpoint(self) -> None:
        self.add_source(rows(("a", True, 1), ("b", True, 2)))
        self.runner.run(spec(), self.transform())
        watermark_before = self.store.watermarks("active_items")
        output_before = self.store.read_dataset("published")
        self.store.commit_dataset("source", rows(("b", True, 2)), operation="UPDATE")

        with self.assertRaisesRegex(IncrementalError, "input removals require") as caught:
            self.runner.run(spec(), self.transform())

        self.assertEqual(caught.exception.code, "RETENTION_REQUIRED")
        self.assertEqual(self.store.watermarks("active_items"), watermark_before)
        self.assertEqual(self.store.read_dataset("published"), output_before)
        self.assertEqual(self.store.run_history("active_items")[-1]["status"], "failed")

    def test_retention_enabled_deletes_removed_output(self) -> None:
        self.add_source(rows(("a", True, 1), ("b", True, 2)))
        transform_spec = spec(allow_retention=True)
        self.runner.run(transform_spec, self.transform())
        self.store.commit_dataset("source", rows(("b", True, 2)), operation="UPDATE")

        self.runner.run(transform_spec, self.transform())

        self.assertEqual(self.store.read_dataset("published"), rows(("b", True, 2)))

    def test_semantic_version_bump_rebuilds_changed_transform_semantics(self) -> None:
        self.add_source(rows(("a", True, 1), ("b", False, 2)))
        original = spec()
        self.runner.run(original, self.transform())

        def include_all(_slices: Mapping[str, InputSlice], mode: str):
            if mode != "snapshot":
                return OutputDelta()
            return rows(("a", True, 1), ("b", False, 2))

        result = self.runner.run(spec(semantic_version=2), include_all)

        self.assertEqual(result["mode"], "snapshot")
        self.assertEqual(result["reasons"], ["semantic version changed"])
        self.assertCountEqual(self.store.read_dataset("published"), rows(("a", True, 1), ("b", False, 2)))

    def test_semantic_version_bump_can_atomically_change_output_schema(self) -> None:
        self.add_source(rows(("a", True, 1)))
        self.runner.run(spec(), self.transform())
        upgraded_spec = spec(semantic_version=2, output_schema={"id": "string", "label": "string"})

        def upgraded(_slices: Mapping[str, InputSlice], mode: str):
            if mode != "snapshot":
                return OutputDelta()
            return [{"id": "a", "label": "reclassified"}]

        result = self.runner.run(upgraded_spec, upgraded)

        self.assertEqual(result["mode"], "snapshot")
        self.assertEqual(self.store.read_dataset("published"), [{"id": "a", "label": "reclassified"}])

    def test_semantic_version_bump_allows_required_incremental_snapshot(self) -> None:
        self.add_source(rows(("a", True, 1)))
        first_spec = spec(require_incremental=True)
        self.runner.run(first_spec, self.transform())
        self.store.commit_dataset("source", rows(("b", True, 2)), operation="APPEND")

        upgraded = self.runner.run(spec(require_incremental=True, semantic_version=2), self.transform())

        self.assertEqual(upgraded["mode"], "snapshot")
        self.assertEqual(upgraded["reasons"], ["semantic version changed"])
        self.assertCountEqual(self.store.read_dataset("published"), rows(("a", True, 1), ("b", True, 2)))

    def test_require_incremental_rejects_explicit_forced_snapshot(self) -> None:
        self.add_source(rows(("a", True, 1)))
        strict = spec(require_incremental=True)
        self.runner.run(strict, self.transform())

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(strict, self.transform(), force_snapshot=True)

        self.assertEqual(caught.exception.code, "REQUIRE_INCREMENTAL")

    def test_strict_append_requires_append_inputs_and_new_output_keys(self) -> None:
        self.add_source(rows(("a", True, 1)))
        strict = spec(strict_append=True)
        self.runner.run(strict, self.transform())
        self.store.commit_dataset("source", rows(("b", True, 2)), operation="APPEND")

        result = self.runner.run(strict, self.transform())

        self.assertEqual(result["output_operation"], "APPEND")
        self.assertCountEqual(self.store.read_dataset("published"), rows(("a", True, 1), ("b", True, 2)))

    def test_strict_append_rejects_non_append_input_transaction(self) -> None:
        self.add_source(rows(("a", True, 1)))
        strict = spec(strict_append=True)
        self.runner.run(strict, self.transform())
        self.store.commit_dataset("source", rows(("a", True, 2)), operation="UPDATE")

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(strict, self.transform())

        self.assertEqual(caught.exception.code, "STRICT_APPEND_VIOLATION")

    def test_strict_append_checks_unchanged_input_transactions(self) -> None:
        self.add_source(rows(("a", True, 1)))
        strict = spec(strict_append=True)
        self.runner.run(strict, self.transform())
        self.store.commit_dataset("source", rows(("a", True, 1)), operation="SNAPSHOT")

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(strict, self.transform())

        self.assertEqual(caught.exception.code, "STRICT_APPEND_VIOLATION")

    def test_strict_append_rejects_existing_output_key(self) -> None:
        self.add_source(rows(("a", True, 1)))
        strict = spec(strict_append=True)
        transform = filter_map_transform(
            "source", "id", "id", lambda row: row["active"],
            lambda row: {**row, "id": "same-output-key"},
        )
        self.runner.run(strict, transform)
        self.store.commit_dataset("source", rows(("b", True, 2)), operation="APPEND")

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(strict, transform)

        self.assertEqual(caught.exception.code, "STRICT_APPEND_VIOLATION")

    def test_failed_output_validation_rolls_back_output_and_watermark(self) -> None:
        self.add_source(rows(("a", True, 1)))
        self.runner.run(spec(), self.transform())
        old_watermarks = self.store.watermarks("active_items")
        old_output = self.store.read_dataset("published")
        self.store.commit_dataset("source", rows(("a", True, 2)), operation="UPDATE")

        def invalid_transform(_slices: Mapping[str, InputSlice], mode: str):
            if mode == "snapshot":
                return rows(("a", True, 2))
            return OutputDelta(upserts=[{"id": "a", "active": True, "value": "wrong"}])

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(spec(), invalid_transform)

        self.assertEqual(caught.exception.code, "TYPE_MISMATCH")
        self.assertEqual(self.store.watermarks("active_items"), old_watermarks)
        self.assertEqual(self.store.read_dataset("published"), old_output)
        self.assertEqual(self.store.run_history("active_items")[-1]["status"], "failed")

    def test_input_advance_during_compute_is_detected_before_publish(self) -> None:
        self.add_source(rows(("a", True, 1)))
        self.runner.run(spec(), self.transform())
        old_watermarks = self.store.watermarks("active_items")
        self.store.commit_dataset("source", rows(("a", True, 2)), operation="UPDATE")

        def racing_transform(slices: Mapping[str, InputSlice], mode: str):
            if mode == "snapshot":
                return rows(("a", True, 1))
            self.store.commit_dataset("source", rows(("a", True, 2), ("b", True, 3)), operation="UPDATE")
            return OutputDelta(upserts=[{"id": "a", "active": True, "value": 2}])

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(spec(), racing_transform)

        self.assertEqual(caught.exception.code, "INPUT_CONFLICT")
        self.assertEqual(self.store.watermarks("active_items"), old_watermarks)
        self.assertEqual(self.store.read_dataset("published"), rows(("a", True, 1)))

    def test_input_slices_deeply_freeze_nested_json(self) -> None:
        source_schema = {"id": "string", "payload": "json"}
        self.store.register_dataset("source", [{"id": "a", "payload": {"items": [1]}}], source_schema)
        transform_spec = TransformSpec(
            name="immutable_input",
            inputs=("source",),
            output="published",
            key="id",
            output_schema={"id": "string", "payload": "json"},
        )

        def transform(slices: Mapping[str, InputSlice], _mode: str):
            payload = slices["source"].current[0]["payload"]
            with self.assertRaises(TypeError):
                payload["changed"] = True  # type: ignore[index]
            with self.assertRaises(AttributeError):
                payload["items"].append(2)
            return [{"id": "a", "payload": payload}]

        self.runner.run(transform_spec, transform)

        self.assertEqual(self.store.read_dataset("source"), [{"id": "a", "payload": {"items": [1]}}])
        self.assertEqual(self.store.read_dataset("published"), [{"id": "a", "payload": {"items": [1]}}])

    def test_number_keys_use_numeric_equality_across_integer_and_float(self) -> None:
        self.add_source(rows(("source", True, 1)))
        number_spec = spec(key="id", output_schema={"id": "number"})

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(number_spec, lambda _slices, _mode: [{"id": 1}, {"id": 1.0}])

        self.assertEqual(caught.exception.code, "INCREMENTAL_DUPLICATE_KEY")

    def test_output_schema_change_requires_semantic_version_bump(self) -> None:
        self.add_source(rows(("a", True, 1)))
        self.runner.run(spec(), self.transform())
        externally_changed_schema = {**OUTPUT_SCHEMA, "value": "number"}
        db = self.store.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE datasets SET schema_json=? WHERE name=?",
                (json.dumps(externally_changed_schema, sort_keys=True, separators=(",", ":")), "published"),
            )
            db.commit()
        finally:
            db.close()

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(spec(), self.transform())

        self.assertEqual(caught.exception.code, "OUTPUT_SCHEMA_CONFLICT")
        self.assertEqual(self.store.read_dataset("published"), rows(("a", True, 1)))

    def test_json_schema_rejects_non_json_values_and_cycles(self) -> None:
        schema = {"id": "string", "payload": "json"}
        cycle: list[Any] = []
        cycle.append(cycle)

        with self.assertRaises(IncrementalError) as object_key:
            self.store.register_dataset("bad_key", [{"id": "a", "payload": {1: "not-json"}}], schema)
        self.assertEqual(object_key.exception.code, "TYPE_MISMATCH")
        with self.assertRaises(IncrementalError) as recursive:
            self.store.register_dataset("bad_cycle", [{"id": "a", "payload": cycle}], schema)
        self.assertEqual(recursive.exception.code, "TYPE_MISMATCH")

    def test_date_and_timestamp_schema_reject_invalid_values(self) -> None:
        schema = {"day": "date", "created_at": "timestamp"}
        with self.assertRaises(IncrementalError) as invalid_date:
            self.store.register_dataset("bad_date", [{"day": "2023-02-29", "created_at": "2024-02-29T12:00:00Z"}], schema)
        self.assertEqual(invalid_date.exception.code, "TYPE_MISMATCH")
        with self.assertRaises(IncrementalError) as invalid_timestamp:
            self.store.register_dataset("bad_timestamp", [{"day": "2024-02-29", "created_at": "2024-02-29T12:00:00"}], schema)
        self.assertEqual(invalid_timestamp.exception.code, "TYPE_MISMATCH")
        self.store.register_dataset("valid_timestamp", [{"day": "2024-02-29", "created_at": "2024-02-29T12:00:00.123+05:30"}], schema)

    def test_delete_key_must_match_declared_output_key_type(self) -> None:
        self.add_source(rows(("a", True, 1)))
        self.runner.run(spec(), self.transform())
        self.store.commit_dataset("source", [], operation="APPEND")

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(spec(), lambda _slices, _mode: OutputDelta(deletes=[1]))

        self.assertEqual(caught.exception.code, "TYPE_MISMATCH")

    def test_missing_output_delete_key_is_rejected(self) -> None:
        self.add_source(rows(("a", True, 1)))
        self.runner.run(spec(), self.transform())
        self.store.commit_dataset("source", [], operation="APPEND")

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(spec(), lambda _slices, _mode: OutputDelta(deletes=["not-present"]))

        self.assertEqual(caught.exception.code, "BAD_TRANSFORM_RESULT")

    def test_unchanged_rows_cannot_produce_output_delta(self) -> None:
        self.add_source(rows(("a", True, 1)))
        self.runner.run(spec(), self.transform())
        self.store.commit_dataset("source", [], operation="APPEND")

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(spec(), lambda _slices, _mode: OutputDelta(upserts=rows(("rogue", True, 2))))

        self.assertEqual(caught.exception.code, "BAD_TRANSFORM_RESULT")

    def test_input_schema_change_requires_semantic_version_bump(self) -> None:
        self.add_source(rows(("a", True, 1)))
        self.runner.run(spec(), self.transform())
        evolved_schema = {**INPUT_SCHEMA, "value": "number"}
        db = self.store.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE datasets SET schema_json=? WHERE name=?",
                (json.dumps(evolved_schema, sort_keys=True, separators=(",", ":")), "source"),
            )
            db.commit()
        finally:
            db.close()

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(spec(), self.transform())

        self.assertEqual(caught.exception.code, "INCREMENTAL_CONTRACT_CHANGED")
        self.assertEqual(self.store.read_dataset("published"), rows(("a", True, 1)))

    def test_contract_change_requires_semantic_version_bump(self) -> None:
        self.add_source(rows(("a", True, 1)))
        self.runner.run(spec(), self.transform())

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(spec(output_schema={"id": "string", "active": "boolean", "value": "number"}), self.transform())

        self.assertEqual(caught.exception.code, "INCREMENTAL_CONTRACT_CHANGED")

    def test_semantic_version_rollback_is_rejected(self) -> None:
        self.add_source(rows(("a", True, 1)))
        self.runner.run(spec(semantic_version=2), self.transform())

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(spec(), self.transform())

        self.assertEqual(caught.exception.code, "SEMANTIC_VERSION_ROLLBACK")

    def test_snapshot_only_input_advancement_fails_incremental_run(self) -> None:
        self.add_source(rows(("a", True, 1)))
        transform_spec = spec(snapshot_inputs=("source",))
        self.runner.run(transform_spec, self.transform())
        self.store.commit_dataset("source", rows(("a", True, 2)), operation="UPDATE")

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(transform_spec, self.transform())

        self.assertEqual(caught.exception.code, "SNAPSHOT_INPUT_CHANGED")

    def test_noop_and_failure_runs_are_audited_and_advance_watermarks_atomically(self) -> None:
        self.add_source(rows(("a", True, 1)))
        self.runner.run(spec(), self.transform())
        self.store.commit_dataset("source", [], operation="APPEND")
        output_history = self.store.output_history("published")

        result = self.runner.run(spec(), self.transform())

        self.assertEqual(result["status"], "skipped")
        self.assertNotEqual(self.store.watermarks("active_items")["source"], 1)
        self.assertEqual(self.store.output_history("published"), output_history)
        self.assertEqual(self.store.run_history("active_items")[-1]["status"], "success")
        self.assertIsNotNone(result["run_id"])

        self.store.commit_dataset("source", rows(("a", True, 2)), operation="UPDATE")
        old_marks = self.store.watermarks("active_items")
        with self.assertRaisesRegex(RuntimeError, "secret input data"):
            self.runner.run(
                spec(),
                lambda _slices, _mode: (_ for _ in ()).throw(RuntimeError("secret input data")),
            )
        self.assertEqual(self.store.watermarks("active_items"), old_marks)
        failed_run = self.store.run_history("active_items")[-1]
        self.assertEqual(failed_run["status"], "failed")
        self.assertEqual(failed_run["error_code"], "TRANSFORM_EXECUTION_FAILED")
        self.assertIsNone(failed_run["error_message"])

    def test_no_changes_skip_without_output_commit(self) -> None:
        self.add_source(rows(("a", True, 1)))
        self.runner.run(spec(), self.transform())
        history_before = self.store.output_history("published")

        result = self.runner.run(spec(), self.transform())

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(self.store.output_history("published"), history_before)
        self.assertEqual(self.store.run_history("active_items")[-1]["status"], "success")
        self.assertEqual(result["run_id"], self.store.run_history("active_items")[-1]["id"])

    def test_restricted_views_cannot_feed_batch_transforms(self) -> None:
        self.store.register_dataset("source", rows(("a", True, 1)), INPUT_SCHEMA, restricted_view=True)

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(spec(), self.transform())

        self.assertEqual(caught.exception.code, "RESTRICTED_VIEW_BATCH")

    def test_multi_input_transform_can_use_per_input_primary_keys(self) -> None:
        self.store.register_dataset("left", [{"left_id": "a", "value": 1}], {"left_id": "string", "value": "integer"})
        self.store.register_dataset("right", [{"right_id": "b", "value": 2}], {"right_id": "string", "value": "integer"})
        multi = spec(inputs=("left", "right"), input_keys=(("left", "left_id"), ("right", "right_id")))

        def transform(slices: Mapping[str, InputSlice], mode: str):
            if mode == "snapshot":
                return [
                    {"id": row[key_name], "active": True, "value": row["value"]}
                    for name, slice_value in slices.items()
                    for key_name in ("left_id" if name == "left" else "right_id",)
                    for row in slice_value.current
                ]
            upserts = [
                {"id": row[key_name], "active": True, "value": row["value"]}
                for name, slice_value in slices.items()
                for key_name in ("left_id" if name == "left" else "right_id",)
                for row in (*slice_value.added, *slice_value.modified)
            ]
            deletes = [
                row[key_name]
                for name, slice_value in slices.items()
                for key_name in ("left_id" if name == "left" else "right_id",)
                for row in slice_value.removed
            ]
            return OutputDelta(upserts, deletes)

        result = self.runner.run(multi, transform)
        self.assertEqual(result["mode"], "snapshot")
        self.assertCountEqual(self.store.read_dataset("published"), rows(("a", True, 1), ("b", True, 2)))

    def test_input_key_may_differ_from_output_key(self) -> None:
        input_schema = {"source_id": "string", "active": "boolean", "value": "integer"}
        self.store.register_dataset("source", [{"source_id": "a", "active": True, "value": 1}], input_schema)
        custom = spec(input_keys=(("source", "source_id"),))

        def transform(slices: Mapping[str, InputSlice], mode: str):
            item = slices["source"]
            project = lambda row: {"id": row["source_id"], "active": row["active"], "value": row["value"]}
            if mode == "snapshot":
                return [project(row) for row in item.current]
            return OutputDelta(upserts=[project(row) for row in (*item.added, *item.modified)])

        result = self.runner.run(custom, transform)

        self.assertEqual(result["mode"], "snapshot")
        self.assertEqual(self.store.read_dataset("published"), rows(("a", True, 1)))

    def test_duplicate_output_keys_are_rejected(self) -> None:
        self.add_source(rows(("a", True, 1)))

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(spec(), lambda _slices, _mode: rows(("a", True, 1), ("a", True, 2)))

        self.assertEqual(caught.exception.code, "INCREMENTAL_DUPLICATE_KEY")
        self.assertFalse(any(run["status"] == "success" for run in self.store.run_history("active_items")))

    def test_output_conflict_is_never_silently_overwritten(self) -> None:
        self.add_source(rows(("a", True, 1)))
        self.runner.run(spec(), self.transform())
        self.store.commit_dataset("published", rows(("external", True, 99)), operation="UPDATE")

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(spec(), self.transform())
        self.assertEqual(caught.exception.code, "OUTPUT_CONFLICT")
        with self.assertRaises(IncrementalError) as forced:
            self.runner.run(spec(), self.transform(), force_snapshot=True)
        self.assertEqual(forced.exception.code, "OUTPUT_CONFLICT")
        self.assertEqual(self.store.read_dataset("published"), rows(("external", True, 99)))

    def test_invalid_transform_spec_is_recorded_as_typed_failure(self) -> None:
        invalid = spec(semantic_version="invalid")

        with self.assertRaises(IncrementalError) as caught:
            self.runner.run(invalid, self.transform())

        self.assertEqual(caught.exception.code, "BAD_TRANSFORM")
        self.assertEqual(self.store.run_history("active_items")[-1]["error_code"], "BAD_TRANSFORM")

    def test_output_conflict_recovery_requires_new_semantic_version(self) -> None:
        self.add_source(rows(("a", True, 1)))
        self.runner.run(spec(), self.transform())
        self.store.commit_dataset("published", rows(("external", True, 99)), operation="UPDATE")

        with self.assertRaises(IncrementalError):
            self.runner.run(spec(semantic_version=2), self.transform())

        self.assertEqual(self.store.read_dataset("published"), rows(("external", True, 99)))


if __name__ == "__main__":
    unittest.main()
