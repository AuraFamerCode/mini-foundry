# mini-foundry — polyglot core migration

This repository contains two small, testable migration components: a Rust physical-ontology validation/planning library and a Python incremental-transform reference runner. It is **not a complete, professional-grade or production-ready platform**. The original JavaScript app (`server.js`, `lib/*.js`, `public/*`) remains in the repository, and no Python API, browser UI, or integrated replacement runtime has been delivered. Treat the current work as foundations, not as a deployable system. A concurrent control-plane service slice (`rust/foundry-api`, Axum/Tokio/PostgreSQL) now exists and passes unit tests, but it has never run against a live PostgreSQL instance and is not connected to the legacy app. An optional advisory package (`python/laya_advisory`) proposes column-masking policies with a local Laya typed-decision model; its output is an unverified suggestion until a human approves it.

The Rust core is pure in-memory validation and planning. The Python runner is a local SQLite correctness reference. The service slice is a compiled starting point, not an operating system. There is no production HTTP service in operation, identity provider, authorization layer, shared durable storage contract, object store, worker scheduler, observability stack, deployment configuration, backup/recovery protocol, or end-to-end integration between the components. A passing unit test only describes the tested code path; it is not evidence of scale, security, availability, or operational readiness.

## Target service ownership

| Concern | Target owner | Status |
|---|---|---|
| In-memory dataset, property, primary-key, link-reference, cardinality and ontology-snapshot checks; destructive PK-change plan | `rust/ontology-core` | Implemented and unit-tested. No persistence, connector, indexer or migration executor; JSON cell syntax is connector-owned and not parsed here |
| Input deltas, semantic-version/contract checks, strict-append and retention policies, output schema validation, SQLite output/run/checkpoint transactions and optimistic version checks | `python/foundry_engine` | Implemented as a standard-library local reference and unit-tested. No distributed execution, durable shared-store guarantees, scheduling or production scaling |
| Concurrent dataset API: bounded request admission, optimistic-version dataset transactions (snapshot/append/delete), keyset row pagination, health probes | `rust/foundry-api` | Initial slice implemented and unit-tested. Never exercised against a live PostgreSQL; no authentication, multi-tenancy or deployment path |
| Advisory column-sensitivity proposals feeding the masking-policy model (local Laya typed-decisions model, optional dependency) | `python/laya_advisory` | Implemented and unit-tested with a stub router; live model signals carry measured biases (see below). Proposals only — never auto-applied |
| API completion, identity and authorization, shared durable storage beyond this slice, writeback execution, index publication, HTML/CSS console | Not yet migrated | Requires the next migration slices |

Arrow/Parquet and a versioned control-plane contract are possible future choices, not implemented interfaces or commitments. No wire protocol is implemented. Before these components are connected, define one authoritative mutation owner and make the adapter validate and commit object metadata, link/index updates, edit/writeback purges, output versions, and checkpoints in a real transaction boundary with tested rollback and recovery.

## Concurrent dataset API slice

`rust/foundry-api` is an Axum/Tokio service with PostgreSQL as the source of truth, sized for a single host serving hundreds of active requests. A semaphore middleware admits at most `MAX_CONCURRENT_REQUESTS` in-flight requests (default 256) and answers excess with `503` plus `Retry-After: 1`; the pool is capped by `MAX_DB_CONNECTIONS` (default 32).

Endpoints: `GET /health/live`, `GET /health/ready` (probes the database), `GET|POST /v1/datasets`, `GET /v1/datasets/:dataset`, `GET /v1/datasets/:dataset/rows` (keyset pagination, `limit` ≤ 1000), and `POST /v1/datasets/:dataset/transactions`. Each transaction carries `expected_version`; the handler locks the dataset metadata row `FOR UPDATE`, verifies the version, validates rows through `ontology-core` (including canonical primary-key tokens, so `1`, `+1` and `1.00` collide rather than duplicate), applies snapshot/append/delete, persists the delta to `dataset_transactions`, and bumps the version in one database transaction. Unique violations map to `409 ALREADY_EXISTS`.

Honest limits: the concurrent-correctness design is verified by unit tests only — there is not yet an integration test against a live PostgreSQL, so transaction and serialization behavior is unproven in practice. There is no authentication or authorization on any endpoint, no multi-tenancy, no rate limiting beyond the concurrency semaphore, no metrics/tracing export wired beyond `tracing` hooks, and no deployment configuration. Never expose it directly; treat it as a component to be wrapped.

## Object type and link rules

The Rust core checks explicit primary and title properties, one physical backing dataset per object type, mapped column types, row value types for its declared primitives, nonempty unique keys, and FK/join/object-backed link schema and endpoint references. It validates in-memory supplied rows only; it does not read external data, parse JSON cells, create database constraints, or enforce anything after the caller discards the validated snapshot. Numeric key parsing is handwritten and intentionally not a claim of full connector/SQL numeric-type equivalence. Errors include `OntologyMetadata:IncompatibleFoundryFieldSchemaForPropertyType` and `OntologyMetadata:DuplicatePrimaryKey`.

`plan_primary_key_change` requires explicit confirmation and fresh copies of the object/dataset in the supplied model; it validates the proposed key and physical model before returning a purge *plan*. Dataset names are caller-supplied labels—not proof that the edit/writeback datasets exist, are correctly scoped, or can be purged. The function does not execute a mutation or prove atomicity. Do not call it a completed migration: a production adapter still needs durable ownership checks, authorization, purge allow-listing, index/link rebuild, audit, transaction, rollback and concurrency protection.

## Incremental runner

`TransformSpec` declares input/output names, schemas, keys, snapshot inputs, semantic version, retention, and strict-append policy. Workers receive explicit `added`, `removed`, `modified`, `current`, and `previous` slices. Initial builds and semantic-version bumps use complete snapshots. Later runs return `OutputDelta(upserts, deletes)` so updates and removals cannot be mistaken for append-only data.

The local SQLite reference writes output snapshots, successful-run records and watermarks in a single SQLite transaction and checks captured input/output transaction ids and metadata before publish. This is a local database guarantee, not a distributed transaction or cross-process orchestration protocol. Failure history is best-effort and deliberately omits exception text; it is not an operational audit log. Output ownership, input/output schema contracts, duplicate keys and scalar types are checked by the runner, but SQLite here has no tenant isolation, authorization, encryption, migrations, backups or production operations. Transform code is trusted Python callable code: never load user-submitted Python. Joins/aggregations must explicitly emit every affected output key, and correctness is the transform author's responsibility.

## Advisory masking-policy proposals (optional)

`python/laya_advisory` turns a dataset schema into per-column proposals: a sensitivity tier (`restricted` / `internal` / `public`) and a row-identifier flag. It uses the Laya typed-decision model (`Router.predict` returns one structured answer with probabilities per question, one batched forward pass for the whole schema). The core logic is stdlib-only and testable with an injected stub router; `laya` is a lazy, optional dependency and the model runs locally on cached weights — column names, types and any sample values you pass are never sent to a network service.

Deliberate design constraints:

- Proposals only. Nothing is written to `data/state.json`, `lib/security.js` policy state, or any read path; a human reviews and configures any policy afterwards.
- Name rules act as floors. A rule-matched column can be escalated by the model but never downgraded below its rule tier.
- The identifier flag is rule-first. The model can only add the flag to rule-less columns, and only above a conservative threshold, because the model's yes/no axis shows a measured yes-bias on schema-like inputs (~0.75–0.85 even for counts and cities).
- The model's `confidence` on schema-like inputs is measured to be near zero on the choice axis; probabilities are surfaced for review, not treated as calibrated. Use them as one signal among several, never as a substitute for review.

Run the demo (needs `laya` installed, e.g. in `.venv`):

```sh
python3 -m venv .venv && .venv/bin/python -m pip install laya
.venv/bin/python demo_laya_advisory.py
```

## Checks

From the repository root:

```sh
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
python3 -m py_compile python/foundry_engine/*.py python/tests/*.py python/laya_advisory/*.py python/tests/test_laya_advisory.py
python3 -m unittest discover -s python/tests -v
```

The advisory-classifier tests are stub-based and run on the plain interpreter; only the demo needs `laya` installed.

The service slice adds:

```sh
cargo test -p foundry-api
DATABASE_URL=postgres://user:pass@host/db BIND_ADDR=127.0.0.1:6475 cargo run -p foundry-api
```

The Python package declares Python 3.9+. The Rust workspace declares Rust 1.78+; `rust-toolchain.toml` selects the installed stable toolchain rather than an exact reproducible compiler. Run the format, test, Clippy, compile and unit-test checks shown above with the toolchains used for your release. The unit tests of `foundry-api` do not touch a database; an integration pass against a real PostgreSQL is still required before trusting its transaction behavior. The legacy `npm test` covers only the JavaScript app and is not validation of these components; its failures are tracked separately.

## What production-grade still requires

1. Write explicit system requirements, threat model, data classification/tenancy model, service boundaries, SLOs, capacity targets and failure semantics.
2. Choose and implement a durable source of truth plus a versioned Rust/Python contract. Add schema migrations, idempotency, durable job ownership, transaction/rollback and recovery procedures.
3. Add authentication and authorization to every API, dataset, action and transform; establish secrets handling, audit access, rate limits and abuse controls.
4. Build the API/UI, worker scheduler, bounded concurrency/backpressure, retry policy, cancellation, dead-letter handling and operational dashboards.
5. Add integration and property-based tests, corruption/concurrency/fault-injection tests, backup-restore drills, dependency/security scanning, deployment automation and staged rollout/rollback.
6. Benchmark the actual workloads at an agreed scale and compare correctness, latency, throughput and cost before making performance claims.
7. Migrate the legacy JavaScript app only after the new system passes shadow comparisons and operational acceptance; remove JavaScript from the target only when the replacement UI and runtime are complete.

None of those requirements is satisfied merely by the current unit tests. No performance, security, availability or production-readiness claim is made.
