# Guiding Principles for Client Dagster ETL Pipelines

This document outlines architectural and operational principles for building production Dagster pipelines at Cogapp. It's written for developers familiar with ETL concepts but potentially new to Dagster.

## Dagster Concepts (Quick Reference)

- **Asset**: A data output produced by your pipeline—a database table, a Parquet file, or an Elasticsearch index. By declaring dependencies between assets, Dagster determines execution order automatically.

- **Job**: A selection of assets that run together. Jobs appear as launchable units in the Dagster UI and can be scheduled.

- **Resource**: A way to pass configuration and shared services (database connections, API credentials, file paths) into your pipeline code without hardcoding them. Instead of writing `db = connect("prod-server.example.com")` directly in your code, you define a resource that Dagster provides at runtime. This means the same code can run against different databases in dev, staging, and production—you just swap the resource configuration. Resources are declared as function parameters, and Dagster automatically provides them when your code runs.

- **IO Manager**: Handles storage and retrieval of data between pipeline steps. When one asset finishes, the IO manager saves its output (e.g., DataFrame to Parquet). When the next asset runs, the IO manager loads that data back in. You don't write file-saving code in every asset—the IO manager does it automatically based on what you return.

---

## Architecture & Design

### Separate Ingestion from Transformation

**Why**: Decoupling harvest from transformation lets you swap processing engines (Polars, SQL, etc.) without touching ingestion logic. You can run multiple transformation implementations in parallel to compare approaches or validate rewrites before switching over.

**How**: Build a shared harvest layer that all downstream pipelines consume. Harvest assets load raw data from sources (APIs, databases, CSVs) and **must** write to Parquet files. Transformation assets read from these Parquet files and never touch source systems directly.

### Group Assets by Logical Stage, Not Individual Operation

**Why**: Pipelines with 50+ individual transformations become unmanageable as separate assets. Too much granularity makes the asset graph unreadable and creates too many places to look when understanding or debugging the pipeline.

**How**: Organize transformations into 5-10 logical stages per pipeline: **harvest**, **clean**, **join/enrich**, **reshape**, **output**. Each stage is an asset. Within a stage, chain related operations together.

Split into separate assets when:
- You're joining a new data source
- You're crossing a meaningful checkpoint (cleaning → enrichment)
- You'd want to re-materialize this step independently

**Avoid**: Monolithic assets that do everything in a single function (hard to test, no intermediate snapshots), and nano-assets that split tightly coupled expressions into separate steps (too many places to look).

### Declare Explicit Dependencies When Parallelism Matters

**Why**: Dagster parallelizes by default. If an asset depends on an external system that must complete first, Dagster won't know unless you declare it.

**How**: Use `deps=["asset_key"]` to declare non-data dependencies. Document *why* the dependency exists if it isn't obvious from the data flow.

### Design Assets to Be Safe to Re-run

**Why**: Pipelines fail partway through, schedules overlap, and developers re-run assets during debugging. If an asset produces different results when run twice with the same input—or leaves behind duplicate records—you'll have inconsistent data.

**How**: Every asset should be *safe to re-run*: given the same input, it produces the same output, and running it multiple times doesn't create duplicates or side effects. For database writes, use upserts or truncate-then-insert. For file outputs, overwrite rather than append. Avoid depending on wall-clock time or external state that changes between runs.

---

## Data & Storage

### Default to Polars Lazy DataFrames

**Why**: Polars' lazy evaluation defers computation until you call `.collect()`, allowing the query optimizer to analyze the entire pipeline first. This enables automatic optimizations (predicate pushdown, projection pruning, operation reordering) that reduce memory usage and runtime. Lazy evaluation also prevents loading entire datasets into memory when only a subset is needed.

**How**: Use [Polars lazy DataFrames](https://docs.pola.rs/user-guide/lazy/using/) for transformations. Build your pipeline as a chain of lazy operations, then collect at the end. Avoid row-by-row operations (`.map()`, `.apply()`, Python loops)—these bypass vectorized execution and are orders of magnitude slower.

### Use Parquet for Intermediate Storage

**Why**: Parquet provides columnar compression, schema preservation, and broad interoperability. It's the de facto standard for data pipelines.

**How**: Configure [`PolarsParquetIOManager`](https://docs.dagster.io/integrations/libraries/polars) as your default IO manager. Intermediate assets are automatically serialized as Parquet files. Final outputs (JSON, Elasticsearch, sitemaps) are written by dedicated output assets.

### Handle Schema Changes Defensively

**Why**: Source systems change without warning. A new column, a renamed field, or a changed data type can break pipelines silently or cause runtime errors.

**How**: Select columns explicitly (not `SELECT *`). Use Pandera schemas to validate expected columns at harvest boundaries. Fail fast on missing required columns. Log unexpected columns as warnings for investigation.

---

## Dagster Patterns

### Organize Assets into Jobs

**Why**: Jobs define what runs together, what appears in the Dagster UI launchpad, and what gets scheduled. Without jobs, you have assets but no clear execution entry points.

**How**: Create [jobs](https://docs.dagster.io/concepts/ops-jobs-graphs/jobs) that select groups of related assets. Each job should have a clear purpose: "ingest and transform collection data", "generate nightly reports". Use `dg.define_asset_job()` with asset selection by group.

### Inject Configuration via Resources

**Why**: Hardcoded paths and credentials make pipelines brittle and impossible to test. Resources enable environment-specific configuration (dev, staging, prod) and improve testability.

**How**: Use [`ConfigurableResource`](https://docs.dagster.io/concepts/resources) for all external dependencies: database connections, API endpoints, file paths, credentials. Resources are type-safe, injectable at runtime, and can be overridden in tests.

### Version Assets to Track Staleness

**Why**: When you change asset logic, downstream assets become stale. Without versioning, Dagster can't detect this and you may use outdated computed data.

**How**: Assign a [`code_version`](https://docs.dagster.io/guides/build/assets/asset-versioning-and-caching) string to every asset. Increment it when you modify transformation logic. Dagster will mark downstream assets as stale in the UI.

**Note**: Code versioning is most critical when deploying to shared environments. During local development, you can iterate without updating versions.

### Tag Assets for Organization

**Why**: Large pipelines become difficult to navigate. Tags help organize assets visually in the UI and enable filtering/selection.

**How**: Use [`kinds`](https://docs.dagster.io/concepts/metadata-tags/asset-metadata#kinds) to tag by technology (`{"polars"}`, `{"elasticsearch"}`). Use `group_name` to organize by pipeline stage (`"harvest"`, `"transform"`, `"output"`).

### Handle Retries at the Job Level

**Why**: Retry logic scattered across assets is hard to maintain. Dagster provides framework-level retry policies.

**How**: Configure [retry policies](https://docs.dagster.io/deployment/execution/run-retries) on jobs, not within individual assets. Use exponential backoff for transient failures (network issues, rate limits).

---

## Quality & Validation

### Validate at Boundaries, Not Everywhere

**Why**: Validation scattered throughout pipeline logic creates noise. Boundary validation catches problems early without cluttering transform code.

**How**: Use Pandera schema checks and Dagster asset checks at layer boundaries: after harvest, after transform, before output. Distinguish between **blocking** checks (halt on failure) and **non-blocking** checks (warn but continue).

### Emit Rich Metadata for Every Asset

**Why**: The Dagster UI should be self-documenting. Developers and stakeholders should understand what happened without reading code.

**How**: Every asset should use `context.add_output_metadata()` to include record counts, column lists, table previews (markdown), charts (Altair), and processing time. Build shared helper functions to reduce boilerplate.

### Monitor Freshness for Critical Assets

**Why**: Stale data in production is often silent—pipelines succeed but data isn't updated.

**How**: Add [`FreshnessPolicy`](https://docs.dagster.io/concepts/assets/asset-checks/freshness-checks) to critical assets (outputs feeding client-facing systems, nightly reports). Dagster will alert if the asset hasn't been materialized within the policy window.

### Test Transformation Logic in Isolation

**Why**: Pipelines that only run end-to-end are slow to test and hard to debug. Unit tests catch bugs early.

**How**: Extract transformation logic into pure Python functions (DataFrame in, DataFrame out) that are easy to unit test. Create small test fixtures (5-10 rows) for edge cases. Use [`materialize_to_memory()`](https://docs.dagster.io/guides/test/testing-assets) for integration tests. Mock external systems via resources.

---

## Operations

### Alert on Failures, Notify Selectively on Success

**Why**: Production failures need immediate attention. Success notifications for critical runs provide peace of mind and audit trails.

**How**: Configure notifications for pipeline failures (Slack, email). For high-stakes runs, consider success notifications as well. Use Dagster sensors or run status hooks, not ad-hoc notification code in assets.

### Design for Developer Handover

**Why**: Other Cogapp developers will maintain, debug, and extend these pipelines. Tribal knowledge creates bottlenecks.

**How**: Use clear job and asset names that describe *what*, not *how*. Add descriptive metadata to assets. Document architectural decisions in code comments or ADRs. Keep the Dagster UI readable.

---

## Project Standards

### Share Reusable Components via CollectionFlow

**Why**: Copying code between client projects creates maintenance nightmares. Bug fixes and improvements don't propagate.

**What goes in CollectionFlow**: IO managers, harvesters (API clients, database connectors), notification managers, metadata helpers.

**What stays in client repos**: Client-specific business logic, transformation pipelines, custom validation rules.

**Quality bar**: 100% test coverage, complete type hints, docstrings with usage examples.

### Start Every Project with Consistent Scaffolding

**Why**: Consistent tooling reduces onboarding friction and enables shared CI/CD patterns.

**How**: Every client project starts from the same template: UV for package management, Ruff + ty for linting/type checking, Lefthook for pre-commit hooks, GitHub Actions for CI/CD.

