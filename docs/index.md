# Honey Duck

A Polars/Parquet data pipeline with Dagster orchestration, using dlt for data ingestion and DuckDB for SQL queries.

## Quick Start

```bash
# Install dependencies
uv sync

# Start Dagster UI
uv run dg dev

# Or run pipeline via CLI
uv run dg launch --job processors_pipeline

# List all definitions
uv run dg list defs
```

Open [http://localhost:3000](http://localhost:3000) and click **"Materialize all"** to run the pipeline!

## Features

- **6 parallel implementations** sharing a common harvest layer
- **Lazy evaluation** with Polars for optimal performance
- **Visualization helpers** for Dagster asset metadata (Altair charts, markdown tables)
- **Reusable processors** for DuckDB, Polars, and Pandas transforms
- **Comprehensive validation** with clear error messages

## Architecture

```
CSV files → dlt harvest → Transform assets → Output assets → JSON
                 ↓              ↓                 ↓
           Parquet raw    Parquet storage    JSON files
```

## Documentation

<div class="grid cards" markdown>

-   :material-clock-fast:{ .lg .middle } __Getting Started__

    ---

    Get up and running in 15 minutes with the quick start tutorial.

    [:octicons-arrow-right-24: Quick Start](getting-started/quick-start.md)

-   :material-book-open-variant:{ .lg .middle } __User Guide__

    ---

    Best practices, Polars patterns, and performance tuning.

    [:octicons-arrow-right-24: Best Practices](user-guide/best-practices.md)

-   :material-api:{ .lg .middle } __API Reference__

    ---

    Auto-generated documentation from source code.

    [:octicons-arrow-right-24: API Docs](api/index.md)

-   :material-connection:{ .lg .middle } __Integrations__

    ---

    Elasticsearch, S3, PostgreSQL, and more.

    [:octicons-arrow-right-24: Integrations](integrations/elasticsearch.md)

</div>

## Implementations

| Suffix | Description | Features |
|--------|-------------|----------|
| (none) | Original with processor classes | Processor pattern |
| `_polars` | Pure Polars with intermediate steps | Lazy evaluation, visualization |
| `_duckdb` | Pure DuckDB SQL queries | SQL-based transforms |
| `_polars_fs` | Polars variant (different group) | Same logic, separate group |
| `_polars_ops` | Graph-backed assets with ops | Detailed observability |
| `_polars_multi` | Multi-asset pattern | Tightly coupled steps |
