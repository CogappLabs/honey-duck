# honey-duck

A Polars/Parquet data pipeline with Dagster orchestration, using dlt for data ingestion and DuckDB for SQL queries.

## Quick Start

```bash
# Install dependencies
uv sync

# Start Dagster UI
uv run dg dev
```

Open http://localhost:3000 and click **"Materialize all"** to run the pipeline.

## Documentation

**[View Full Documentation](https://cogapplabs.github.io/honey-duck)**

- [Getting Started](https://cogapplabs.github.io/honey-duck/getting-started/tutorial/) - Installation and first steps
- [User Guide](https://cogapplabs.github.io/honey-duck/user-guide/best-practices/) - Best practices and patterns
- [API Reference](https://cogapplabs.github.io/honey-duck/api/) - Processor and helper docs
- [Integrations](https://cogapplabs.github.io/honey-duck/integrations/elasticsearch/) - Elasticsearch, S3, and more

**For contributors**: See [CLAUDE.md](CLAUDE.md) for project structure and coding conventions.

## Architecture

```
CSV files → dlt harvest → Transform assets → Output assets → JSON
                ↓               ↓                  ↓
           Parquet raw     Parquet files      JSON files
           (data/harvest)  (data/storage)     (data/output)
```

6 parallel implementations share the harvest layer, each demonstrating different Dagster patterns:

```
dlt_harvest_* (shared) ──→ transform_<impl> ──→ output_<impl>
```

| Implementation | Pattern |
|----------------|---------|
| `processors_pipeline` | Processor classes |
| `polars_pipeline` | Pure Polars with intermediate steps |
| `duckdb_pipeline` | Pure DuckDB SQL |
| `polars_ops_pipeline` | Graph-backed assets with ops |
| `polars_multi_pipeline` | Multi-asset pattern |

See the [Architecture Guide](https://cogapplabs.github.io/honey-duck/getting-started/architecture/) for details.
