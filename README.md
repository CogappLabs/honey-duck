# honey-duck

A DuckDB-backed DataFrame pipeline supporting pandas, polars, and SQL processors.

## Quick Start

```bash
# Install dependencies
uv sync

# Run default pipeline
uv run python -m honey_duck

# Dry run (show plan only)
uv run python -m honey_duck --dry-run

# Run specific pipeline
uv run python -m honey_duck pipelines/full.py

# Use file-based DuckDB (for debugging/inspection)
uv run python -m honey_duck --db data/output/pipeline.duckdb
```

## Architecture

```
CSV/JSON → DuckDB table(s)
                ↓
        Processor 1 (SQL lookup - no data loading)
                ↓
           DuckDB table
                ↓
        Processor 2 (pandas)
                ↓
           DuckDB table
                ↓
        Processor 3 (polars)
                ↓
           JSON/CSV/Parquet
```

## Pipeline Definition

Pipelines are defined in Python files with three exports:

```python
# pipelines/full.py

from honey_duck import (
    DuckDBLookupProcessor,
    DuckDBSQLProcessor,
    PandasFilterProcessor,
    PandasUppercaseProcessor,
    PolarsWindowProcessor,
)

SOURCES = {
    "pipeline_data": "data/input/sales.csv",
    "artworks": "data/input/artworks.csv",
    "artists": "data/input/artists.csv",
}

PROCESSORS = [
    DuckDBLookupProcessor("artworks", left_on="artwork_id", right_on="artwork_id", columns=["title", "artist_id"]),
    DuckDBLookupProcessor("artists", left_on="artist_id", right_on="artist_id", columns=["name", "nationality"]),
    DuckDBSQLProcessor("SELECT *, sale_price_usd - price_usd as diff FROM pipeline_data"),
    PandasUppercaseProcessor("name"),
    PandasFilterProcessor("sale_price_usd", min_value=30_000_000),
    PolarsWindowProcessor(partition_by="name", order_by="sale_price_usd"),
]

OUTPUT = "data/output/full.json"

# Optional: use file-based DuckDB instead of in-memory
# DB_PATH = "data/output/pipeline.duckdb"
```

## Processor Types

| Type | Base Class | Description |
|------|------------|-------------|
| pandas | `Processor` | Load → process → store |
| polars | `PolarsProcessor` | Load → process → store |
| SQL | `DuckDBProcessor` | Runs directly in DuckDB (zero overhead) |

## Built-in Processors

**DuckDB (SQL)**
- `DuckDBLookupProcessor` - SQL join with lookup table
- `DuckDBSQLProcessor` - Arbitrary SQL
- `DuckDBAggregateProcessor` - SQL GROUP BY (collapses rows)

**Pandas**
- `PandasFilterProcessor` - Filter rows by column value
- `PandasUppercaseProcessor` - Uppercase a string column

**Polars**
- `PolarsAggregateProcessor` - Group by with aggregations (collapses rows)
- `PolarsWindowProcessor` - Window functions (adds rank column)
