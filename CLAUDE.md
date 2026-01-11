# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync

# Optional: Enable persistent run history (copy .env.example to .env)
cp .env.example .env

# Start Dagster UI
uv run dagster dev

# Run pipeline via CLI
uv run dagster job execute -j full_pipeline

# Run tests
uv run pytest
```

## Structure

```
honey_duck/
├── __init__.py           # Package metadata
└── defs/
    ├── definitions.py    # Combined Dagster Definitions
    ├── dlt_sources.py    # dlt source configuration
    ├── dlt_assets.py     # dagster-dlt asset wrapper
    ├── assets.py         # Original transform/output (processor classes)
    ├── assets_polars.py  # Pure Polars implementation
    ├── assets_pandas.py  # Pure Pandas implementation
    ├── assets_duckdb.py  # Pure DuckDB SQL implementation
    ├── assets_polars_fs.py # Polars with FilesystemIOManager
    ├── resources.py      # Path constants and configuration
    ├── constants.py      # Business constants (thresholds, tiers)
    ├── schemas.py        # Pandera validation schemas
    ├── jobs.py           # Job definitions
    └── checks.py         # Asset checks

cogapp_deps/              # Utilities (simulates external package)
├── dagster/              # Dagster helpers
│   └── io.py             # write_json_output helper
└── processors/           # DataFrame processors
    ├── polars/           # PolarsFilterProcessor, PolarsStringProcessor
    └── duckdb/           # DuckDBQueryProcessor, DuckDBSQLProcessor, etc.

data/
├── input/                # Source CSV files
└── output/               # Generated outputs (organized by type)
    ├── json/             # Asset JSON outputs
    ├── storage/          # IO manager Parquet files (inter-asset communication)
    └── dlt/              # DuckDB database for dlt harvest

dagster_home/             # Dagster persistence directory
```

## Key Patterns

**Asset graph** (5 parallel implementations sharing harvest layer):
```
dlt_harvest_* (shared) ──→ sales_transform_<impl> ──→ sales_output_<impl>
                       └──→ artworks_transform_<impl> ──→ artworks_output_<impl>
```

**Implementations**: original (processor classes), polars, duckdb, polars_fs

**Jobs**:
- `full_pipeline` - Original implementation with processor classes
- `polars_pipeline` - Pure Polars expressions
- `duckdb_pipeline` - Pure DuckDB SQL
- `polars_fs_pipeline` - Polars with FilesystemIOManager

**IO Managers**:
- `io_manager` (default): PolarsParquetIOManager - stores DataFrames as Parquet files in `data/output/storage/`
- `fs_io_manager`: FilesystemIOManager - pickles DataFrames to files (used by polars_fs implementation)

**Storage Organization**:
```
data/output/
├── json/     - Asset JSON outputs (sales_output.json, artworks_output.json, etc.)
├── storage/  - IO manager Parquet files (inter-asset communication)
└── dlt/      - DuckDB database for dlt harvest (dagster.duckdb)
```

**DuckDB Usage**:
- DuckDB is still used for: SQL transformations via processors and dlt harvest storage
- dlt harvest: CSV/SQLite loaded to DuckDB `raw` schema
- Inter-asset communication: Now uses Parquet (not DuckDB tables)

**Note**: All asset implementations return Polars DataFrames for compatibility with PolarsParquetIOManager.

## Adding New Assets

1. Add asset function in `defs/assets.py`
2. Decorate with `@dg.asset(kinds={"polars"}, group_name="...")`
3. **Return type must be `pl.DataFrame`** for PolarsParquetIOManager compatibility
4. Add rich metadata via `context.add_output_metadata()`
5. Add to `definitions.py` assets list

**Important**: Assets must return Polars DataFrames. If using DuckDB internally, convert at the end:
```python
# DuckDB example
result = conn.sql("SELECT * FROM table").pl()
return result
```

## Adding New dlt Sources

1. Add resource to `dlt_sources.py` honey_duck_source function
2. Asset key will be auto-generated as `dlt_honey_duck_harvest_{name}`
3. Table created in `raw` schema
