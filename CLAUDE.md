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
│   └── io.py             # DuckDBPandasPolarsIOManager
└── processors/           # DataFrame processors
    ├── pandas/           # PandasReplaceOnConditionProcessor
    ├── polars/           # PolarsFilterProcessor, PolarsStringProcessor
    └── duckdb/           # DuckDBJoinProcessor, DuckDBWindowProcessor

data/
├── input/                # Source CSV files
└── output/               # Generated output (*.duckdb, *.json)

dagster_home/             # Dagster persistence directory
```

## Key Patterns

**Asset graph** (5 parallel implementations sharing harvest layer):
```
dlt_harvest_* (shared) ──→ sales_transform_<impl> ──→ sales_output_<impl>
                       └──→ artworks_transform_<impl> ──→ artworks_output_<impl>
```

**Implementations**: original (processor classes), polars, pandas, duckdb, polars_fs

**Jobs**:
- `full_pipeline` - Original implementation
- `polars_pipeline` - Pure Polars expressions
- `pandas_pipeline` - Pure Pandas expressions
- `duckdb_pipeline` - Pure DuckDB SQL
- `polars_fs_pipeline` - Polars with FilesystemIOManager

**IO Managers**:
- `io_manager` (default): DuckDBPandasPolarsIOManager - stores DataFrames as DuckDB tables
- `fs_io_manager`: FilesystemIOManager - pickles DataFrames to files

**dlt harvest**: CSV files loaded to DuckDB `raw` schema via dagster-dlt integration.

## Adding New Assets

1. Add asset function in `defs/assets.py`
2. Decorate with `@dg.asset(kinds={"duckdb"}, group_name="...")`
3. Add rich metadata via `context.add_output_metadata()`
4. Add to `definitions.py` assets list

## Adding New dlt Sources

1. Add resource to `dlt_sources.py` honey_duck_source function
2. Asset key will be auto-generated as `dlt_honey_duck_harvest_{name}`
3. Table created in `raw` schema
