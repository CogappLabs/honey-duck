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
    ├── assets_polars.py  # Pure Polars implementation (split into steps)
    ├── assets_duckdb.py  # Pure DuckDB SQL implementation
    ├── assets_polars_fs.py # Polars with FilesystemIOManager
    ├── assets_polars_ops.py # Graph-backed assets with ops (detailed observability)
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
# Polars implementation (split into steps for intermediate persistence)
dlt_harvest_* (shared) ──→ sales_joined_polars ──→ sales_transform_polars ──→ sales_output_polars
                       └──→ artworks_catalog_polars ──┐
                       └──→ artworks_sales_agg_polars ─┼──→ artworks_transform_polars ──→ artworks_output_polars
                       └──→ artworks_media_polars ─────┘

# Polars ops implementation (graph-backed assets - single asset with ops)
dlt_harvest_* (shared) ──→ sales_transform_polars_ops ──→ sales_output_polars_ops
                       └──→ artworks_transform_polars_ops ──→ artworks_output_polars_ops

# Other implementations (single transform asset per pipeline)
dlt_harvest_* ──→ sales_transform_<impl> ──→ sales_output_<impl>
              └──→ artworks_transform_<impl> ──→ artworks_output_<impl>
```

**Implementations**:
- original (processor classes)
- polars (split into steps for intermediate persistence)
- polars_ops (graph-backed assets with ops for detailed observability)
- duckdb (pure SQL)
- polars_fs (FilesystemIOManager)

**Jobs**:
- `full_pipeline` - Original implementation with processor classes
- `polars_pipeline` - Pure Polars expressions (split into steps)
- `polars_ops_pipeline` - Graph-backed assets with ops (detailed observability)
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
- DuckDB is used for: SQL transformations via processors and dlt harvest storage
- dlt harvest: CSV/SQLite loaded to DuckDB `raw` schema (via dagster-dlt)
- DuckDB processors now return Polars DataFrames (using `.pl()` instead of `.df()`)
- Inter-asset communication: Uses Parquet files (not DuckDB tables)

**Intermediate Asset Persistence**:
- Polars pipeline splits transformations into logical steps
- Each step persists to Parquet via PolarsParquetIOManager
- Enables debugging individual transformation steps
- All assets return Polars DataFrames for IO manager compatibility

**Graph-Backed Assets with Ops** (Detailed Observability Pattern):
- Use `@dg.graph_asset` decorator to compose ops into a single asset
- Each transformation step is an `@dg.op` with detailed logging
- Provides op-level observability without intermediate persistence
- Single asset appears in lineage graph (cleaner graph)
- Best for: Proof-of-concept projects, complex pipelines needing detailed logs
- Example: `assets_polars_ops.py`

**When to use graph-backed assets vs split assets:**
- **Split assets** (`polars_pipeline`): When you need intermediate Parquet files for debugging
- **Graph-backed assets** (`polars_ops_pipeline`): When you need detailed op logs but not intermediate persistence
- Users can choose based on their debugging/observability needs

## Adding New Assets

**See `defs/PATTERNS.md` for detailed examples with minimal boilerplate.**

### Quick Start (3 lines of code!)

```python
from honey_duck.defs.helpers import transform_asset, read_harvest_tables, add_standard_metadata

@transform_asset()  # Automatic error handling, timing, validation!
def my_transform(context) -> pl.DataFrame:
    tables = read_harvest_tables(
        ("sales_raw", ["sale_id", "sale_price_usd"]),
        asset_name="my_transform",
    )
    result = tables["sales_raw"].filter(pl.col("sale_price_usd") > 1000).collect()
    add_standard_metadata(context, result)
    return result
```

**What you get automatically:**
- ✅ Error handling with clear messages
- ✅ Table/column validation
- ✅ Timing metadata
- ✅ Standard metadata (record count, preview)
- ✅ Proper group_name, kinds, and dependencies

### Traditional Approach (Still Supported)

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
