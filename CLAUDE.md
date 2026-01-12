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
uv run dagster job execute -j processors_pipeline

# Run tests
uv run pytest
```

## Structure

```
honey_duck/
├── __init__.py           # Package metadata
└── defs/
    ├── definitions.py    # Combined Dagster Definitions (6 jobs)
    ├── dlt_sources.py    # dlt source configuration
    ├── dlt_assets.py     # dagster-dlt asset wrapper
    ├── assets.py         # Original transform/output (processor classes)
    ├── assets_polars.py  # Pure Polars with split intermediate steps
    ├── assets_duckdb.py  # Pure DuckDB SQL implementation
    ├── assets_polars_fs.py   # Polars variant (same logic, different group)
    ├── assets_polars_ops.py  # Graph-backed assets with ops
    ├── assets_polars_multi.py # Multi-asset pattern
    ├── helpers.py        # STANDARD_HARVEST_DEPS constant
    ├── resources.py      # ConfigurableResource classes for path config
    ├── constants.py      # Business constants (thresholds, tiers)
    ├── schemas.py        # Pandera validation schemas
    ├── jobs.py           # Job definitions (6 jobs)
    └── checks.py         # Asset checks (Pandera + quality)

cogapp_deps/              # Utilities (simulates external package)
├── dagster/              # Dagster helpers
│   ├── __init__.py       # Exports write_json_output, track_timing, etc.
│   ├── io.py             # DuckDBPandasPolarsIOManager (unused)
│   └── helpers.py        # track_timing, add_dataframe_metadata, altair_to_metadata, etc.
└── processors/           # DataFrame processors
    ├── pandas/           # PandasReplaceOnConditionProcessor
    ├── polars/           # PolarsFilterProcessor, PolarsStringProcessor
    └── duckdb/           # DuckDBJoinProcessor, DuckDBWindowProcessor

data/
├── input/                # Source CSV files
├── output/               # Final outputs (JSON files per implementation)
├── storage/              # IO manager intermediate storage (Parquet)
└── harvest/              # dlt raw Parquet data + pipeline state

dagster_home/             # Dagster persistence directory
```

## Key Patterns

**Asset graph** (6 parallel implementations sharing harvest layer):

```
                      ┌──→ sales_transform ──→ sales_output (original)
                      ├──→ sales_joined_polars ──→ sales_transform_polars ──→ sales_output_polars
dlt_harvest_* (shared)├──→ sales_transform_duckdb ──→ sales_output_duckdb
                      ├──→ sales_transform_polars_fs ──→ sales_output_polars_fs
                      ├──→ sales_transform_polars_ops ──→ sales_output_polars_ops
                      └──→ sales_pipeline_multi ──→ sales_output_polars_multi
                      (same pattern for artworks with more intermediate steps in polars)
```

**Implementations** (6 total):
1. **original** - Processor classes (assets.py)
2. **polars** - Split intermediate steps for observability (assets_polars.py)
3. **duckdb** - Pure DuckDB SQL queries (assets_duckdb.py)
4. **polars_fs** - Polars variant, same logic different group (assets_polars_fs.py)
5. **polars_ops** - Graph-backed assets with ops for detailed observability (assets_polars_ops.py)
6. **polars_multi** - Multi-asset pattern for tightly coupled steps (assets_polars_multi.py)

**Jobs** (6 total):
- `processors_pipeline` - Original implementation with processor classes
- `polars_pipeline` - Pure Polars with intermediate step assets
- `duckdb_pipeline` - Pure DuckDB SQL
- `polars_fs_pipeline` - Polars variant
- `polars_ops_pipeline` - Graph-backed assets (ops)
- `polars_multi_pipeline` - Multi-asset pattern

**ConfigurableResource Pattern**:
All path configuration uses Dagster's ConfigurableResource for runtime injection:
- `PathsResource` - harvest_dir, data_dir
- `OutputPathsResource` - output paths per implementation
- `DatabaseResource` - DuckDB database path

**IO Managers**:
- `io_manager` (default): PolarsParquetIOManager - stores Polars DataFrames as Parquet
- `pandas_io_manager`: DuckDBPandasIOManager - stores Pandas DataFrames in DuckDB

**Storage pattern**:
- Harvest layer: dlt writes Parquet to `data/harvest/raw/`
- Transform/output layers: Parquet files in `data/storage/`
- Final outputs: JSON files in `data/output/`
- Pandas pipeline: DuckDB tables (exception, uses pandas_io_manager)

**Output dependency ordering**:
Sales outputs depend on artworks outputs (`deps=["artworks_output_*"]`) to ensure
deterministic asset ordering for consistent JSON output. This is a workaround for
non-deterministic parallel execution in Dagster.

## Adding New Assets

1. Add asset function in appropriate `defs/assets_*.py` file
2. Import `STANDARD_HARVEST_DEPS` from `helpers.py` for consistent dependencies
3. Decorate with `@dg.asset(kinds={"polars"}, group_name="...", deps=STANDARD_HARVEST_DEPS)`
4. Inject resources: `paths: PathsResource`, `output_paths: OutputPathsResource`
5. Add rich metadata via `context.add_output_metadata()`
6. Add to `definitions.py` assets list

## Adding New dlt Sources

1. Add resource to `dlt_sources.py` honey_duck_source function
2. Asset key will be auto-generated as `dlt_harvest_{name}`
3. Data written to `data/harvest/raw/{name}/`

## Polars Best Practices

**Consolidate `with_columns` chains** - Multiple expressions in a single `with_columns` run in parallel:
```python
# ✅ GOOD - parallel execution
result = df.with_columns(
    pl.col("a").alias("x"),
    pl.col("b").alias("y"),
    pl.col("c").str.to_uppercase(),
)

# ❌ BAD - sequential execution
result = df.with_columns(pl.col("a").alias("x"))
result = result.with_columns(pl.col("b").alias("y"))
```

**Use `sort_by` inside aggregations** - `sort()` before `group_by()` doesn't guarantee order within groups:
```python
# ✅ GOOD - sorted within each group
result = df.group_by("id").agg(
    pl.struct("a", "b").sort_by("a").alias("items")
)

# ❌ BAD - sort order not preserved after group_by
result = df.sort("a").group_by("id").agg(pl.struct("a", "b").alias("items"))
```

**Prefer semi-joins over `is_in()`** - Avoids early materialization:
```python
# ✅ GOOD - stays lazy
result = sales.join(artworks.select("id"), on="id", how="semi")

# ❌ BAD - forces collect of ids
result = sales.filter(pl.col("id").is_in(artworks["id"]))
```

## Visualization Helpers

Add charts and tables to Dagster asset metadata:

```python
from cogapp_deps.dagster import altair_to_metadata, table_preview_to_metadata

# Bar chart (Altair via Polars .plot)
chart = df.plot.bar(x="category", y="count")
context.add_output_metadata(altair_to_metadata(chart, "distribution_chart"))

# Markdown table preview
context.add_output_metadata(
    table_preview_to_metadata(df.head(5), "preview", "Top 5 Results")
)
```

## Known Limitations

**Graph-backed ops** (assets_polars_ops.py): Individual ops within a @graph_asset
cannot receive ConfigurableResource injection. These ops use a module-level
`_DEFAULT_HARVEST_DIR` constant. This is a Dagster limitation - ops don't participate
in resource injection the way assets do.
