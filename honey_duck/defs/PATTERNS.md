# ETL Pipeline Patterns

This document shows how to create new harvests, transforms, and outputs with minimal boilerplate.

## Pattern 1: Simple Transform Asset (Recommended)

**Transform with automatic timing, validation, and metadata using standard Dagster decorators:**

```python
import dagster as dg
import polars as pl
from cogapp_deps.dagster import (
    read_harvest_tables_lazy,
    track_timing,
    add_dataframe_metadata,
)
from honey_duck.defs.helpers import AssetGroups, STANDARD_HARVEST_DEPS
from honey_duck.defs.resources import HARVEST_PARQUET_DIR
from honey_duck.defs.constants import MIN_SALE_VALUE_USD

@dg.asset(
    kinds={"polars"},
    group_name=AssetGroups.TRANSFORM_POLARS,
    deps=STANDARD_HARVEST_DEPS,
)
def my_transform(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Transform sales with artworks - demonstrates helper patterns."""

    with track_timing(context, "transformation"):
        # Read multiple tables in one call with validation
        tables = read_harvest_tables_lazy(
            HARVEST_PARQUET_DIR,
            ("sales_raw", ["sale_id", "sale_price_usd"]),
            ("artworks_raw", ["artwork_id", "title"]),
            asset_name="my_transform",
        )

        # Transform logic
        result = (
            tables["sales_raw"]
            .join(tables["artworks_raw"], on="artwork_id")
            .filter(pl.col("sale_price_usd") > MIN_SALE_VALUE_USD)
            .collect()
        )

    # Add metadata (automatic: record count, preview, columns)
    add_dataframe_metadata(
        context,
        result,
        unique_artworks=result["artwork_id"].n_unique(),
    )

    return result
```

**What you get from helper functions:**
- ✅ Automatic timing tracking and logging ("Completed transformation in 123.4ms")
- ✅ Error handling with clear, actionable messages
- ✅ Table/column validation (raises MissingTableError, MissingColumnError)
- ✅ Standard metadata (record count, preview, columns, processing_time_ms)
- ✅ Lists available tables/columns when validation fails

## Pattern 2: Output Asset

**Output with freshness policy using standard Dagster decorators:**

```python
import dagster as dg
import polars as pl
from honey_duck.defs.helpers import AssetGroups
from honey_duck.defs.config import CONFIG
from cogapp_deps.dagster import write_json_output

@dg.asset(
    kinds={"polars", "json"},
    group_name=AssetGroups.OUTPUT_POLARS,
    freshness_policy=dg.FreshnessPolicy(maximum_lag_minutes=24 * 60),
)
def my_output(
    context: dg.AssetExecutionContext,
    my_transform: pl.DataFrame,  # Depends on transform
) -> pl.DataFrame:
    """Output description - write JSON to output directory."""

    # Filter
    result = my_transform.filter(pl.col("value") > 1000)

    # Write JSON (automatic error handling)
    write_json_output(
        result,
        CONFIG.json_output_dir / "my_output.json",
        context,
    )

    return result
```

**Standard Dagster features:**
- ✅ Freshness policy (24 hours)
- ✅ Proper group_name and kinds (polars, json)
- ✅ Automatic dependency resolution from function parameters

## Pattern 3: Multi-Table Transform

**Reading multiple tables efficiently:**

```python
import dagster as dg
import polars as pl
from honey_duck.defs.helpers import (
    read_harvest_tables,
    add_standard_metadata,
    AssetGroups,
    STANDARD_HARVEST_DEPS,
)

@dg.asset(
    kinds={"polars"},
    group_name=AssetGroups.TRANSFORM_POLARS,
    deps=STANDARD_HARVEST_DEPS,
)
def complex_transform(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Join multiple tables with validation."""

    # Read all tables at once (automatic validation)
    tables = read_harvest_tables(
        ("sales_raw", ["sale_id", "artwork_id", "sale_price_usd"]),
        ("artworks_raw", ["artwork_id", "artist_id", "title"]),
        ("artists_raw", ["artist_id", "name", "nationality"]),
        asset_name="complex_transform",
    )

    # Transform with all tables
    result = (
        tables["sales_raw"]
        .join(tables["artworks_raw"], on="artwork_id")
        .join(tables["artists_raw"], on="artist_id")
        .collect()
    )

    add_standard_metadata(context, result)
    return result
```

## Pattern 4: Graph-Backed Asset with Ops

**For detailed observability (standard Dagster graph-backed assets):**

```python
import dagster as dg
import polars as pl
from honey_duck.defs.helpers import read_harvest_tables, AssetGroups

@dg.op
def load_and_join(context: dg.OpExecutionContext) -> pl.DataFrame:
    """Op: Load and join tables."""
    tables = read_harvest_tables(
        ("sales_raw", ["sale_id", "sale_price_usd"]),
        ("artworks_raw", ["artwork_id", "title"]),
        asset_name="load_and_join",
    )

    result = (
        tables["sales_raw"]
        .join(tables["artworks_raw"], on="artwork_id")
        .collect()
    )

    context.log.info(f"Joined {len(result):,} records")
    return result

@dg.op
def add_metrics(context: dg.OpExecutionContext, df: pl.DataFrame) -> pl.DataFrame:
    """Op: Add computed metrics."""
    result = df.with_columns(
        (pl.col("sale_price_usd") * 2).alias("doubled")
    )
    context.log.info(f"Added metrics to {len(result):,} records")
    return result

@dg.graph_asset(kinds={"polars"}, group_name=AssetGroups.TRANSFORM_POLARS_OPS)
def my_graph_asset() -> pl.DataFrame:
    """Graph-backed asset with op-level logs."""
    df = load_and_join()
    result = add_metrics(df)
    return result
```

## Common Configurations

**Access configuration values:**

```python
from honey_duck.defs.config import CONFIG

# Use throughout your assets
min_value = CONFIG.min_sale_value_usd  # 30_000_000
db_path = CONFIG.duckdb_path
output_dir = CONFIG.json_output_dir
```

**Override via environment variables:**

```bash
export MIN_SALE_VALUE_USD=50000000
export PRICE_TIER_BUDGET_MAX_USD=1000000
export FRESHNESS_HOURS=48
```

## Error Handling

**Helper functions provide automatic error handling:**

```python
import dagster as dg
import polars as pl
from honey_duck.defs.helpers import (
    read_harvest_tables,
    AssetGroups,
    STANDARD_HARVEST_DEPS,
)

@dg.asset(
    kinds={"polars"},
    group_name=AssetGroups.TRANSFORM_POLARS,
    deps=STANDARD_HARVEST_DEPS,
)
def my_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    # If table doesn't exist:
    # → MissingTableError with list of available tables

    # If columns are missing:
    # → MissingColumnError with list of available columns

    # If database doesn't exist:
    # → FileNotFoundError with helpful message

    tables = read_harvest_tables(
        ("nonexistent_table", ["col1"]),  # Will fail with clear message
        asset_name="my_asset",
    )
    return tables["nonexistent_table"].collect()
```

**Example error output:**

```
MissingTableError: [my_asset] Table 'raw.nonexistent_table' not found in database.
Available tables: ['sales_raw', 'artworks_raw', 'artists_raw', 'media'].
Did you run the harvest job first?
Run: uv run dagster job execute -j full_pipeline
```

## Adding to Definitions

**Register your new assets:**

```python
# honey_duck/defs/definitions.py

from .my_new_assets import my_transform, my_output

defs = dg.Definitions(
    assets=[
        # ... existing assets
        my_transform,
        my_output,
    ],
    # ... rest of config
)
```

## Quick Reference

| Task | Helper | Automatic Features |
|------|--------|-------------------|
| Read tables (Parquet) | `read_harvest_tables_lazy()` | Batch reading, validation, error context |
| Read tables (DuckDB) | `read_harvest_tables()` | Validation, error context, actionable errors |
| Timing | `track_timing()` context | Auto logging, processing_time_ms metadata |
| Metadata | `add_dataframe_metadata()` | Count, preview, columns, processing time |
| Config | `CONFIG.property` | Validated on import, auto-creates directories |
| Constants | `AssetGroups.*` | Standard group names |
| Dependencies | `STANDARD_HARVEST_DEPS` | Common harvest dependencies |

### Helper Details

**Batch Reading (Parquet):**
```python
# Instead of individual reads:
sales = read_harvest_table_lazy(HARVEST_PARQUET_DIR, "sales_raw", ...)
artworks = read_harvest_table_lazy(HARVEST_PARQUET_DIR, "artworks_raw", ...)

# Do batch read:
tables = read_harvest_tables_lazy(
    HARVEST_PARQUET_DIR,
    ("sales_raw", ["sale_id", "sale_price_usd"]),  # With column validation
    ("artworks_raw", None),  # No column validation
    asset_name="my_asset",
)
sales = tables["sales_raw"]
artworks = tables["artworks_raw"]
```

**Automatic Timing:**
```python
# Instead of manual timing:
start_time = time.perf_counter()
result = expensive_operation()
elapsed_ms = (time.perf_counter() - start_time) * 1000
context.add_output_metadata({"processing_time_ms": round(elapsed_ms, 2)})
context.log.info(f"Completed in {elapsed_ms:.1f}ms")

# Use context manager:
with track_timing(context, "processing"):
    result = expensive_operation()
    # Automatically logs and adds metadata
```

## Anti-Patterns to Avoid

❌ **Don't manually read tables without validation:**
```python
# Bad - no validation
conn = duckdb.connect(DUCKDB_PATH)
df = conn.sql("SELECT * FROM raw.sales_raw").pl()
```

✅ **Do use validated helpers:**
```python
# Good - automatic validation
tables = read_harvest_tables(
    ("sales_raw", ["sale_id", "sale_price_usd"]),
    asset_name="my_asset",
)
```

❌ **Don't manually add boilerplate:**
```python
# Bad - repetitive boilerplate
import duckdb

@dg.asset(
    kinds={"polars"},
    group_name="transform_polars",
    deps=[dg.AssetKey("dlt_harvest_sales_raw"), ...],
)
def my_asset(context):
    conn = duckdb.connect(DUCKDB_PATH)
    # Manual table checking
    tables = conn.sql("SHOW TABLES").pl()
    if "sales_raw" not in tables["name"].to_list():
        raise ValueError("Table not found")  # No context!

    df = conn.sql("SELECT * FROM raw.sales_raw").pl()
    # Manual metadata
    context.add_output_metadata({
        "count": len(df),
        "columns": df.columns,
    })
    return df
```

✅ **Do use helper functions:**
```python
# Good - helpers provide validation and metadata
import dagster as dg
import polars as pl
from honey_duck.defs.helpers import (
    read_harvest_tables,
    add_standard_metadata,
    AssetGroups,
    STANDARD_HARVEST_DEPS,
)

@dg.asset(
    kinds={"polars"},
    group_name=AssetGroups.TRANSFORM_POLARS,
    deps=STANDARD_HARVEST_DEPS,
)
def my_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    tables = read_harvest_tables(
        ("sales_raw", ["sale_id", "sale_price_usd"]),
        asset_name="my_asset",
    )
    result = tables["sales_raw"].collect()
    add_standard_metadata(context, result)
    return result
```
