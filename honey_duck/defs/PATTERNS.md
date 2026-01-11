# ETL Pipeline Patterns

This document shows how to create new harvests, transforms, and outputs with minimal boilerplate.

## Pattern 1: Simple Transform Asset

**Boilerplate-free transform with automatic error handling:**

```python
from honey_duck.defs.helpers import (
    transform_asset,
    read_harvest_tables,
    add_standard_metadata,
    CONFIG,
)

@transform_asset(group_name="transform_polars")
def my_transform(context) -> pl.DataFrame:
    """Transform description - automatic error handling included!"""

    # Read tables with validation (one line!)
    tables = read_harvest_tables(
        ("sales_raw", ["sale_id", "sale_price_usd"]),
        ("artworks_raw", ["artwork_id", "title"]),
        asset_name="my_transform",  # For error context
    )

    # Transform logic
    result = (
        tables["sales_raw"]
        .join(tables["artworks_raw"], on="artwork_id")
        .filter(pl.col("sale_price_usd") > CONFIG.min_sale_value_usd)
        .collect()
    )

    # Add metadata (one line!)
    add_standard_metadata(
        context,
        result,
        unique_artworks=result["artwork_id"].n_unique(),
    )

    return result
```

**What you get automatically:**
- ✅ Error handling with clear messages
- ✅ Table/column validation
- ✅ Timing metadata
- ✅ Standard metadata (record count, preview, columns)
- ✅ Proper group_name and kinds
- ✅ Harvest dependencies

## Pattern 2: Output Asset

**Boilerplate-free output with freshness policy:**

```python
from honey_duck.defs.helpers import output_asset
from cogapp_deps.dagster import write_json_output

@output_asset(group_name="output_polars", freshness_hours=24)
def my_output(
    context,
    my_transform: pl.DataFrame,  # Depends on transform
) -> pl.DataFrame:
    """Output description - automatic error handling included!"""

    # Filter
    result = my_transform.filter(pl.col("value") > 1000)

    # Write JSON
    write_json_output(
        result,
        CONFIG.json_output_dir / "my_output.json",
        context,
    )

    return result
```

**What you get automatically:**
- ✅ Error handling
- ✅ Freshness policy (24 hours)
- ✅ Proper group_name and kinds (polars, json)
- ✅ Timing logs

## Pattern 3: Multi-Table Transform

**Reading multiple tables efficiently:**

```python
@transform_asset()
def complex_transform(context) -> pl.DataFrame:
    """Join multiple tables with validation."""

    # Read all tables at once
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

**For detailed observability:**

```python
from honey_duck.defs.helpers import read_harvest_tables

@dg.op
def load_and_join(context) -> pl.DataFrame:
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
def add_metrics(context, df: pl.DataFrame) -> pl.DataFrame:
    """Op: Add computed metrics."""
    result = df.with_columns(
        (pl.col("sale_price_usd") * 2).alias("doubled")
    )
    context.log.info(f"Added metrics to {len(result):,} records")
    return result

@dg.graph_asset(kinds={"polars"}, group_name="transform_polars_ops")
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

**Errors are handled automatically:**

```python
@transform_asset()
def my_asset(context) -> pl.DataFrame:
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
| Transform | `@transform_asset()` | Error handling, timing, validation, deps |
| Output | `@output_asset()` | Error handling, freshness, timing |
| Read tables | `read_harvest_tables()` | Validation, error context |
| Metadata | `add_standard_metadata()` | Count, preview, columns |
| Config | `CONFIG.property` | Validated on import |

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
@dg.asset(
    kinds={"polars"},
    group_name="transform_polars",
    deps=[dg.AssetKey("dlt_harvest_sales_raw"), ...],
)
def my_asset(context):
    start_time = time.perf_counter()
    try:
        # logic
        elapsed = time.perf_counter() - start_time
        context.add_output_metadata({"time": elapsed})
    except Exception as e:
        context.log.error(f"Failed: {e}")
        raise
```

✅ **Do use decorators:**
```python
# Good - automatic boilerplate
@transform_asset()
def my_asset(context):
    # logic
    return result
```
