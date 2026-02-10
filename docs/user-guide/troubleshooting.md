---
title: Troubleshooting
description: Solutions to common Dagster, Polars, and DuckDB issues including import errors, memory problems, and pipeline failures.
---

# Troubleshooting Guide - Common Issues & Solutions

Solutions to common problems when working with Dagster in honey-duck.

## Table of Contents

- [Asset Errors](#asset-errors)
- [Dependency Issues](#dependency-issues)
- [Data Validation Errors](#data-validation-errors)
- [IO Manager Issues](#io-manager-issues)
- [Performance Issues](#performance-issues)
- [UI/Server Issues](#uiserver-issues)
- [Environment Issues](#environment-issues)

---

## Asset Errors

### Error: "DagsterInvalidDefinitionError: Cannot annotate context parameter"

```python
# WRONG
@dg.asset
def my_asset(context: dg.AssetExecutionContext) -> dict:
    pass
```

**Problem**: Some Dagster decorators don't allow typed context parameters.

**Solution**:
```python
# For regular assets - use type annotation
@dg.asset
def my_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    pass

# For dynamic assets - remove type annotation
@dg.asset(deps=["other_asset"])
def notification_asset(context) -> dict:  # ← No type annotation
    pass
```

---

### Error: "Asset 'sales_transform' has no materializations"

**Problem**: You're trying to use an asset that hasn't been materialized yet.

**Solution**:
```bash
# Materialize the missing asset first
uv run dg launch --assets sales_transform

# Or materialize with all dependencies
uv run dg launch --assets sales_output --select +sales_output
```

**In UI**: Click "Materialize" button on the asset page.

---

### Error: "No such asset: 'sales_data'"

```python
# WRONG - Parameter name doesn't match asset name
@dg.asset
def my_output(context, sales_data: pl.DataFrame):  # Looking for 'sales_data'
    pass
```

**Problem**: Parameter name must exactly match the asset name.

**Solution**:
```python
# CORRECT - Parameter matches asset name
@dg.asset
def my_output(context, sales_transform: pl.DataFrame):  # Matches 'sales_transform'
    pass
```

**Check available assets**:
```bash
uv run dg list defs
```

---

## Dependency Issues

### Error: "Circular dependency detected"

**Problem**: Assets depend on each other in a loop:
```
A → B → C → A  # ← Circular!
```

**Solution**:

1. **Find the cycle**:
```bash
uv run dg list defs --show-deps
```

2. **Break the cycle** by refactoring:
```python
# Instead of:
# A depends on B
# B depends on A  ← Circular!

# Do:
# C = shared logic
# A depends on C
# B depends on C
```

---

### Error: "ImportError: cannot import name 'my_asset'"

**Problem**: Missing import or circular import.

**Solution**:

1. **Check the import**:
```python
# In definitions.py
from honey_duck.defs.polars.my_assets import my_asset  # ← Verify file/folder
```

2. **Avoid circular imports**:
```python
# WRONG - Don't import definitions in asset files
from honey_duck.defs.definitions import defs

# CORRECT - Only import utilities
from honey_duck.defs.shared.resources import PathsResource
```

3. **Verify file exists**:
```bash
ls src/honey_duck/defs/polars/my_assets.py
```

---

## Data Validation Errors

### Error: "MissingTableError: Table 'sales_raw' not found in schema 'raw'"

**Problem**: Harvest data hasn't been loaded or table name is wrong.

**Solution**:

1. **Check if harvest data exists**:
```bash
ls data/output/dlt/harvest_parquet/
```

2. **Materialize harvest assets first**:
```bash
uv run dg launch --assets dlt_harvest_sales
```

3. **Check table names**:
```python
from cogapp_libs.dagster import read_harvest_tables_lazy

# This validates table exists and shows available tables in error message
tables = read_harvest_tables_lazy(
    paths.harvest_dir,
    ("sales_raw", []),  # ← Will list available if wrong
    asset_name="my_asset",
)
```

**Error message shows available tables**:
```
MissingTableError: Table 'sales_raw' not found.
Available tables: ['artworks_raw', 'artists_raw', 'media']
```

---

### Error: "MissingColumnError: Column 'sale_price' not found in table 'sales_raw'"

**Problem**: Column name is wrong or doesn't exist in data.

**Solution**:

1. **Check actual columns**:
```bash
uv run python -c "
import polars as pl
df = pl.read_parquet('data/output/dlt/harvest_parquet/sales_raw.parquet')
print(df.columns)
"
```

2. **Fix column name**:
```python
# Error says available columns
MissingColumnError: Column 'sale_price' not found.
Available columns: ['sale_id', 'sale_price_usd', 'artwork_id']

# Use correct name
("sales_raw", ["sale_price_usd"])  # ← Not 'sale_price'
```

3. **Use helper validation**:
```python
from cogapp_libs.dagster import read_harvest_tables_lazy

# Validates columns exist
tables = read_harvest_tables_lazy(
    paths.harvest_dir,
    ("sales_raw", ["sale_id", "sale_price_usd"]),  # ← Auto-validated
    asset_name="my_asset",
)
```

---

### Error: "ColumnNotFoundError" (Polars)

```python
# Polars error during transformation
polars.exceptions.ColumnNotFoundError: sale_price
```

**Problem**: Typo in column name or column was dropped in earlier operation.

**Solution**:

1. **Add debug logging**:
```python
@dg.asset
def my_asset(context: dg.AssetExecutionContext):
    result = load_data()

    # Debug: print columns
    context.log.info(f"Columns available: {result.columns}")

    # Now use correct column
    result = result.filter(pl.col("sale_price_usd") > 100)
    return result
```

2. **Check for typos**:
```python
# Common typos:
pl.col("sale_price")      # Missing _usd
pl.col("sale_price_usd")  # Correct
```

---

## IO Manager Issues

### Error: "Failed to pickle DataFrame"

**Problem**: Trying to use wrong IO manager for data type.

**Solution**:

Ensure asset returns Polars DataFrame for PolarsParquetIOManager:
```python
@dg.asset
def my_asset(context) -> pl.DataFrame:  # ← Polars, not Pandas
    # If using DuckDB
    result = conn.sql("SELECT * FROM table").pl()  # ← Use .pl() not .df()
    return result
```

---

### Error: "FileNotFoundError: No such file or directory: 'data/output/storage/...'"

**Problem**: IO manager trying to load asset that doesn't exist.

**Solution**:

1. **Materialize upstream asset**:
```bash
uv run dg launch --assets upstream_asset
```

2. **Or materialize with dependencies**:
```bash
uv run dg launch --assets my_asset --select +my_asset
```

3. **Check file system**:
```bash
# Check if asset files exist
ls -R data/output/storage/
```

---

## Performance Issues

### Issue: "Asset taking too long to materialize"

**Symptoms**: Asset runs for minutes/hours.

**Solutions**:

1. **Use lazy evaluation** (Polars):
```python
# SLOW - Collects too early
df = pl.read_parquet(path).collect()  # ← Loads all
df = df.filter(pl.col("price") > 1000)  # Works on full dataset

# FAST - Push down filters
df = pl.scan_parquet(path)  # ← Lazy
df = df.filter(pl.col("price") > 1000)  # Filter planned
df = df.collect()  # ← Only loads filtered data
```

2. **Profile your code**:
```python
from cogapp_libs.dagster import track_timing

@dg.asset
def my_asset(context):
    with track_timing(context, "loading"):
        data = load_data()

    with track_timing(context, "transforming"):
        result = transform(data)

    # Check metadata to see which part is slow
    return result
```

3. **Check data size**:
```python
@dg.asset
def my_asset(context):
    result = transform()

    context.log.info(f"Result size: {len(result):,} rows")
    context.log.info(f"Memory: {result.estimated_size('mb'):.2f} MB")

    return result
```

---

### Issue: "Out of memory errors"

**Solutions**:

1. **Stream data with lazy operations**:
```python
# Instead of loading entire file
df = pl.scan_parquet(path).filter(...).collect()
```

2. **Process in batches**:
```python
@dg.asset
def process_large_data(context):
    total_rows = 0

    for batch in pl.read_parquet(path, use_pyarrow=True, n_rows=10000):
        processed = transform(batch)
        total_rows += len(processed)

    context.log.info(f"Processed {total_rows:,} rows")
```

3. **Use DuckDB for large datasets**:
```python
# DuckDB streams results
conn.sql("""
    SELECT * FROM 'large_file.parquet'
    WHERE price > 1000
""").pl()  # Only loads filtered results
```

---

## UI/Server Issues

### Error: "Failed to connect to Dagster"

**Problem**: Dagster server not running or wrong port.

**Solution**:

1. **Start the server**:
```bash
uv run dg dev
```

2. **Check the port**:
```bash
# Default port
http://localhost:3000

# Custom port
uv run dg dev --port 3001
http://localhost:3001
```

3. **Check process**:
```bash
# Is Dagster running?
ps aux | grep dagster

# Kill stuck process
pkill -f dagster
```

---

### Error: "Code location failed to load"

**Problem**: Python syntax error or import error in definitions.

**Solution**:

1. **Check error in UI**: Look at code location error message.

2. **Test import directly**:
```bash
uv run python -c "from honey_duck.defs.definitions import defs"
```

3. **Common causes**:
```python
# Missing comma
assets=[
    asset1
    asset2  # Missing comma
]

# Indentation error
@dg.asset
def my_asset(context):
result = ...  # Wrong indentation

# Circular import
from honey_duck.defs.definitions import defs  # In asset file
```

---

### Issue: "UI is slow or unresponsive"

**Solutions**:

1. **Clear browser cache**: Ctrl+Shift+R (Chrome/Firefox)

2. **Restart Dagster**:
```bash
pkill -f dagster
uv run dg dev
```

3. **Check for too many runs**:
```bash
# Delete old runs
uv run dagster run delete --all -j my_job
```

---

## Environment Issues

### Error: "ModuleNotFoundError: No module named 'polars'"

**Problem**: Dependencies not installed.

**Solution**:

```bash
# Install dependencies
uv sync

# Verify installation
uv run python -c "import polars; print(polars.__version__)"
```

---

### Error: "Permission denied" when writing files

**Problem**: No write permissions to output directory.

**Solution**:

1. **Check permissions**:
```bash
ls -la data/output/
```

2. **Fix permissions**:
```bash
chmod -R u+w data/output/
```

3. **Or create directory**:
```bash
mkdir -p data/output/json data/output/storage data/output/dlt
```

---

### Error: "Database is locked" (DuckDB)

**Problem**: Multiple processes trying to write to DuckDB at once.

**Solution**:

In honey-duck, output assets have `deps=["other_output"]` to enforce ordering:

```python
@dg.asset(
    deps=["artworks_output"],  # ← Wait for artworks_output first
)
def sales_output(context, sales_transform: pl.DataFrame):
    # Won't run until artworks_output completes
    pass
```

**Why**: DuckDB only allows one writer at a time.

---

## Getting More Help

### Enable Debug Logging

```bash
# Set log level
export DAGSTER_CLI_LOG_LEVEL=DEBUG
uv run dg dev

# Or in code
@dg.asset
def my_asset(context):
    context.log.set_level(logging.DEBUG)
    context.log.debug("Detailed debug info")
```

### Check Dagster Logs

```bash
# View recent run logs
uv run dagster run logs <run_id>

# Server logs when running dg dev
# → Check terminal output
```

### Validate Your Code

```bash
# Check Python syntax
uv run python -m py_compile src/honey_duck/defs/polars/my_assets.py

# Check imports
uv run python -c "from honey_duck.defs.definitions import defs"

# Run tests
uv run pytest -xvs
```

### Still Stuck?

1. **Check Dagster Docs**: https://docs.dagster.io
2. **Check honey-duck docs**: [Getting Started](../getting-started/quick-start.md)
3. **View example implementations**: `src/honey_duck/defs/polars/assets.py`
4. **Read error messages carefully**: They often include helpful suggestions!

---

## Common Error Message Patterns

### Pattern 1: "Expected X, got Y"

→ **Type mismatch**: Check return type annotations

### Pattern 2: "Asset 'X' not found"

→ **Typo or missing import**: Check asset name spelling

### Pattern 3: "Table/Column 'X' not found"

→ **Data validation error**: Use helper functions for auto-validation

### Pattern 4: "Failed to load"

→ **Missing materialization**: Materialize upstream assets first

### Pattern 5: "Circular dependency"

→ **Design issue**: Refactor to break the cycle

---

**Pro Tip**: Most errors have helpful messages. Read them carefully -- they often tell you exactly what's wrong.
