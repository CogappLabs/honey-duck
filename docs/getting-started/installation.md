# Getting Started with Dagster - For New Developers

A hands-on guide to understanding and working with Dagster in the honey-duck project.

## What is Dagster?

Dagster is a **data orchestration platform** that helps you build, test, and monitor data pipelines. Think of it as a smart way to organize your data processing code with:

-  **Visual lineage**: See how data flows through your pipeline
- ⚡ **Incremental execution**: Only recompute what changed
-  **Observability**: Track what's happening in real-time
- **Testing**: Test your pipeline logic before deploying
- 🔄 **Scheduling**: Run pipelines on a schedule or trigger manually

## Core Concepts (5-Minute Version)

### 1. Assets - The Building Blocks

An **asset** is a piece of data you want to create and keep track of. In honey-duck:

```python
@dg.asset
def sales_transform(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """This function creates the 'sales_transform' asset."""
    # Your transformation logic here
    result = load_and_transform_sales()
    return result
```

**Key points:**
- The function name becomes the asset name
- Return value is the asset's data
- Dagster tracks when it was created and what it depends on

### 2. Dependencies - How Assets Connect

Assets can depend on other assets:

```python
@dg.asset
def sales_output(
    context: dg.AssetExecutionContext,
    sales_transform: pl.DataFrame,  # ← Dependency!
) -> pl.DataFrame:
    """This asset depends on sales_transform."""
    # sales_transform is automatically loaded and passed in
    return sales_transform.filter(pl.col("price") > 1000)
```

Dagster automatically:
- Runs `sales_transform` before `sales_output`
- Passes the data between them
- Shows the connection in the UI

### 3. Context - Your Pipeline Toolbox

The `context` parameter gives you access to:

```python
@dg.asset
def my_asset(context: dg.AssetExecutionContext):
    context.log.info("Logging messages")  # Logging
    context.add_output_metadata({"rows": 100})  # Metadata
    # ... your code
```

### 4. IO Managers - How Data is Stored

IO Managers handle saving/loading data between assets:

```python
# Dagster automatically:
# 1. Saves sales_transform as Parquet (PolarsParquetIOManager)
# 2. Loads it when sales_output needs it
# 3. Shows file paths in the UI
```

You don't need to write file I/O code - Dagster handles it!

## Your First Asset (Step by Step)

Let's create a simple asset that filters artworks by price:

### Step 1: Create the Asset

Add to `src/honey_duck/defs/polars/my_first_asset.py`:

```python
import dagster as dg
import polars as pl
from cogapp_deps.dagster import read_harvest_tables_lazy, add_dataframe_metadata
from honey_duck.defs.shared.resources import PathsResource

@dg.asset(
    kinds={"polars"},
    group_name="tutorial",
)
def expensive_artworks(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Find artworks priced over $1 million."""

    # Load data from harvest
    tables = read_harvest_tables_lazy(
        HARVEST_PARQUET_DIR,
        ("artworks_raw", ["artwork_id", "title", "price_usd"]),
        asset_name="expensive_artworks",
    )

    # Filter for expensive artworks
    result = (
        tables["artworks_raw"]
        .filter(pl.col("price_usd") > 1_000_000)
        .sort("price_usd", descending=True)
        .collect()
    )

    # Add metadata (shows up in UI)
    add_dataframe_metadata(
        context,
        result,
        total_value=float(result["price_usd"].sum()),
        avg_price=float(result["price_usd"].mean()),
    )

    context.log.info(f"Found {len(result)} expensive artworks")

    return result
```

### Step 2: Register the Asset

The asset is **automatically registered** via `load_from_defs_folder()`:

```python
# No manual registration needed - assets in defs/ are auto-discovered

defs = dg.Definitions(
    assets=[
        # ... existing assets
        expensive_artworks,  # ← Add this
    ],
    # ... rest of config
)
```

### Step 3: Materialize It

Start Dagster UI:
```bash
uv run dg dev
```

Open http://localhost:3000 and:
1. Go to **Assets** tab
2. Find `expensive_artworks`
3. Click **Materialize**
4. Watch it run!

### Step 4: View the Results

After materialization:
- Click on the asset to see metadata
- Click **"Show Markdown"** on preview
- See logs in the **Logs** tab
- View file location in **Metadata**

## Common Patterns

### Pattern 1: Reading from Harvest Tables

```python
from cogapp_deps.dagster import read_harvest_tables_lazy

# Read multiple tables at once
tables = read_harvest_tables_lazy(
    HARVEST_PARQUET_DIR,
    ("sales_raw", ["sale_id", "sale_price_usd"]),  # Validates columns
    ("artworks_raw", ["artwork_id", "title"]),
    asset_name="my_asset",  # For error messages
)

# Use lazy operations, then collect
result = (
    tables["sales_raw"]
    .join(tables["artworks_raw"], on="artwork_id")
    .filter(pl.col("sale_price_usd") > 1000)
    .collect()  # ← Execute here
)
```

### Pattern 2: Adding Metadata

```python
from cogapp_deps.dagster import add_dataframe_metadata, track_timing

@dg.asset
def my_transform(context: dg.AssetExecutionContext) -> pl.DataFrame:
    # Track execution time automatically
    with track_timing(context, "transformation"):
        result = expensive_operation()

    # Add rich metadata
    add_dataframe_metadata(
        context,
        result,
        unique_customers=result["customer_id"].n_unique(),
        total_revenue=float(result["revenue"].sum()),
        date_range=f"{result['date'].min()} to {result['date'].max()}",
    )

    return result
```

### Pattern 3: Writing JSON Output

```python
from cogapp_deps.dagster import write_json_and_return
from honey_duck.defs.shared.resources import OutputPathsResource

@dg.asset(kinds={"polars", "json"})
def my_output(
    context: dg.AssetExecutionContext,
    my_transform: pl.DataFrame,
) -> pl.DataFrame:
    """Filter and write to JSON."""
    filtered = my_transform.filter(pl.col("amount") > 100)

    return write_json_and_return(
        filtered,
        JSON_OUTPUT_DIR / "my_output.json",
        context,
        extra_metadata={"threshold": 100},
    )
```

### Pattern 4: External Dependencies

When your asset depends on external files (not other assets):

```python
from honey_duck.defs.shared.helpers import STANDARD_HARVEST_DEPS

@dg.asset(
    deps=STANDARD_HARVEST_DEPS,  # CSV/SQLite files
    kinds={"dlt"},
)
def my_harvest(context: dg.AssetExecutionContext):
    # This asset depends on dlt_harvest_* assets
    # which load from CSV files
    pass
```

## Understanding the Asset Graph

In Dagster UI, you'll see nodes and edges:

```
csv_sales → dlt_harvest_sales → sales_transform → sales_output
                                      ↓
                                 sales_joined
```

- **Nodes** (boxes): Assets
- **Edges** (arrows): Dependencies
- **Colors**:
  - Green: Successfully materialized
  - Gray: Not yet materialized
  - Yellow: In progress
  - Red: Failed

**Tip**: Click "View lineage" to see the full dependency graph!

## Testing Your Assets

### Method 1: In the UI

1. **Materialize single asset**: Click asset → Materialize
2. **Materialize with dependencies**: Check "Materialize upstream assets"
3. **View results**: Click asset → See metadata/preview/logs

### Method 2: In Tests

```python
# tests/test_my_asset.py
from dagster import materialize
from honey_duck.defs.polars.my_first_asset import expensive_artworks

def test_expensive_artworks():
    # Materialize the asset
    result = materialize([expensive_artworks])
    assert result.success

    # Check output
    output = result.output_for_node("expensive_artworks")
    assert len(output) > 0
    assert all(output["price_usd"] > 1_000_000)
```

### Method 3: Via CLI

```bash
# Materialize specific asset
uv run dg launch --assets expensive_artworks

# Materialize with dependencies
uv run dg launch --assets sales_output --select +sales_output

# Run full job
uv run dg launch --job polars_pipeline
```

## Debugging Tips

### Check Logs

```python
@dg.asset
def my_asset(context: dg.AssetExecutionContext):
    context.log.info("Starting processing...")
    context.log.debug(f"Data shape: {df.shape}")
    context.log.warning("Low data quality detected")
    # context.log.error("Critical error!")  # For errors
```

View logs in:
- **Dagster UI**: Asset page → Logs tab
- **Terminal**: When running `dg dev`

### Inspect Intermediate Data

```python
@dg.asset
def my_asset(context: dg.AssetExecutionContext):
    result = transform_data()

    # Add preview to metadata
    context.add_output_metadata({
        "preview": dg.MetadataValue.md(
            result.head(10).to_pandas().to_markdown()
        ),
    })

    return result
```

### Check File Paths

Assets stored by IO managers are in:
```
data/output/storage/
  ├── expensive_artworks/
  │   └── expensive_artworks
  └── sales_transform/
      └── sales_transform
```

Read them directly for debugging:
```python
import polars as pl
df = pl.read_parquet("data/output/storage/expensive_artworks/expensive_artworks")
```

## Common Mistakes & Fixes

### Mistake 1: Forgetting to Return Data

```python
@dg.asset
def my_asset(context):
    result = transform_data()
    # Forgot to return!  ← BUG
```

**Fix:**
```python
@dg.asset
def my_asset(context):
    result = transform_data()
    return result  # ← Always return!
```

### Mistake 2: Wrong Parameter Name

```python
@dg.asset
def sales_output(context, sales_data: pl.DataFrame):  # ← Wrong name
    # Dagster looks for asset named "sales_data", not "sales_transform"
```

**Fix:**
```python
@dg.asset
def sales_output(context, sales_transform: pl.DataFrame):  # ← Match asset name
    pass
```

### Mistake 3: Modifying Input Data

```python
@dg.asset
def my_asset(context, input_data: pl.DataFrame):
    input_data = input_data.filter(...)  # May cause issues
    return input_data
```

**Fix:**
```python
@dg.asset
def my_asset(context, input_data: pl.DataFrame):
    result = input_data.filter(...)  # Create new variable
    return result
```

### Mistake 4: Not Using Helper Functions

```python
@dg.asset
def my_asset(context):
    # Manual file reading - error-prone!
    df = pl.read_parquet("data/output/dlt/harvest_parquet/sales_raw.parquet")
```

**Fix:**
```python
@dg.asset
def my_asset(context):
    # Use helper - automatic validation!
    tables = read_harvest_tables_lazy(
        HARVEST_PARQUET_DIR,
        ("sales_raw", ["sale_id", "price"]),
        asset_name="my_asset",
    )
```

## Next Steps

### 1. Explore Existing Assets

Read these files to see patterns:
- `src/honey_duck/defs/polars/assets.py` - Clean Polars implementation
- `src/honey_duck/defs/duckdb/assets.py` - SQL-based approach
- See [Polars Patterns](../user-guide/polars-patterns.md) for detailed pattern documentation

### 2. Learn Multi-Assets

For related outputs:
```python
@dg.multi_asset(
    outs={
        "sales_summary": dg.AssetOut(),
        "sales_details": dg.AssetOut(),
    }
)
def sales_pipeline(context):
    # Create both assets in one function
    summary = create_summary()
    details = create_details()

    yield dg.Output(summary, output_name="sales_summary")
    yield dg.Output(details, output_name="sales_details")
```

### 3. Add Asset Checks

Validate data quality:
```python
from dagster import AssetCheckResult, asset_check

@asset_check(asset=expensive_artworks)
def check_price_range(context):
    """Ensure all artworks are actually expensive."""
    df = load_asset(expensive_artworks)

    min_price = df["price_usd"].min()

    return AssetCheckResult(
        passed=min_price > 1_000_000,
        metadata={"min_price": float(min_price)},
    )
```

### 4. Add Schedules

Run assets on a schedule:
```python
from dagster import ScheduleDefinition

daily_schedule = ScheduleDefinition(
    name="daily_pipeline",
    cron_schedule="0 2 * * *",  # 2 AM daily
    target=[expensive_artworks],
)
```

## Resources

- **Dagster Docs**: https://docs.dagster.io
- **Polars Docs**: https://pola-rs.github.io/polars/
- **honey-duck Patterns**: `docs/defs/PATTERNS.md`
- **CLI Reference**: `docs/CLI_REFERENCE.md` (see below)
- **Troubleshooting**: `docs/TROUBLESHOOTING.md` (see below)

## Quick Reference

```bash
# Start Dagster UI
uv run dg dev

# Materialize asset
uv run dg launch --assets my_asset

# Run job
uv run dg launch --job polars_pipeline

# Run tests
uv run pytest

# Check asset graph
# → Open http://localhost:3000 → Assets → View lineage
```

**Welcome to Dagster!** Start small, experiment in the UI, and build up from there. 
