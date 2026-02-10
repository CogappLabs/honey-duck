---
title: Tutorial - Your First Asset
description: Step-by-step tutorial to create your first Dagster asset with Polars transformations and DuckDB queries.
---

# Quick Start Tutorial: Your First Asset

**Goal**: Get from `git clone` to writing your first custom Dagster asset in 15 minutes.

**What you'll learn**:
1. Install and run the project
2. Understand the asset graph
3. Create your first transform asset
4. Add custom business logic
5. View results in Dagster UI

---

## Step 1: Install and Run (5 minutes)

### Clone and Setup

```bash
# Clone the repository
git clone https://github.com/CogappLabs/honey-duck.git
cd honey-duck

# Install dependencies with uv (fast!)
uv sync

# Optional: Enable persistent run history
cp .env.example .env
# Edit .env and set DAGSTER_HOME to an absolute path like /home/user/dagster_home
```

### Start Dagster UI

```bash
# Start the Dagster development server
uv run dg dev
```

Open http://localhost:3000 in your browser. You should see:

- **Assets** tab: Shows all pipeline assets
- **Jobs** tab: Shows executable job definitions
- **Runs** tab: Shows execution history

### Run Your First Pipeline

1. Click **Assets** in the top navigation
2. Click **Materialize all** button in the top-right
3. Watch the pipeline execute!

**What happened?**:
- `dlt_harvest_*` assets loaded CSV files into Parquet
- `*_transform_*` assets joined and transformed data
- `*_output_*` assets wrote JSON files to `data/output/json/`

---

## Step 2: Understand the Asset Graph (3 minutes)

### View the Lineage

In Dagster UI:
1. Click **Assets** → **View global asset lineage**
2. See the data flow:

```
dlt_harvest_sales_raw ──┐
dlt_harvest_artworks_raw ┼──→ sales_joined_polars ──→ sales_transform_polars ──→ sales_output_polars
dlt_harvest_artists_raw ─┘
```

### What Each Layer Does

| Layer | Purpose | Example |
|-------|---------|---------|
| **Harvest** | Load raw data | CSV → Parquet |
| **Transform** | Join, compute | Add price metrics |
| **Output** | Filter, export | High-value sales to JSON |

---

## Step 3: Create Your First Asset (5 minutes)

Let's create an asset that finds artworks by specific artists.

### Create the Asset File

Open `src/honey_duck/defs/polars/assets.py` and add this at the end:

```python
@dg.asset(
    kinds={"polars"},
    deps=STANDARD_HARVEST_DEPS,
    group_name="transform_polars",
)
def artist_artworks_tutorial(
    context: dg.AssetExecutionContext,
    paths: PathsResource,  # Injected resource for path configuration
) -> pl.DataFrame:
    """Find all artworks by Vincent van Gogh and Claude Monet.

    This is a tutorial asset demonstrating:
    - Reading harvest tables
    - Filtering with Polars
    - Adding metadata
    """

    with track_timing(context, "loading and filtering"):
        # Read artworks and artists tables
        tables = read_harvest_tables_lazy(
            paths.harvest_dir,  # Use injected resource
            ("artworks_raw", ["artwork_id", "title", "artist_id", "year"]),
            ("artists_raw", ["artist_id", "name", "nationality"]),
            asset_name="artist_artworks_tutorial",
        )

        # Join and filter for specific artists
        result = (
            tables["artworks_raw"]
            .join(tables["artists_raw"], on="artist_id", how="left")
            .filter(
                pl.col("name").is_in(["Vincent van Gogh", "Claude Monet"])
            )
            .sort("year")
            .collect()
        )

    # Add metadata
    add_dataframe_metadata(
        context,
        result,
        unique_artists=result["name"].n_unique(),
        year_range=f"{result['year'].min()} to {result['year'].max()}",
    )

    context.log.info(
        f"Found {len(result)} artworks by Van Gogh and Monet"
    )

    return result
```

### Register the Asset

The asset is automatically registered! Dagster uses Python module discovery.

### Materialize Your Asset

1. Go to Dagster UI (http://localhost:3000)
2. **Refresh** the page (or it auto-refreshes)
3. Find `artist_artworks_tutorial` in the asset list
4. Click the asset name → **Materialize** button
5. Watch it execute!

### View the Results

1. Click on the materialization in the runs list
2. See the metadata:
   - Record count
   - Unique artists
   - Year range
   - Preview table

---

## Step 4: Add Custom Business Logic (2 minutes)

Let's add a custom computed column for artwork age.

### Modify the Asset

Replace the previous asset with this enhanced version:

```python
@dg.asset(
    kinds={"polars"},
    deps=STANDARD_HARVEST_DEPS,
    group_name="transform_polars",
)
def artist_artworks_tutorial(
    context: dg.AssetExecutionContext,
    paths: PathsResource,
) -> pl.DataFrame:
    """Find artworks by famous artists with age computation."""

    with track_timing(context, "transformation"):
        tables = read_harvest_tables_lazy(
            paths.harvest_dir,
            ("artworks_raw", ["artwork_id", "title", "artist_id", "year", "medium"]),
            ("artists_raw", ["artist_id", "name", "nationality"]),
            asset_name="artist_artworks_tutorial",
        )

        current_year = 2024

        result = (
            tables["artworks_raw"]
            .join(tables["artists_raw"], on="artist_id", how="left")
            .filter(
                pl.col("name").is_in([
                    "Vincent van Gogh",
                    "Claude Monet",
                    "Pablo Picasso",
                ])
            )
            # Add computed columns
            .with_columns([
                (pl.lit(current_year) - pl.col("year")).alias("artwork_age"),
                pl.when(pl.col("year") < 1900)
                  .then(pl.lit("19th Century"))
                  .otherwise(pl.lit("20th Century+"))
                  .alias("era"),
            ])
            .sort(["name", "year"])
            .collect()
        )

    # Enhanced metadata
    add_dataframe_metadata(
        context,
        result,
        unique_artists=result["name"].n_unique(),
        year_range=f"{result['year'].min()}-{result['year'].max()}",
        oldest_artwork=result["year"].min(),
        avg_age=f"{result['artwork_age'].mean():.1f} years",
        media_types=result["medium"].n_unique(),
    )

    context.log.info(
        f"Found {len(result)} artworks across {result['name'].n_unique()} artists"
    )

    return result
```

### Materialize Again

1. Go to Dagster UI
2. Click `artist_artworks_tutorial` asset
3. Click **Materialize**
4. See the new metadata!

---

## Step 5: Create an Output Asset (Optional)

Let's export the results to JSON.

### Add Output Asset

```python
@dg.asset(
    kinds={"polars", "json"},
    group_name="output_polars",
    freshness_policy=dg.FreshnessPolicy(maximum_lag_minutes=24 * 60),
)
def artist_artworks_output_tutorial(
    context: dg.AssetExecutionContext,
    artist_artworks_tutorial: pl.DataFrame,
) -> pl.DataFrame:
    """Export famous artworks to JSON."""

    # Filter for 19th century works
    result = artist_artworks_tutorial.filter(
        pl.col("era") == "19th Century"
    )

    # Write to JSON
    output_path = Path("data/output/json/famous_artists.json")
    write_json_output(
        result,
        output_path,
        context,
        extra_metadata={
            "filtered_from": len(artist_artworks_tutorial),
            "filter_criterion": "19th Century",
        },
    )

    context.log.info(f"Exported {len(result)} artworks to {output_path}")

    return result
```

### Materialize Both Assets

1. In Dagster UI, select both assets:
   - `artist_artworks_tutorial`
   - `artist_artworks_output_tutorial`
2. Click **Materialize selected**
3. Watch them execute in order (dependency resolution!)
4. Check `data/output/json/famous_artists.json` for your output

---

## Next Steps

### Congratulations! 

You've created your first Dagster asset with:
- Data loading from Parquet
- Polars transformations
- Custom business logic
- Metadata tracking
- JSON output

### What to Learn Next

**Dive Deeper:**

- [Polars Patterns](../user-guide/polars-patterns.md) - DataFrame best practices
- [Performance Tuning](../user-guide/performance.md) - Optimize your pipelines
- [Best Practices](../user-guide/best-practices.md) - Production guidelines

**Explore Features:**

- [Asset Checks](https://docs.dagster.io/guides/test/asset-checks) - Data quality validation
- [Schedules](https://docs.dagster.io/guides/automate/schedules/) - Automated runs
- [Sensors](https://docs.dagster.io/guides/automate/sensors/) - Event-driven pipelines

**Advanced Topics:**

- [Elasticsearch Integration](../integrations/elasticsearch.md) - Full-text search
- [API Bulk Harvesting](../integrations/api-harvesting.md) - Voyage AI
- [Sitemap Generation](../integrations/sitemaps.md) - SEO optimization

---

## Common Questions

### Q: Where do I find my output files?

**A**: Check these directories:
- **JSON outputs**: `data/output/json/`
- **Parquet storage**: `data/output/storage/` (IO manager files)
- **DuckDB database**: See `DatabaseResource` in `shared/resources.py` for the configured path

### Q: How do I debug my asset?

**A**: Use these techniques:

```python
@dg.asset
def my_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    # Add logging
    context.log.info("Starting transformation...")

    result = transform()

    # Log preview
    context.log.info(f"Preview:\n{result.head(5)}")

    # Log shape
    context.log.info(f"Shape: {result.shape}")

    return result
```

### Q: Can I use pandas instead of Polars?

**A**: Yes! Both work with the same IO manager:

```python
import pandas as pd

@dg.asset
def my_asset(context) -> pd.DataFrame:
    # Pandas operations
    return df
```

### Q: How do I add a custom dependency?

**A**: Add it to `pyproject.toml`:

```toml
[project]
dependencies = [
    # ... existing deps
    "your-package>=1.0",
]
```

Then run: `uv sync`

### Q: Where should I put custom code?

**Guidelines**:
- **Project-specific logic** → `src/honey_duck/defs/` (organized by technology: polars/, duckdb/, etc.)
- **Reusable utilities** → `cogapp_libs/`

---

## Troubleshooting

### Issue: "Table not found"

**Cause**: Harvest assets haven't run yet.

**Solution**:
1. Materialize harvest assets first:
   - `dlt_harvest_sales_raw`
   - `dlt_harvest_artworks_raw`
   - `dlt_harvest_artists_raw`
2. Then materialize your asset

### Issue: "Column not found"

**Cause**: Wrong column name or table.

**Solution**: Check available columns:
```python
df = pl.scan_parquet(paths.harvest_dir / "sales_raw")
print(df.collect_schema())  # Shows all columns
```

### Issue: Asset not appearing in UI

**Cause**: Syntax error or not registered.

**Solution**:
1. Check console for Python errors
2. Make sure asset is in a file imported by `definitions.py`
3. Restart Dagster: `Ctrl+C` then `uv run dg dev`

---

## Cheat Sheet

### Essential Imports

```python
import dagster as dg
import polars as pl
from cogapp_libs.dagster import (
    read_harvest_tables_lazy,
    track_timing,
    add_dataframe_metadata,
    write_json_output,
)
from honey_duck.defs.shared.helpers import STANDARD_HARVEST_DEPS
from honey_duck.defs.shared.resources import PathsResource
```

### Basic Asset Template

```python
@dg.asset(
    kinds={"polars"},
    deps=STANDARD_HARVEST_DEPS,
    group_name="transform_polars",
)
def my_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Asset description."""

    with track_timing(context, "operation"):
        # Your logic here
        result = pl.DataFrame({"id": [1, 2, 3]})

    add_dataframe_metadata(context, result)
    return result
```

### Common Polars Operations

```python
# Filter
df.filter(pl.col("value") > 100)

# Select columns
df.select(["id", "name", "value"])

# Add column
df.with_columns((pl.col("a") + pl.col("b")).alias("c"))

# Join
df1.join(df2, on="id", how="left")

# Group by
df.group_by("category").agg(pl.sum("amount"))

# Sort
df.sort(["date", "id"], descending=[True, False])
```

---

**You're ready to build data pipelines.**

For help:
- Dagster Slack: https://dagster.io/slack
- Polars Discord: https://discord.gg/4UfP7XY7YB
- Documentation: See README.md for full guide list
