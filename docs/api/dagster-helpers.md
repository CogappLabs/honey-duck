---
title: Dagster Helpers
description: Helper functions for Dagster assets including metadata builders, timing utilities, and visualization helpers.
---

# Dagster Helpers

Utilities for building Dagster assets with less boilerplate.

!!! info "External Documentation"
    - **Dagster**: [docs.dagster.io](https://docs.dagster.io/) | [Assets Guide](https://docs.dagster.io/concepts/assets/software-defined-assets)
    - **Polars**: [pola.rs/docs](https://docs.pola.rs/) | [API Reference](https://docs.pola.rs/api/python/stable/reference/)
    - **DuckDB**: [duckdb.org/docs](https://duckdb.org/docs/)

## Data Loading

### read_harvest_table_lazy

::: cogapp_libs.dagster.validation.read_harvest_table_lazy

### read_harvest_tables_lazy

::: cogapp_libs.dagster.validation.read_harvest_tables_lazy

### read_parquet_table_lazy

::: cogapp_libs.dagster.validation.read_parquet_table_lazy

---

## Validation

### validate_dataframe

::: cogapp_libs.dagster.validation.validate_dataframe

---

## Metadata

### add_dataframe_metadata

::: cogapp_libs.dagster.helpers.add_dataframe_metadata

### track_timing

::: cogapp_libs.dagster.helpers.track_timing

### write_json_output

::: cogapp_libs.dagster.io.write_json_output

---

## Visualization

### altair_to_metadata

::: cogapp_libs.dagster.helpers.altair_to_metadata

### table_preview_to_metadata

::: cogapp_libs.dagster.helpers.table_preview_to_metadata

---

## Column Lineage

Helpers for tracking column-level data flow and displaying example values in the Dagster UI.

### Lineage DSL

Build column lineage definitions with a compact, declarative syntax.

#### build_lineage

::: cogapp_libs.dagster.lineage.build_lineage

**Example:**

```python
from cogapp_libs.dagster.lineage import build_lineage
import dagster as dg

SALES_RAW = dg.AssetKey("dlt_harvest_sales_raw")
ARTWORKS_RAW = dg.AssetKey("dlt_harvest_artworks_raw")
ARTISTS_RAW = dg.AssetKey("dlt_harvest_artists_raw")

SALES_TRANSFORM_LINEAGE = build_lineage(
    passthrough={
        "sale_id": SALES_RAW,
        "artwork_id": SALES_RAW,
        "title": ARTWORKS_RAW,
        "artist_name": ARTISTS_RAW,
    },
    rename={
        "artwork_year": (ARTWORKS_RAW, "year"),
        "list_price_usd": (ARTWORKS_RAW, "price_usd"),
    },
    computed={
        "price_diff": [(SALES_RAW, "sale_price_usd"), (ARTWORKS_RAW, "price_usd")],
    },
)

context.add_output_metadata({
    "dagster/column_lineage": SALES_TRANSFORM_LINEAGE,
})
```

#### passthrough_lineage

::: cogapp_libs.dagster.lineage.passthrough_lineage

**Example:**

```python
from cogapp_libs.dagster.lineage import passthrough_lineage
import dagster as dg

# For output assets where columns pass through from a single source
SALES_OUTPUT_LINEAGE = passthrough_lineage(
    source=dg.AssetKey("sales_transform_soda"),
    columns=["sale_id", "artwork_id", "title", "artist_name", "pct_change"],
    renames={"price_difference": "price_diff"},  # output_col: source_col
)
```

### DuckDB View Registration

#### register_harvest_views

::: cogapp_libs.dagster.lineage.register_harvest_views

Simplifies registering harvest parquet files as DuckDB views.

**Example:**

```python
from cogapp_libs.dagster.lineage import register_harvest_views

with duckdb.get_connection() as conn:
    # Register views for sales, artworks, and artists
    register_harvest_views(conn, paths.harvest_dir, ["sales", "artworks", "artists"])

    # Now query using simple view names
    conn.sql("SELECT * FROM sales JOIN artworks USING (artwork_id)")
```

**Default view mappings** (defined in `HARVEST_VIEWS`):

| View Name | Harvest Subdirectory |
|-----------|---------------------|
| `sales` | `raw/sales_raw/**/*.parquet` |
| `artworks` | `raw/artworks_raw/**/*.parquet` |
| `artists` | `raw/artists_raw/**/*.parquet` |
| `media` | `raw/media/**/*.parquet` |

### Metadata Collection

Helpers that consolidate lineage, stats, examples, and timing into a single call.

#### collect_parquet_metadata

::: cogapp_libs.dagster.lineage.collect_parquet_metadata

**Example:**

```python
from cogapp_libs.dagster.lineage import collect_parquet_metadata, register_harvest_views

@dg.asset(io_manager_key="parquet_path_io_manager")
def sales_transform_soda(context, duckdb, paths, output_paths) -> str:
    start = time.perf_counter()
    output_path = Path(output_paths.transforms_dir) / "sales.parquet"

    with duckdb.get_connection() as conn:
        register_harvest_views(conn, paths.harvest_dir, ["sales", "artworks", "artists"])
        conn.sql(TRANSFORM_SQL).write_parquet(str(output_path), compression="zstd")

        collect_parquet_metadata(
            context, conn, str(output_path),
            lineage=SALES_TRANSFORM_LINEAGE,
            stats_sql=f"SELECT count(*) AS record_count, sum(value) AS total FROM '{output_path}'",
            example_id=("sale_id", 2),
            elapsed_ms=(time.perf_counter() - start) * 1000,
        )

    return str(output_path)
```

#### collect_json_output_metadata

::: cogapp_libs.dagster.lineage.collect_json_output_metadata

**Example:**

```python
from cogapp_libs.dagster.lineage import collect_json_output_metadata

@dg.asset(io_manager_key="parquet_path_io_manager")
def sales_output_soda(context, duckdb, sales_transform_soda: str, output_paths) -> str:
    start = time.perf_counter()

    with duckdb.get_connection() as conn:
        # Write JSON output...

        collect_json_output_metadata(
            context, conn,
            input_path=sales_transform_soda,
            output_path=output_paths.sales_json,
            lineage=SALES_OUTPUT_LINEAGE,
            example_id=("sale_id", 2),
            example_renames={"price_difference": "price_diff"},  # Handle column renames
            extra_metadata={"record_count": count, "filter_threshold": "$50,000"},
            elapsed_ms=(time.perf_counter() - start) * 1000,
        )

    return sales_transform_soda
```

### Example Row Helpers

#### get_example_row

::: cogapp_libs.dagster.lineage.get_example_row

**Example:**

```python
from cogapp_libs.dagster.lineage import get_example_row

with duckdb.get_connection() as conn:
    # Get a specific row by ID
    examples = get_example_row(conn, "data/sales.parquet", "sale_id", 2)
    # {"sale_id": "2", "sale_price_usd": "$86.3M", "title": "Day Dream"}

context.add_output_metadata({
    "lineage_examples": examples,
})
```

#### format_value

::: cogapp_libs.dagster.lineage.format_value

Formats values for display in lineage visualizations:

- `86300000` → `$86.3M`
- `1500` → `$1.5K`
- `-560000` → `-$560K`
- `123.45` → `$123.45`

#### add_lineage_examples_to_dlt_results

::: cogapp_libs.dagster.lineage.add_lineage_examples_to_dlt_results

**Example:**

```python
from cogapp_libs.dagster.lineage import add_lineage_examples_to_dlt_results

ASSET_CONFIG = {
    "dlt_harvest_sales_raw": {
        "path": "raw/sales_raw/**/*.parquet",
        "id_field": "sale_id",
        "id_value": 2,
    },
}

@dg.multi_asset(specs=HARVEST_SPECS)
def dlt_harvest_assets(context, dlt, paths):
    results = list(dlt.run(...))
    yield from add_lineage_examples_to_dlt_results(
        results, paths.harvest_dir, ASSET_CONFIG
    )
```

---

## DuckDB Asset Factories

Declarative factories for building DuckDB-based Dagster assets with minimal boilerplate.

### duckdb_transform_asset

::: cogapp_libs.dagster.duckdb.duckdb_transform_asset

**Example:**

```python
from cogapp_libs.dagster.duckdb import duckdb_transform_asset

sales_transform_soda = duckdb_transform_asset(
    name="sales_transform_soda",
    sql="""
        SELECT s.sale_id, s.sale_date, s.sale_price_usd,
               aw.title, ar.name.trim().upper() AS artist_name
        FROM sales s
        LEFT JOIN artworks aw USING (artwork_id)
        LEFT JOIN artists ar USING (artist_id)
        ORDER BY s.sale_date DESC
    """,
    harvest_views=["sales", "artworks", "artists"],
    lineage=SALES_TRANSFORM_LINEAGE,
    example_id=("sale_id", 2),
    group_name="transform_soda",
    kinds={"duckdb", "sql"},
)
```

### duckdb_output_asset

::: cogapp_libs.dagster.duckdb.duckdb_output_asset

**Example:**

```python
from cogapp_libs.dagster.duckdb import duckdb_output_asset

sales_output_soda = duckdb_output_asset(
    name="sales_output_soda",
    source="sales_transform_soda",
    output_path_attr="sales_soda",
    where="sale_price_usd >= 50_000",
    select="* EXCLUDE (price_diff), price_diff AS price_difference",
    lineage=SALES_OUTPUT_LINEAGE,
    example_id=("sale_id", 2),
    example_renames={"price_difference": "price_diff"},
    group_name="output_soda",
    kinds={"duckdb", "json"},
)
```

### DuckDBContext

::: cogapp_libs.dagster.duckdb.DuckDBContext

For complex operations beyond what factories support:

```python
from cogapp_libs.dagster.duckdb import DuckDBContext

@dg.asset
def custom_transform(context, duckdb, paths) -> str:
    with DuckDBContext(duckdb, paths, context) as ctx:
        ctx.register_views(["sales", "artworks"])

        # Custom SQL operations
        ctx.conn.sql("CREATE TABLE temp AS SELECT ...").write_parquet(path)

        ctx.add_parquet_metadata(path, lineage=LINEAGE, example_id=("id", 1))

    return str(path)
```

---

## DuckDB Friendly SQL

DuckDB provides SQL extensions that make queries more readable and concise.

### Dot Notation for String Functions

Chain string functions using method syntax instead of nested function calls:

```sql
-- Traditional SQL
SELECT upper(trim(name)) AS artist_name FROM artists

-- DuckDB friendly
SELECT name.trim().upper() AS artist_name FROM artists
```

Common chainable functions: `upper()`, `lower()`, `trim()`, `ltrim()`, `rtrim()`, `replace()`, `substring()`.

### GROUP BY ALL

Automatically group by all non-aggregated columns:

```sql
-- Traditional SQL
SELECT artwork_id, count(*) AS sale_count, sum(sale_price_usd) AS total
FROM sales
GROUP BY artwork_id

-- DuckDB friendly
SELECT artwork_id, count(*) AS sale_count, sum(sale_price_usd) AS total
FROM sales
GROUP BY ALL
```

### SELECT * EXCLUDE / REPLACE

Select all columns except specific ones, or replace column definitions:

```sql
-- Exclude columns
SELECT * EXCLUDE (internal_id, created_at) FROM sales

-- Replace column values
SELECT * REPLACE (sale_price_usd / 100 AS sale_price_usd) FROM sales

-- Combine with rename
SELECT * EXCLUDE (price_diff), price_diff AS price_difference FROM sales
```

### Numeric Underscores

Improve readability of large numbers:

```sql
SELECT * FROM sales WHERE sale_price_usd >= 1_000_000  -- $1M threshold
```

### Aggregate with ORDER BY

Order values within aggregations:

```sql
-- Ordered list aggregation
SELECT artwork_id, list(filename ORDER BY sort_order) AS media_files
FROM media
GROUP BY ALL

-- String aggregation with order
SELECT artist_id, string_agg(title, ', ' ORDER BY year DESC) AS works
FROM artworks
GROUP BY ALL
```

### Conditional Aggregates

Count or aggregate based on conditions:

```sql
-- Count matching rows
SELECT
    count(*) AS total_sales,
    count_if(sale_price_usd > 1_000_000) AS million_dollar_sales
FROM sales

-- Filter within aggregate
SELECT sum(sale_price_usd) FILTER (WHERE buyer_country = 'United States') AS us_sales
FROM sales
```

### ifnull / coalesce

Handle NULL values concisely:

```sql
-- Two-argument null replacement
SELECT ifnull(sale_count, 0) AS sale_count FROM artworks

-- Multiple fallback values
SELECT coalesce(preferred_name, display_name, 'Unknown') AS name FROM artists
```

For more DuckDB SQL features, see:

- [DuckDB Friendly SQL](https://duckdb.org/docs/sql/dialect/friendly_sql.html)
- [Text Functions](https://duckdb.org/docs/sql/functions/text)
- [Aggregate Functions](https://duckdb.org/docs/sql/functions/aggregates)

---

## Polars vs DuckDB Comparison

Common data transformations shown in both Polars and DuckDB SQL.

### String Operations

| Operation | Polars | DuckDB |
|-----------|--------|--------|
| Strip whitespace | `col("name").str.strip_chars()` | `name.trim()` |
| Uppercase | `col("name").str.to_uppercase()` | `name.upper()` |
| Lowercase | `col("name").str.to_lowercase()` | `name.lower()` |
| Replace | `col("name").str.replace("old", "new")` | `name.replace('old', 'new')` |
| Contains (bool) | `col("name").str.contains("pattern")` | `name.contains('pattern')` |
| Concatenate | `pl.concat_str([col("a"), col("b")], separator=" ")` | `concat_ws(' ', a, b)` |
| Extract first char | `col("name").str.slice(0, 1)` | `name[1]` or `left(name, 1)` |

**Example: Clean artist name**

```python
# Polars
df.with_columns(
    pl.col("name").str.strip_chars().str.to_uppercase().alias("artist_name")
)

# DuckDB
SELECT name.trim().upper() AS artist_name FROM artists
```

### Null Handling

| Operation | Polars | DuckDB |
|-----------|--------|--------|
| Fill with constant | `col("x").fill_null(0)` | `coalesce(x, 0)` or `ifnull(x, 0)` |
| Fill from column | `col("x").fill_null(col("y"))` | `coalesce(x, y)` |
| Check if null | `col("x").is_null()` | `x IS NULL` |
| Drop nulls | `df.drop_nulls("x")` | `WHERE x IS NOT NULL` |

**Example: Fill missing prices**

```python
# Polars
df.with_columns(
    pl.col("sale_price").fill_null(pl.col("list_price")).alias("final_price")
)

# DuckDB
SELECT coalesce(sale_price, list_price) AS final_price FROM sales
```

### Filtering (Drop Rows)

| Operation | Polars | DuckDB |
|-----------|--------|--------|
| Equals | `df.filter(col("status") == "active")` | `WHERE status = 'active'` |
| Not equals | `df.filter(col("status") != "deleted")` | `WHERE status != 'deleted'` |
| Greater than | `df.filter(col("price") > 1000)` | `WHERE price > 1000` |
| Multiple conditions | `df.filter((col("a") > 1) & (col("b") < 10))` | `WHERE a > 1 AND b < 10` |

**Example: Filter high-value sales**

```python
# Polars
df.filter(pl.col("sale_price_usd") >= 50_000)

# DuckDB
SELECT * FROM sales WHERE sale_price_usd >= 50_000
```

### Conditional Replace

| Operation | Polars | DuckDB |
|-----------|--------|--------|
| When/then | `pl.when(cond).then(val).otherwise(col("x"))` | `CASE WHEN cond THEN val ELSE x END` |
| Simple if | `pl.when(cond).then(val).otherwise(col("x"))` | `if(cond, val, x)` |

**Example: Price tier categorization**

```python
# Polars
df.with_columns(
    pl.when(pl.col("price") < 1000).then(pl.lit("budget"))
      .when(pl.col("price") < 10000).then(pl.lit("mid"))
      .otherwise(pl.lit("premium"))
      .alias("price_tier")
)

# DuckDB
SELECT CASE
    WHEN price < 1000 THEN 'budget'
    WHEN price < 10_000 THEN 'mid'
    ELSE 'premium'
END AS price_tier
FROM artworks
```

### List/Array Operations

| Operation | Polars | DuckDB |
|-----------|--------|--------|
| Get first | `col("items").list.first()` | `items[1]` or `list_extract(items, 1)` |
| Explode to rows | `df.explode("items")` | `UNNEST(items)` |
| Aggregate to list | `col("x").implode()` in group_by | `list(x)` or `array_agg(x)` |
| List length | `col("items").list.len()` | `len(items)` or `array_length(items)` |
| Remove nulls | `col("items").list.drop_nulls()` | `list_filter(items, x -> x IS NOT NULL)` |

**Example: Aggregate media files**

```python
# Polars
df.group_by("artwork_id").agg(
    pl.col("filename").alias("media_files")
)

# DuckDB
SELECT artwork_id, list(filename ORDER BY sort_order) AS media_files
FROM media
GROUP BY ALL
```

### Type Casting

| Operation | Polars | DuckDB |
|-----------|--------|--------|
| To string | `col("x").cast(pl.Utf8)` | `x::VARCHAR` or `CAST(x AS VARCHAR)` |
| To integer | `col("x").cast(pl.Int64)` | `x::INTEGER` or `CAST(x AS INTEGER)` |
| To float | `col("x").cast(pl.Float64)` | `x::DOUBLE` or `CAST(x AS DOUBLE)` |
| To date | `col("x").str.to_date("%Y-%m-%d")` | `x::DATE` or `strptime(x, '%Y-%m-%d')` |

### Column Operations

| Operation | Polars | DuckDB |
|-----------|--------|--------|
| Rename | `df.rename({"old": "new"})` | `SELECT old AS new` |
| Select columns | `df.select(["a", "b"])` | `SELECT a, b` |
| Drop columns | `df.drop(["x", "y"])` | `SELECT * EXCLUDE (x, y)` |
| Add constant | `df.with_columns(pl.lit("value").alias("new"))` | `SELECT *, 'value' AS new` |
| Copy column | `df.with_columns(pl.col("a").alias("b"))` | `SELECT *, a AS b` |

!!! tip "Honeysuckle Processor Equivalents"
    See the [Processor Equivalents Guide](../guides/processor-equivalents.md) for detailed examples
    showing how the top 15 most-used Honeysuckle processors map to native Polars and DuckDB operations.

---

## Exceptions

### PipelineError

::: cogapp_libs.dagster.exceptions.PipelineError

### MissingTableError

::: cogapp_libs.dagster.exceptions.MissingTableError

### MissingColumnError

::: cogapp_libs.dagster.exceptions.MissingColumnError

### DataValidationError

::: cogapp_libs.dagster.exceptions.DataValidationError
