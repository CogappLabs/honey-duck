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

## Exceptions

### PipelineError

::: cogapp_libs.dagster.exceptions.PipelineError

### MissingTableError

::: cogapp_libs.dagster.exceptions.MissingTableError

### MissingColumnError

::: cogapp_libs.dagster.exceptions.MissingColumnError

### DataValidationError

::: cogapp_libs.dagster.exceptions.DataValidationError
