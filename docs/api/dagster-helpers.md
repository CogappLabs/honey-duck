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

### get_example_row

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

### format_value

::: cogapp_libs.dagster.lineage.format_value

Formats values for display in lineage visualizations:

- `86300000` → `$86.3M`
- `1500` → `$1.5K`
- `-560000` → `-$560K`
- `123.45` → `$123.45`

### add_lineage_examples_to_dlt_results

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
