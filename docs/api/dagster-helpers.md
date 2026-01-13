# Dagster Helpers

Utilities for building Dagster assets with less boilerplate.

!!! info "External Documentation"
    - **Dagster**: [docs.dagster.io](https://docs.dagster.io/) | [Assets Guide](https://docs.dagster.io/concepts/assets/software-defined-assets)
    - **Polars**: [pola.rs/docs](https://docs.pola.rs/) | [API Reference](https://docs.pola.rs/api/python/stable/reference/)
    - **DuckDB**: [duckdb.org/docs](https://duckdb.org/docs/)

## Data Loading

### read_harvest_table_lazy

::: cogapp_deps.dagster.validation.read_harvest_table_lazy

### read_harvest_tables_lazy

::: cogapp_deps.dagster.validation.read_harvest_tables_lazy

### read_parquet_table_lazy

::: cogapp_deps.dagster.validation.read_parquet_table_lazy

---

## Validation

### validate_dataframe

::: cogapp_deps.dagster.validation.validate_dataframe

---

## Metadata

### add_dataframe_metadata

::: cogapp_deps.dagster.helpers.add_dataframe_metadata

### track_timing

::: cogapp_deps.dagster.helpers.track_timing

### write_json_output

::: cogapp_deps.dagster.io.write_json_output

---

## Visualization

### altair_to_metadata

::: cogapp_deps.dagster.helpers.altair_to_metadata

### table_preview_to_metadata

::: cogapp_deps.dagster.helpers.table_preview_to_metadata

---

## Exceptions

### PipelineError

::: cogapp_deps.dagster.exceptions.PipelineError

### MissingTableError

::: cogapp_deps.dagster.exceptions.MissingTableError

### MissingColumnError

::: cogapp_deps.dagster.exceptions.MissingColumnError

### DataValidationError

::: cogapp_deps.dagster.exceptions.DataValidationError
