---
title: API Reference
description: API documentation for Honey Duck's processors, IO managers, Dagster helpers, and utility functions.
---

# API Reference

Auto-generated documentation from source code docstrings.

## Modules

### Dagster Helpers

Utilities for building Dagster assets with less boilerplate.

- [**Dagster Helpers**](dagster-helpers.md) - Data loading, validation, metadata, visualization

### Processors

Reusable DataFrame transformation classes.

- [**Processors**](processors.md) - DuckDB, Polars, and Pandas processors

### IO Managers

Custom IO managers for various storage backends.

- [**IO Managers**](io-managers.md) - Elasticsearch, OpenSearch, JSON

## Quick Import

```python
from cogapp_libs.dagster import (
    # Data Loading
    read_harvest_table_lazy,
    read_harvest_tables_lazy,

    # Validation
    validate_dataframe,
    MissingTableError,
    MissingColumnError,

    # Metadata
    add_dataframe_metadata,
    track_timing,
    write_json_output,

    # Visualization
    altair_to_metadata,
    table_preview_to_metadata,

    # IO Managers
    ElasticsearchIOManager,
    OpenSearchIOManager,
    JSONIOManager,
)
```

## Usage Pattern

```python
import dagster as dg
import polars as pl
from cogapp_libs.dagster import (
    read_harvest_tables_lazy,
    track_timing,
    add_dataframe_metadata,
    altair_to_metadata,
)

@dg.asset(kinds={"polars"}, group_name="transform")
def my_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Example asset using cogapp_libs helpers."""

    with track_timing(context, "data loading"):
        tables = read_harvest_tables_lazy(
            "data/harvest/raw",
            ("sales_raw", ["sale_id", "amount"]),
            asset_name="my_asset",
        )
        result = tables["sales_raw"].collect()

    # Add chart to metadata
    chart = result.plot.bar(x="category", y="amount")
    context.add_output_metadata(altair_to_metadata(chart, "distribution"))

    # Add standard metadata
    add_dataframe_metadata(context, result, total=float(result["amount"].sum()))

    return result
```
