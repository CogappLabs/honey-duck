"""Helper functions to reduce boilerplate in ETL pipelines.

This module provides helper functions for common ETL operations with automatic
error handling and validation. Use these with standard Dagster decorators.

Example:
    import dagster as dg
    import polars as pl
    from honey_duck.defs.helpers import read_harvest_tables, add_standard_metadata

    @dg.asset(kinds={"polars"}, group_name="transform_polars", deps=STANDARD_HARVEST_DEPS)
    def sales_transform(context: dg.AssetExecutionContext) -> pl.DataFrame:
        # Read tables with automatic validation
        tables = read_harvest_tables(
            ("sales_raw", ["sale_id", "sale_price_usd"]),
            ("artworks_raw", ["artwork_id", "title"]),
            asset_name="sales_transform",
        )

        result = tables["sales_raw"].join(tables["artworks_raw"], on="artwork_id").collect()

        # Add standard metadata automatically
        add_standard_metadata(context, result, unique_artworks=result["artwork_id"].n_unique())
        return result
"""

import dagster as dg
import polars as pl

from cogapp_deps.dagster import (
    add_dataframe_metadata,
    read_duckdb_table_lazy,
    read_tables_from_duckdb,
)

from .config import CONFIG


def read_harvest_tables(
    *table_specs: tuple[str, list[str]],
    asset_name: str,
) -> dict[str, pl.LazyFrame]:
    """Read multiple harvest tables with validation in one call.

    This helper reduces boilerplate when reading multiple related tables.
    Error handling is automatic - raises clear exceptions if tables or
    columns are missing.

    Args:
        *table_specs: Tuples of (table_name, required_columns)
        asset_name: Name of calling asset for error context

    Returns:
        Dictionary mapping table names to LazyFrames

    Raises:
        MissingTableError: If a table doesn't exist (lists available tables)
        MissingColumnError: If required columns are missing (lists available columns)
        FileNotFoundError: If database doesn't exist (with helpful message)

    Example:
        tables = read_harvest_tables(
            ("sales_raw", ["sale_id", "sale_price_usd"]),
            ("artworks_raw", ["artwork_id", "title"]),
            asset_name="sales_transform",
        )
        sales = tables["sales_raw"]
        artworks = tables["artworks_raw"]
    """
    return read_tables_from_duckdb(
        CONFIG.duckdb_path,
        *table_specs,
        schema="raw",
        asset_name=asset_name,
    )


def add_standard_metadata(
    context: dg.AssetExecutionContext,
    df: pl.DataFrame,
    **extra_metadata,
) -> None:
    """Add standard metadata for a DataFrame result.

    Automatically includes:
    - Record count
    - Column list
    - Preview (first 5 rows as markdown table)
    - Any extra metadata provided

    Args:
        context: Asset execution context
        df: Result DataFrame
        **extra_metadata: Additional metadata to include

    Example:
        add_standard_metadata(
            context,
            result,
            unique_artworks=result["artwork_id"].n_unique(),
            total_value=float(result["sale_price_usd"].sum()),
        )
    """
    add_dataframe_metadata(context, df, **extra_metadata)


# Asset group constants for consistency
class AssetGroups:
    """Standard asset group names.

    Use these constants for consistent group naming across assets.

    Example:
        @dg.asset(group_name=AssetGroups.TRANSFORM_POLARS)
        def my_asset(context):
            ...
    """

    HARVEST = "harvest"
    TRANSFORM = "transform"
    TRANSFORM_POLARS = "transform_polars"
    TRANSFORM_POLARS_OPS = "transform_polars_ops"
    TRANSFORM_DUCKDB = "transform_duckdb"
    TRANSFORM_POLARS_FS = "transform_polars_fs"
    OUTPUT = "output"
    OUTPUT_POLARS = "output_polars"
    OUTPUT_POLARS_OPS = "output_polars_ops"
    OUTPUT_DUCKDB = "output_duckdb"
    OUTPUT_POLARS_FS = "output_polars_fs"


# Standard harvest dependencies
STANDARD_HARVEST_DEPS = [
    dg.AssetKey("dlt_harvest_sales_raw"),
    dg.AssetKey("dlt_harvest_artworks_raw"),
    dg.AssetKey("dlt_harvest_artists_raw"),
    dg.AssetKey("dlt_harvest_media"),
]
