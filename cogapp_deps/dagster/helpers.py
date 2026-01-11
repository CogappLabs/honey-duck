"""Generic helper functions for Dagster asset development.

Reduces boilerplate when creating assets by providing common utilities
for metadata, table reading, and more. Use with standard Dagster decorators.
"""

from pathlib import Path

import dagster as dg
import polars as pl

from .validation import read_duckdb_table_lazy


def read_tables_from_duckdb(
    db_path: str | Path,
    *table_specs: tuple[str, list[str]],
    schema: str = "raw",
    asset_name: str,
) -> dict[str, pl.LazyFrame]:
    """Read multiple tables from DuckDB with validation in one call.

    Reduces boilerplate when reading multiple related tables.
    Error handling is automatic - raises clear exceptions if tables
    or columns are missing.

    Args:
        db_path: Path to DuckDB database
        *table_specs: Tuples of (table_name, required_columns)
        schema: Schema name (default: "raw")
        asset_name: Name of calling asset (for error context)

    Returns:
        Dictionary mapping table names to LazyFrames

    Raises:
        MissingTableError: If a table doesn't exist (lists available tables)
        MissingColumnError: If required columns are missing (lists available columns)
        FileNotFoundError: If database doesn't exist

    Example:
        >>> tables = read_tables_from_duckdb(
        ...     "pipeline.duckdb",
        ...     ("sales", ["sale_id", "sale_price_usd"]),
        ...     ("artworks", ["artwork_id", "title"]),
        ...     schema="raw",
        ...     asset_name="sales_transform",
        ... )
        >>> sales = tables["sales"]
        >>> artworks = tables["artworks"]
        >>> result = sales.join(artworks, on="artwork_id").collect()
    """
    result = {}
    for table_name, required_columns in table_specs:
        result[table_name] = read_duckdb_table_lazy(
            db_path,
            table_name,
            schema=schema,
            required_columns=required_columns,
            asset_name=asset_name,
        )
    return result


def add_dataframe_metadata(
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
        >>> add_dataframe_metadata(
        ...     context,
        ...     result,
        ...     unique_artworks=result["artwork_id"].n_unique(),
        ...     total_value=float(result["sale_price_usd"].sum()),
        ... )
    """
    metadata = {
        "record_count": len(df),
        "columns": df.columns,
        "preview": dg.MetadataValue.md(df.head(5).to_pandas().to_markdown(index=False)),
        **extra_metadata,
    }
    context.add_output_metadata(metadata)
