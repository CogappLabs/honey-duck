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


class track_timing:
    """Context manager to track and log execution time.

    Automatically adds processing_time_ms to asset metadata and logs
    completion message. Use this to eliminate manual timing boilerplate.

    Args:
        context: Asset execution context
        operation: Description of operation (e.g., "transform", "processing")
        log_message: Optional custom log message template. Use {elapsed_ms} placeholder.

    Example:
        >>> @dg.asset(kinds={"polars"})
        >>> def my_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
        ...     with track_timing(context, "transformation"):
        ...         result = expensive_operation()
        ...         # Automatically logs: "Completed transformation in 123.4ms"
        ...         # Automatically adds processing_time_ms to metadata
        ...     return result

        >>> # With custom log message:
        >>> with track_timing(context, "loading", log_message="Loaded {count} records in {elapsed_ms:.1f}ms"):
        ...     result = load_data()
        ...     context.log.info(f"Loaded {len(result)} records")  # Will use this count
    """

    def __init__(
        self,
        context: dg.AssetExecutionContext,
        operation: str = "processing",
        log_message: str | None = None,
    ):
        self.context = context
        self.operation = operation
        self.log_message = log_message
        self.start_time = None
        self.elapsed_ms = None

    def __enter__(self):
        import time

        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        import time

        if self.start_time is not None:
            self.elapsed_ms = (time.perf_counter() - self.start_time) * 1000

            # Add to metadata
            self.context.add_output_metadata({"processing_time_ms": round(self.elapsed_ms, 2)})

            # Log completion
            if self.log_message:
                self.context.log.info(self.log_message.format(elapsed_ms=self.elapsed_ms))
            else:
                self.context.log.info(f"Completed {self.operation} in {self.elapsed_ms:.1f}ms")

        return False  # Don't suppress exceptions
