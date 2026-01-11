"""Generic data validation utilities for Dagster pipelines.

Provides validated data access with automatic error handling.
Works with DuckDB and Polars DataFrames.

Errors are automatically wrapped in Dagster Failure with metadata
for better UI rendering and debugging.
"""

from pathlib import Path

import duckdb
import polars as pl

from .exceptions import MissingColumnError, MissingTableError, raise_as_dagster_failure


def read_duckdb_table_lazy(
    db_path: str | Path,
    table_name: str,
    schema: str = "raw",
    required_columns: list[str] | None = None,
    asset_name: str = "unknown",
) -> pl.LazyFrame:
    """Read table from DuckDB as Polars LazyFrame with validation.

    Validates table existence and required columns before returning.
    Provides actionable error messages listing available tables/columns.

    Args:
        db_path: Path to DuckDB database file
        table_name: Name of table to read (without schema prefix)
        schema: Schema name (default: "raw")
        required_columns: Optional list of required column names
        asset_name: Name of calling asset (for error context)

    Returns:
        LazyFrame with data from table

    Raises:
        FileNotFoundError: If database file doesn't exist
        MissingTableError: If table doesn't exist (lists available tables)
        MissingColumnError: If required columns are missing (lists available columns)

    Example:
        >>> sales = read_duckdb_table_lazy(
        ...     "pipeline.duckdb",
        ...     "sales",
        ...     schema="raw",
        ...     required_columns=["sale_id", "sale_price_usd"],
        ...     asset_name="sales_transform"
        ... )
        >>> result = sales.filter(pl.col("sale_price_usd") > 1000).collect()
    """
    db_path = Path(db_path)

    # Check database exists
    if not db_path.exists():
        error = FileNotFoundError(
            f"[{asset_name}] Database not found at {db_path}. "
            f"Did you run the harvest job first?"
        )
        raise_as_dagster_failure(error)

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        # Check table exists
        tables_df = conn.sql(f"SHOW TABLES FROM {schema}").pl()
        available_tables = tables_df["name"].to_list() if len(tables_df) > 0 else []

        if table_name not in available_tables:
            error = MissingTableError(asset_name, f"{schema}.{table_name}", available_tables)
            raise_as_dagster_failure(error)

        # Read table
        df = conn.sql(f"SELECT * FROM {schema}.{table_name}").pl().lazy()

        # Validate required columns if specified
        if required_columns:
            actual_columns = df.collect_schema().names()
            missing = set(required_columns) - set(actual_columns)
            if missing:
                error = MissingColumnError(asset_name, missing, actual_columns)
                raise_as_dagster_failure(error)

        return df

    finally:
        conn.close()


def validate_dataframe(
    df: pl.DataFrame,
    required_columns: list[str],
    asset_name: str,
) -> None:
    """Validate DataFrame has required columns.

    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        asset_name: Name of asset (for error messages)

    Raises:
        dagster.Failure: If required columns are missing (with metadata)

    Example:
        >>> validate_dataframe(
        ...     sales_df,
        ...     ["sale_id", "sale_price_usd"],
        ...     "sales_transform"
        ... )
    """
    missing = set(required_columns) - set(df.columns)
    if missing:
        error = MissingColumnError(asset_name, missing, df.columns)
        raise_as_dagster_failure(error)
