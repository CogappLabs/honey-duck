"""Utility functions for the honey-duck pipeline.

This module provides validated data access with clear error messages.
"""

from pathlib import Path

import duckdb
import polars as pl

from .exceptions import MissingColumnError, MissingTableError


def read_raw_table_lazy(
    db_path: str | Path,
    table_name: str,
    required_columns: list[str] | None = None,
    asset_name: str = "unknown",
) -> pl.LazyFrame:
    """Read table from raw schema as Polars LazyFrame with validation.

    Args:
        db_path: Path to DuckDB database
        table_name: Name of table in raw schema (e.g., "sales_raw")
        required_columns: Optional list of required column names
        asset_name: Name of calling asset (for error messages)

    Returns:
        LazyFrame with data from table

    Raises:
        MissingTableError: If table doesn't exist in database
        MissingColumnError: If required columns are missing
        FileNotFoundError: If database file doesn't exist

    Example:
        >>> sales = read_raw_table_lazy(
        ...     "data.duckdb",
        ...     "sales_raw",
        ...     required_columns=["sale_id", "sale_price_usd"],
        ...     asset_name="sales_transform"
        ... )
    """
    db_path = Path(db_path)

    # Check database exists
    if not db_path.exists():
        raise FileNotFoundError(
            f"[{asset_name}] Database not found at {db_path}. "
            f"Did you run the harvest job first? "
            f"Run: uv run dagster job execute -j full_pipeline"
        )

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        # Check table exists
        tables_df = conn.sql("SHOW TABLES FROM raw").pl()
        available_tables = tables_df["name"].to_list() if len(tables_df) > 0 else []

        if table_name not in available_tables:
            raise MissingTableError(asset_name, f"raw.{table_name}", available_tables)

        # Read table
        df = conn.sql(f"SELECT * FROM raw.{table_name}").pl().lazy()

        # Validate required columns if specified
        if required_columns:
            actual_columns = df.collect_schema().names()
            missing = set(required_columns) - set(actual_columns)
            if missing:
                raise MissingColumnError(asset_name, missing, actual_columns)

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
        MissingColumnError: If required columns are missing

    Example:
        >>> validate_dataframe(
        ...     sales_df,
        ...     ["sale_id", "sale_price_usd"],
        ...     "sales_transform"
        ... )
    """
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise MissingColumnError(asset_name, missing, df.columns)
