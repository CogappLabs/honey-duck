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


def read_parquet_table_lazy(
    parquet_dir: str | Path,
    table_name: str,
    required_columns: list[str] | None = None,
    asset_name: str = "unknown",
) -> pl.LazyFrame:
    """Read table from Parquet files as Polars LazyFrame with validation.

    Validates table existence and required columns before returning.
    Provides actionable error messages listing available tables/columns.

    Args:
        parquet_dir: Path to directory containing Parquet files (e.g., harvest_parquet/)
        table_name: Name of table to read (subdirectory name)
        required_columns: Optional list of required column names
        asset_name: Name of calling asset (for error context)

    Returns:
        LazyFrame with data from Parquet files

    Raises:
        FileNotFoundError: If parquet directory doesn't exist
        MissingTableError: If table directory doesn't exist (lists available tables)
        MissingColumnError: If required columns are missing (lists available columns)

    Example:
        >>> sales = read_parquet_table_lazy(
        ...     "/path/to/harvest_parquet",
        ...     "sales_raw",
        ...     required_columns=["sale_id", "sale_price_usd"],
        ...     asset_name="sales_transform"
        ... )
        >>> result = sales.filter(pl.col("sale_price_usd") > 1000).collect()
    """
    parquet_dir = Path(parquet_dir)

    # Check parquet directory exists
    if not parquet_dir.exists():
        error = FileNotFoundError(
            f"[{asset_name}] Parquet directory not found at {parquet_dir}. "
            f"Did you run the harvest job first?"
        )
        raise_as_dagster_failure(error)

    # Check table directory exists
    table_dir = parquet_dir / table_name
    if not table_dir.exists():
        available_tables = [d.name for d in parquet_dir.iterdir() if d.is_dir()]
        error = MissingTableError(asset_name, table_name, available_tables)
        raise_as_dagster_failure(error)

    # Read all Parquet files in table directory
    parquet_files = list(table_dir.glob("*.parquet"))
    if not parquet_files:
        error = FileNotFoundError(
            f"[{asset_name}] No Parquet files found in {table_dir}"
        )
        raise_as_dagster_failure(error)

    # Read using scan_parquet which supports multiple files
    df = pl.scan_parquet(table_dir / "*.parquet")

    # Validate required columns if specified
    if required_columns:
        actual_columns = df.collect_schema().names()
        missing = set(required_columns) - set(actual_columns)
        if missing:
            error = MissingColumnError(asset_name, missing, actual_columns)
            raise_as_dagster_failure(error)

    return df


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


def read_harvest_table_lazy(
    harvest_dir: str | Path,
    table_name: str,
    schema: str = "raw",
    required_columns: list[str] | None = None,
    asset_name: str = "unknown",
) -> pl.LazyFrame:
    """Read harvest table from Parquet files as Polars LazyFrame.

    Convenience wrapper around read_parquet_table_lazy for harvest tables.
    Automatically handles the schema subdirectory structure.

    Args:
        harvest_dir: Path to harvest Parquet directory (e.g., data/output/dlt/harvest_parquet)
        table_name: Name of table to read
        schema: Schema/subdirectory name (default: "raw")
        required_columns: Optional list of required column names
        asset_name: Name of calling asset (for error context)

    Returns:
        LazyFrame with data from Parquet files

    Raises:
        FileNotFoundError: If harvest directory doesn't exist
        MissingTableError: If table doesn't exist (lists available tables)
        MissingColumnError: If required columns are missing (lists available columns)

    Example:
        >>> from pathlib import Path
        >>> sales = read_harvest_table_lazy(
        ...     Path("data/output/dlt/harvest_parquet"),
        ...     "sales_raw",
        ...     asset_name="sales_transform"
        ... )
        >>> result = sales.filter(pl.col("sale_price_usd") > 1000).collect()
    """
    harvest_dir = Path(harvest_dir)
    return read_parquet_table_lazy(
        harvest_dir / schema,
        table_name,
        required_columns=required_columns,
        asset_name=asset_name,
    )


def read_harvest_tables_lazy(
    harvest_dir: str | Path,
    *table_specs: tuple[str, list[str] | None],
    schema: str = "raw",
    asset_name: str = "unknown",
) -> dict[str, pl.LazyFrame]:
    """Read multiple harvest tables in one call with validation.

    Convenience function to batch-read multiple related tables. Reduces
    boilerplate when you need to join or process multiple tables together.

    Args:
        harvest_dir: Path to harvest Parquet directory (e.g., data/output/dlt/harvest_parquet)
        *table_specs: Variable number of (table_name, required_columns) tuples.
                      required_columns can be None to skip column validation.
        schema: Schema/subdirectory name (default: "raw")
        asset_name: Name of calling asset (for error context)

    Returns:
        Dictionary mapping table names to LazyFrames

    Raises:
        FileNotFoundError: If harvest directory doesn't exist
        MissingTableError: If any table doesn't exist (lists available tables)
        MissingColumnError: If required columns are missing (lists available columns)

    Example:
        >>> tables = read_harvest_tables_lazy(
        ...     Path("data/output/dlt/harvest_parquet"),
        ...     ("sales_raw", ["sale_id", "sale_price_usd"]),
        ...     ("artworks_raw", ["artwork_id", "title"]),
        ...     ("artists_raw", None),  # No column validation
        ...     asset_name="sales_transform"
        ... )
        >>> sales = tables["sales_raw"]
        >>> artworks = tables["artworks_raw"]
        >>> result = sales.join(artworks, on="artwork_id").collect()
    """
    result = {}
    for table_name, required_columns in table_specs:
        result[table_name] = read_harvest_table_lazy(
            harvest_dir,
            table_name,
            schema=schema,
            required_columns=required_columns,
            asset_name=asset_name,
        )
    return result


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
