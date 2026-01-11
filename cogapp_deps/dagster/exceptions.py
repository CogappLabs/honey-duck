"""Generic exception classes for Dagster ETL pipelines.

Provides structured error handling with actionable error messages.
Use these exceptions in your pipelines for consistent error reporting.
"""


class PipelineError(Exception):
    """Base exception for all pipeline errors."""

    pass


class ConfigurationError(PipelineError):
    """Raised when configuration is invalid or incomplete.

    Example:
        raise ConfigurationError(
            "Database path not configured. "
            "Set DATABASE_PATH environment variable."
        )
    """

    pass


class DataValidationError(PipelineError):
    """Base exception for data validation errors.

    Args:
        asset_name: Name of the asset where error occurred
        message: Detailed error message

    Attributes:
        asset_name: Name of the asset (included in error message)
    """

    def __init__(self, asset_name: str, message: str):
        self.asset_name = asset_name
        super().__init__(f"[{asset_name}] {message}")


class MissingTableError(DataValidationError):
    """Raised when a required database table doesn't exist.

    Automatically lists available tables to help debugging.

    Args:
        asset_name: Name of the asset where error occurred
        table_name: Name of the missing table
        available_tables: List of tables that exist in the database

    Attributes:
        table_name: Name of the missing table
        available_tables: List of available tables

    Example:
        raise MissingTableError(
            "sales_transform",
            "raw.sales",
            ["raw.artworks", "raw.artists"]
        )
    """

    def __init__(
        self, asset_name: str, table_name: str, available_tables: list[str]
    ) -> None:
        self.table_name = table_name
        self.available_tables = available_tables

        message = (
            f"Table '{table_name}' not found in database. "
            f"Available tables: {available_tables}. "
            f"Did you run the harvest job first?"
        )
        super().__init__(asset_name, message)


class MissingColumnError(DataValidationError):
    """Raised when required columns are missing from a DataFrame.

    Automatically lists available columns to help debugging.

    Args:
        asset_name: Name of the asset where error occurred
        missing_columns: Set of column names that are missing
        available_columns: List of columns that exist in the DataFrame

    Attributes:
        missing_columns: Set of missing column names
        available_columns: List of available column names

    Example:
        raise MissingColumnError(
            "sales_transform",
            {"sale_price", "buyer_id"},
            ["sale_id", "artwork_id", "sale_date"]
        )
    """

    def __init__(
        self,
        asset_name: str,
        missing_columns: set[str],
        available_columns: list[str],
    ) -> None:
        self.missing_columns = missing_columns
        self.available_columns = available_columns

        message = (
            f"Missing required columns: {sorted(missing_columns)}. "
            f"Available columns: {sorted(available_columns)}"
        )
        super().__init__(asset_name, message)
