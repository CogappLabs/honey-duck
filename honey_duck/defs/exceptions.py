"""Custom exceptions for the honey-duck pipeline.

These exceptions provide better error messages and help distinguish between
different failure modes in the ETL pipeline.
"""


class HoneyDuckError(Exception):
    """Base exception for all honey-duck pipeline errors."""

    pass


class ConfigurationError(HoneyDuckError):
    """Raised when configuration is invalid or incomplete.

    Examples:
    - Required directories don't exist
    - Environment variables have invalid values
    - File permissions are insufficient
    """

    pass


class DataValidationError(HoneyDuckError):
    """Raised when data fails validation checks.

    Examples:
    - Required columns are missing
    - Data types don't match expected schema
    - Value constraints are violated
    """

    def __init__(self, asset_name: str, message: str):
        """Initialize with asset context.

        Args:
            asset_name: Name of the asset that failed validation
            message: Detailed error message
        """
        self.asset_name = asset_name
        super().__init__(f"[{asset_name}] {message}")


class MissingTableError(DataValidationError):
    """Raised when a required database table doesn't exist.

    Examples:
    - Harvest hasn't run yet
    - Table was deleted
    - Wrong database path
    """

    def __init__(self, asset_name: str, table_name: str, available_tables: list[str]):
        """Initialize with table context.

        Args:
            asset_name: Name of the asset that needs the table
            table_name: Name of the missing table
            available_tables: List of tables that do exist
        """
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

    Examples:
    - Schema changed upstream
    - CSV file has wrong format
    - Join resulted in missing columns
    """

    def __init__(
        self, asset_name: str, missing_columns: set[str], available_columns: list[str]
    ):
        """Initialize with column context.

        Args:
            asset_name: Name of the asset that needs the columns
            missing_columns: Set of missing column names
            available_columns: List of columns that are present
        """
        self.missing_columns = missing_columns
        self.available_columns = available_columns
        message = (
            f"Missing required columns: {sorted(missing_columns)}. "
            f"Available columns: {sorted(available_columns)}"
        )
        super().__init__(asset_name, message)


class ProcessorError(HoneyDuckError):
    """Raised when a processor encounters invalid data or configuration.

    Examples:
    - Invalid operator in PolarsFilterProcessor
    - SQL query syntax error
    - Empty input data
    """

    pass
