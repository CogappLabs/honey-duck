"""DuckDB SQL processors for efficient data transformations.

All processors in this module execute DuckDB SQL queries and return DataFrames.
DuckDB provides excellent performance for analytical queries and joins.

Processors:
    - DuckDBSQLProcessor: Execute raw SQL queries with table registration
    - DuckDBJoinProcessor: Multi-table joins with computed columns
    - DuckDBWindowProcessor: Window functions (ranking, running totals)
    - DuckDBAggregateProcessor: Group-by aggregations

Connection Configuration:
    By default, processors use in-memory DuckDB. To use a persistent database:

        from cogapp_deps.processors.duckdb import configure
        configure(db_path="/path/to/database.duckdb")

    Or set the DUCKDB_PROCESSOR_PATH environment variable.
"""

import os
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

if TYPE_CHECKING:
    pass

# Module-level configuration
_config = {
    "db_path": os.environ.get("DUCKDB_PROCESSOR_PATH", ":memory:"),
    "read_only": False,
}


def configure(db_path: str | Path | None = None, read_only: bool = False) -> None:
    """Configure the default DuckDB connection for processors.

    Args:
        db_path: Path to DuckDB database file, or ":memory:" for in-memory.
                 If None, uses DUCKDB_PROCESSOR_PATH env var or ":memory:".
        read_only: Whether to open the database in read-only mode.
    """
    if db_path is not None:
        _config["db_path"] = str(db_path)
    _config["read_only"] = read_only


def get_connection(read_only: bool | None = None) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection using the configured settings.

    Args:
        read_only: Override the configured read_only setting.

    Returns:
        DuckDB connection.
    """
    ro = read_only if read_only is not None else _config["read_only"]
    return duckdb.connect(_config["db_path"], read_only=ro)


from .aggregate import DuckDBAggregateProcessor
from .join import DuckDBJoinProcessor
from .sql import DuckDBQueryProcessor, DuckDBSQLProcessor
from .window import DuckDBWindowProcessor

__all__ = [
    "configure",
    "get_connection",
    "DuckDBAggregateProcessor",
    "DuckDBJoinProcessor",
    "DuckDBQueryProcessor",
    "DuckDBSQLProcessor",
    "DuckDBWindowProcessor",
]
