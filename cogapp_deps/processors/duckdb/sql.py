"""Generic DuckDB SQL processor for executing raw SQL queries.

Use this when the specific processors (aggregate, window, join) don't fit your needs.
"""

import pandas as pd

try:
    import duckdb

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


class DuckDBSQLProcessor:
    """Execute raw SQL queries against DataFrames using DuckDB.

    Registers DataFrames as tables and executes the provided SQL.
    Use for complex queries that don't fit the specific processors.

    Example:
        >>> processor = DuckDBSQLProcessor(
        ...     sql=\"\"\"
        ...         SELECT a.*, b.category
        ...         FROM df_a a
        ...         LEFT JOIN df_b b ON a.id = b.id
        ...         WHERE a.value > 100
        ...     \"\"\",
        ...     tables={"df_a": df_a, "df_b": df_b},
        ... )
        >>> result = processor.process()
    """

    def __init__(
        self,
        sql: str,
        tables: dict[str, pd.DataFrame] | None = None,
        connection: "duckdb.DuckDBPyConnection | None" = None,
    ):
        if not DUCKDB_AVAILABLE:
            raise ImportError("duckdb is required for DuckDBSQLProcessor")
        self.sql = sql
        self.tables = tables or {}
        self.connection = connection

    def process(self) -> pd.DataFrame:
        """Execute the SQL and return result as DataFrame."""
        conn = self.connection or duckdb.connect(":memory:")

        # Register DataFrames as tables
        for name, df in self.tables.items():
            conn.register(name, df)

        return conn.sql(self.sql).df()

    def __repr__(self) -> str:
        tables = ", ".join(self.tables.keys()) if self.tables else "none"
        return f"DuckDBSQLProcessor(tables=[{tables}])"
