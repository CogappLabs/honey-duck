"""Generic DuckDB SQL processor for executing raw SQL queries.

Use this for custom SQL that doesn't fit the specific processors (join, window, aggregate).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import duckdb


class DuckDBSQLProcessor:
    """Execute raw SQL queries against DataFrames.

    Registers the input DataFrame as "_input" table and executes the provided SQL.
    Use for complex queries that don't fit the specific processors.

    Example:
        >>> processor = DuckDBSQLProcessor(
        ...     sql=\"\"\"
        ...         SELECT *,
        ...             sale_price_usd - list_price_usd AS price_diff,
        ...             ROUND((sale_price_usd - list_price_usd) * 100.0 / list_price_usd, 1) AS pct_change
        ...         FROM _input
        ...         ORDER BY sale_date DESC
        ...     \"\"\"
        ... )
        >>> result = processor.process(input_df)
    """

    def __init__(self, sql: str):
        """Initialize SQL processor.

        Args:
            sql: SQL query to execute. Use "_input" to reference the input DataFrame.
        """
        self.sql = sql

    def process(
        self,
        df: pd.DataFrame,
        tables: dict[str, pd.DataFrame] | None = None,
    ) -> pd.DataFrame:
        """Execute the SQL and return result as DataFrame.

        Args:
            df: Input DataFrame, registered as "_input" table.
            tables: Additional DataFrames to register as named tables for JOINs.

        Returns:
            DataFrame with query results.
        """
        import duckdb as ddb

        conn = ddb.connect(":memory:")
        try:
            conn.register("_input", df)
            if tables:
                for name, table_df in tables.items():
                    conn.register(name, table_df)
            return conn.sql(self.sql).df()
        finally:
            conn.close()

    def __repr__(self) -> str:
        # Show first 50 chars of SQL
        preview = self.sql.strip()[:50].replace("\n", " ")
        return f"DuckDBSQLProcessor({preview}...)"
