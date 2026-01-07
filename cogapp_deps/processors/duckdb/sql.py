"""DuckDB SQL processors for executing raw SQL queries.

Two processors for different use cases:
- DuckDBSQLProcessor: Transform DataFrames with SQL (in-memory)
- DuckDBQueryProcessor: Query configured database tables (persistent)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import duckdb


class DuckDBSQLProcessor:
    """Transform DataFrames using SQL queries.

    Registers input DataFrame as "_input" table and executes SQL in-memory.
    Use for transforming DataFrames with complex SQL logic.

    Example:
        >>> processor = DuckDBSQLProcessor(sql=\"\"\"
        ...     SELECT *,
        ...         price * 1.1 AS price_with_tax
        ...     FROM _input
        ...     WHERE status = 'active'
        ... \"\"\")
        >>> result = processor.process(input_df)

    For multi-table operations, pass additional DataFrames via `tables`:
        >>> processor = DuckDBSQLProcessor(sql=\"\"\"
        ...     SELECT a.*, b.category_name
        ...     FROM _input a
        ...     LEFT JOIN categories b ON a.category_id = b.id
        ... \"\"\")
        >>> result = processor.process(products_df, tables={"categories": categories_df})
    """

    def __init__(self, sql: str):
        """Initialize SQL processor.

        Args:
            sql: SQL query to execute. Use "_input" to reference input DataFrame.
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
        preview = self.sql.strip()[:50].replace("\n", " ")
        return f"DuckDBSQLProcessor({preview}...)"


class DuckDBQueryProcessor:
    """Query tables in the configured DuckDB database.

    Executes SQL against persistent database tables. Requires configure() first.
    Use for extracting data from the database without DataFrame input.

    Example:
        >>> from cogapp_deps.processors.duckdb import configure
        >>> configure(db_path="warehouse.duckdb", read_only=True)
        >>>
        >>> processor = DuckDBQueryProcessor(sql=\"\"\"
        ...     SELECT s.*, a.title
        ...     FROM raw.sales s
        ...     JOIN raw.artworks a ON s.artwork_id = a.artwork_id
        ...     WHERE s.sale_date > '2024-01-01'
        ... \"\"\")
        >>> result = processor.process()
    """

    def __init__(self, sql: str):
        """Initialize query processor.

        Args:
            sql: SQL query to execute against configured database.
        """
        self.sql = sql

    def process(self) -> pd.DataFrame:
        """Execute the SQL against configured database and return result.

        Returns:
            DataFrame with query results.

        Raises:
            RuntimeError: If database is not configured or connection fails.
        """
        from . import get_connection

        conn = get_connection()
        try:
            return conn.sql(self.sql).df()
        finally:
            conn.close()

    def __repr__(self) -> str:
        preview = self.sql.strip()[:50].replace("\n", " ")
        return f"DuckDBQueryProcessor({preview}...)"
