"""DuckDB SQL processors for executing raw SQL queries.

Two processors for different use cases:
- DuckDBSQLProcessor: Transform DataFrames with SQL (in-memory)
- DuckDBQueryProcessor: Query configured database tables (persistent)

All processors return Polars DataFrames for optimal performance.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    pass


class DuckDBSQLProcessor:
    """Transform DataFrames using SQL queries.

    Registers input DataFrame as "_input" table and executes SQL in-memory.
    Use for transforming DataFrames with complex SQL logic.

    Example:
        ```python
        processor = DuckDBSQLProcessor(sql=\"\"\"
            SELECT *,
                price * 1.1 AS price_with_tax
            FROM _input
            WHERE status = 'active'
        \"\"\")
        result = processor.process(input_df)
        ```

    For multi-table operations, pass additional DataFrames via `tables`:
        ```python
        processor = DuckDBSQLProcessor(sql=\"\"\"
            SELECT a.*, b.category_name
            FROM _input a
            LEFT JOIN categories b ON a.category_id = b.id
        \"\"\")
        result = processor.process(products_df, tables={"categories": categories_df})
        ```

    For lazy output (enables downstream streaming):
        ```python
        processor = DuckDBSQLProcessor(sql="SELECT * FROM _input", lazy=True)
        lazy_result = processor.process(input_df)  # Returns LazyFrame
        ```
    """

    def __init__(self, sql: str, lazy: bool = False):
        """Initialize SQL processor.

        Args:
            sql: SQL query to execute. Use "_input" to reference input DataFrame.
            lazy: If True, return LazyFrame for downstream streaming.
        """
        self.sql = sql
        self.lazy = lazy

    def process(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        tables: dict[str, pl.DataFrame | pl.LazyFrame] | None = None,
    ) -> pl.DataFrame | pl.LazyFrame:
        """Execute the SQL and return result.

        Args:
            df: Input DataFrame/LazyFrame, registered as "_input" table.
            tables: Additional DataFrames to register as named tables for JOINs.

        Returns:
            Polars DataFrame or LazyFrame (if lazy=True).
        """
        import duckdb as ddb

        conn = ddb.connect(":memory:")
        try:
            # Collect LazyFrames for DuckDB registration
            input_df = df.collect() if isinstance(df, pl.LazyFrame) else df
            conn.register("_input", input_df)
            if tables:
                for name, table_df in tables.items():
                    resolved = (
                        table_df.collect() if isinstance(table_df, pl.LazyFrame) else table_df
                    )
                    conn.register(name, resolved)
            result = conn.sql(self.sql).pl()
            return result.lazy() if self.lazy else result
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
        ```python
        from cogapp_libs.processors.duckdb import configure
        configure(db_path="warehouse.duckdb", read_only=True)

        processor = DuckDBQueryProcessor(sql=\"\"\"
            SELECT s.*, a.title
            FROM raw.sales s
            JOIN raw.artworks a ON s.artwork_id = a.artwork_id
            WHERE s.sale_date > '2024-01-01'
        \"\"\")
        result = processor.process()
        ```

    For lazy output (enables downstream streaming):
        ```python
        processor = DuckDBQueryProcessor(sql="SELECT * FROM raw.sales", lazy=True)
        lazy_result = processor.process()  # Returns LazyFrame
        ```
    """

    def __init__(self, sql: str, lazy: bool = False):
        """Initialize query processor.

        Args:
            sql: SQL query to execute against configured database.
            lazy: If True, return LazyFrame for downstream streaming.
        """
        self.sql = sql
        self.lazy = lazy

    def process(self) -> pl.DataFrame | pl.LazyFrame:
        """Execute the SQL against configured database and return result.

        Returns:
            Polars DataFrame or LazyFrame (if lazy=True).

        Raises:
            RuntimeError: If database is not configured or connection fails.
        """
        from . import get_connection

        conn = get_connection()
        try:
            result = conn.sql(self.sql).pl()
            return result.lazy() if self.lazy else result
        finally:
            conn.close()

    def __repr__(self) -> str:
        preview = self.sql.strip()[:50].replace("\n", " ")
        return f"DuckDBQueryProcessor({preview}...)"
