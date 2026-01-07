"""DuckDB join processor for multi-table joins.

Execute SQL joins and return DataFrames.

DEPRECATED: Use DuckDBQueryProcessor or DuckDBSQLProcessor with explicit SQL instead.
Explicit SQL is more readable and doesn't require learning alias conventions.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import duckdb


class DuckDBJoinProcessor:
    """Execute multi-table joins and return DataFrame.

    .. deprecated::
        Use DuckDBQueryProcessor (for database queries) or DuckDBSQLProcessor
        (for DataFrame transforms) with explicit SQL instead. Explicit SQL is
        more readable and universally understood.

        Instead of:
            DuckDBJoinProcessor(
                base_table="sales",
                joins=[("artworks", "a.artwork_id", "artwork_id")],
                select_cols=["a.*", "b.title"],
            ).process()

        Use:
            DuckDBQueryProcessor(sql='''
                SELECT s.*, a.title
                FROM sales s
                LEFT JOIN artworks a ON s.artwork_id = a.artwork_id
            ''').process()

    Alias convention (if you must use this class):
        - Base table: aliased as 'a'
        - First join: aliased as 'b'
        - Second join: aliased as 'c'
        - And so on...
    """

    def __init__(
        self,
        joins: list[tuple[str, str, str]],  # (table, left_key, right_key)
        select_cols: list[str] | None = None,
        join_type: str = "LEFT",
        base_table: str | None = None,
    ):
        """Initialize join processor.

        Args:
            joins: List of (table, left_key, right_key) tuples.
                   Tables are aliased as 'b', 'c', 'd', etc.
                   If left_key contains '.', used as-is; otherwise prefixed with 'a.'.
            select_cols: Columns to select. Defaults to ["*"].
            join_type: Type of join (LEFT, INNER, etc.). Defaults to LEFT.
            base_table: Name of base table (aliased as 'a'). If None, expects
                       DataFrame input in process().
        """
        warnings.warn(
            "DuckDBJoinProcessor is deprecated. Use DuckDBQueryProcessor or "
            "DuckDBSQLProcessor with explicit SQL instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.base_table = base_table
        self.joins = joins
        self.select_cols = select_cols or ["*"]
        self.join_type = join_type.upper()

    def _generate_sql(self, base: str) -> str:
        """Generate the JOIN SQL statement."""
        select = ", ".join(self.select_cols)
        join_clauses = []

        for i, (table, left_key, right_key) in enumerate(self.joins):
            alias = chr(ord("b") + i)  # b, c, d, ...
            # If left_key contains '.', use as-is; otherwise prefix with 'a.'
            left_ref = left_key if "." in left_key else f"a.{left_key}"
            join_clauses.append(
                f"{self.join_type} JOIN {table} {alias} "
                f"ON {left_ref} = {alias}.{right_key}"
            )

        joins_sql = "\n        ".join(join_clauses)
        return f"""SELECT {select}
        FROM {base} a
        {joins_sql}"""

    def process(
        self,
        df: pd.DataFrame | None = None,
        conn: "duckdb.DuckDBPyConnection | None" = None,
    ) -> pd.DataFrame:
        """Execute the join and return result as DataFrame.

        Args:
            df: Input DataFrame to use as base table. If provided, it's registered
                as "_input" and used as the base. Required if base_table not set.
            conn: Optional DuckDB connection. If not provided, uses configured connection.

        Returns:
            DataFrame with joined data.
        """
        from . import get_connection

        # Determine base table
        if df is not None:
            base = "_input"
        elif self.base_table is not None:
            base = self.base_table
        else:
            raise ValueError("Either provide df argument or set base_table in __init__")

        should_close = conn is None
        conn = conn or get_connection()

        try:
            if df is not None:
                conn.register("_input", df)

            sql = self._generate_sql(base)
            return conn.sql(sql).df()
        finally:
            if should_close:
                conn.close()

    def __repr__(self) -> str:
        base = self.base_table or "<input>"
        tables = [base] + [j[0] for j in self.joins]
        return f"DuckDBJoinProcessor({' -> '.join(tables)})"
