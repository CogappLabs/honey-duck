"""DuckDB window processor for ranking and running aggregations.

Execute SQL window functions and return DataFrames.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import duckdb


class DuckDBWindowProcessor:
    """Execute window functions and return DataFrame with added columns.

    Takes an input DataFrame, registers it as a table, applies window functions,
    and returns the result with new columns added.

    Example:
        ```python
        processor = DuckDBWindowProcessor(
            exprs={
                "rank_in_artist": "ROW_NUMBER() OVER (PARTITION BY artist ORDER BY price DESC)",
                "artist_total": "SUM(price) OVER (PARTITION BY artist)",
            }
        )
        df_with_windows = processor.process(input_df)
        ```
    """

    def __init__(self, exprs: dict[str, str]):
        """Initialize window processor.

        Args:
            exprs: Dict of {output_col: window_expression}.
                   Each expression should be a valid SQL window function.
        """
        self.exprs = exprs

    def _generate_sql(self) -> str:
        """Generate SQL with window expressions."""
        window_exprs = ", ".join(f"{expr} AS {name}" for name, expr in self.exprs.items())
        return f"SELECT *, {window_exprs} FROM _input"

    def process(
        self,
        df: pd.DataFrame,
        conn: "duckdb.DuckDBPyConnection | None" = None,
    ) -> pd.DataFrame:
        """Apply window functions to DataFrame.

        Args:
            df: Input DataFrame.
            conn: Optional DuckDB connection. If not provided, uses in-memory connection.

        Returns:
            DataFrame with window columns added.
        """
        import duckdb as ddb

        # For window operations, we always use in-memory since we're working
        # with a DataFrame that's passed in (not reading from persistent tables)
        should_close = conn is None
        conn = conn or ddb.connect(":memory:")

        try:
            conn.register("_input", df)
            sql = self._generate_sql()
            return conn.sql(sql).df()
        finally:
            if should_close:
                conn.close()

    def __repr__(self) -> str:
        return f"DuckDBWindowProcessor({list(self.exprs.keys())})"
