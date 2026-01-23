"""DuckDB aggregation processor for group-by operations.

Execute SQL aggregations and return DataFrames.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import duckdb


class DuckDBAggregateProcessor:
    """Execute group-by aggregations and return DataFrame.

    Takes an input DataFrame, registers it as a table, applies aggregations,
    and returns the grouped result.

    Example:
        ```python
        processor = DuckDBAggregateProcessor(
            group_cols=["artwork_id"],
            agg_exprs={
                "total_count": "COUNT(*)",
                "total_value": "SUM(sale_price_usd)",
                "avg_price": "ROUND(AVG(sale_price_usd), 0)",
            },
        )
        aggregated_df = processor.process(sales_df)
        ```
    """

    def __init__(
        self,
        group_cols: list[str],
        agg_exprs: dict[str, str],  # {output_col: sql_expr}
    ):
        """Initialize aggregate processor.

        Args:
            group_cols: Columns to group by.
            agg_exprs: Dict of {output_col: sql_aggregation_expression}.
        """
        self.group_cols = group_cols
        self.agg_exprs = agg_exprs

    def _generate_sql(self) -> str:
        """Generate the aggregation SQL statement."""
        group_str = ", ".join(self.group_cols)
        agg_parts = [f"{expr} AS {name}" for name, expr in self.agg_exprs.items()]
        select_cols = self.group_cols + agg_parts

        return f"""SELECT
            {", ".join(select_cols)}
        FROM _input
        GROUP BY {group_str}"""

    def process(
        self,
        df: pd.DataFrame,
        conn: "duckdb.DuckDBPyConnection | None" = None,
    ) -> pd.DataFrame:
        """Apply aggregations to DataFrame.

        Args:
            df: Input DataFrame.
            conn: Optional DuckDB connection. If not provided, uses in-memory connection.

        Returns:
            Aggregated DataFrame.
        """
        import duckdb as ddb

        # For aggregations, we always use in-memory since we're working
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
        return f"DuckDBAggregateProcessor(GROUP BY {', '.join(self.group_cols)})"
