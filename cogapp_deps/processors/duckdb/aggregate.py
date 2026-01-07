"""DuckDB aggregation processor for group-by operations.

Generate SQL for aggregating data by grouping columns.
"""


class DuckDBAggregateProcessor:
    """Generate SQL for group-by aggregations.

    Example:
        >>> processor = DuckDBAggregateProcessor(
        ...     source_table="sales",
        ...     group_cols=["artwork_id"],
        ...     agg_exprs={
        ...         "total_count": "COUNT(*)",
        ...         "total_value": "SUM(sale_price_usd)",
        ...         "avg_price": "ROUND(AVG(sale_price_usd), 0)",
        ...     },
        ... )
        >>> sql = processor.generate_sql()
    """

    def __init__(
        self,
        source_table: str,
        group_cols: list[str],
        agg_exprs: dict[str, str],  # {output_col: sql_expr}
    ):
        self.source_table = source_table
        self.group_cols = group_cols
        self.agg_exprs = agg_exprs

    def generate_sql(self) -> str:
        """Generate the aggregation SQL statement."""
        group_str = ", ".join(self.group_cols)
        agg_parts = [f"{expr} AS {name}" for name, expr in self.agg_exprs.items()]
        select_cols = self.group_cols + agg_parts

        return f"""SELECT
            {",\n            ".join(select_cols)}
        FROM {self.source_table}
        GROUP BY {group_str}"""

    def __repr__(self) -> str:
        return f"DuckDBAggregateProcessor({self.source_table} GROUP BY {', '.join(self.group_cols)})"
