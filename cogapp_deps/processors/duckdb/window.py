"""DuckDB window processor for ranking and running aggregations.

Generate SQL for window functions like ROW_NUMBER, SUM OVER, etc.
"""


class DuckDBWindowProcessor:
    """Generate SQL for window functions.

    Example:
        >>> processor = DuckDBWindowProcessor(
        ...     exprs={
        ...         "rank_in_artist": "ROW_NUMBER() OVER (PARTITION BY artist ORDER BY price DESC)",
        ...         "artist_total": "SUM(price) OVER (PARTITION BY artist)",
        ...     }
        ... )
        >>> sql_exprs = processor.generate_sql()  # Returns list of expressions
    """

    def __init__(self, exprs: dict[str, str]):
        """
        Args:
            exprs: Dict of {output_col: window_expression}
        """
        self.exprs = exprs

    def generate_sql(self) -> list[str]:
        """Generate list of window function SQL expressions."""
        return [f"{expr} AS {name}" for name, expr in self.exprs.items()]

    def __repr__(self) -> str:
        return f"DuckDBWindowProcessor({list(self.exprs.keys())})"
