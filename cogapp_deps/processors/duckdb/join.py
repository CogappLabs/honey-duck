"""DuckDB join processor for multi-table joins.

Generate SQL for joining tables with computed columns.
"""


class DuckDBJoinProcessor:
    """Generate SQL for multi-table joins.

    Supports both simple joins (all from base table) and chained joins
    (each join references the previous table).

    Example:
        >>> # Simple joins - all from base table 'a'
        >>> processor = DuckDBJoinProcessor(
        ...     base_table="sales",
        ...     joins=[
        ...         ("artworks", "artwork_id", "artwork_id"),
        ...         ("customers", "customer_id", "customer_id"),
        ...     ],
        ...     select_cols=["a.*", "b.title", "c.name"],
        ... )

        >>> # Chained joins - sales -> artworks -> artists
        >>> processor = DuckDBJoinProcessor(
        ...     base_table="sales",
        ...     joins=[
        ...         ("artworks", "a.artwork_id", "artwork_id"),
        ...         ("artists", "b.artist_id", "artist_id"),  # references 'b' (artworks)
        ...     ],
        ...     select_cols=["a.*", "b.title", "c.name"],
        ... )
    """

    def __init__(
        self,
        base_table: str,
        joins: list[tuple[str, str, str]],  # (table, left_key, right_key)
        select_cols: list[str] | None = None,
        join_type: str = "LEFT",
    ):
        self.base_table = base_table
        self.joins = joins
        self.select_cols = select_cols or ["*"]
        self.join_type = join_type.upper()

    def generate_sql(self) -> str:
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
        FROM {self.base_table} a
        {joins_sql}"""

    def __repr__(self) -> str:
        tables = [self.base_table] + [j[0] for j in self.joins]
        return f"DuckDBJoinProcessor({' -> '.join(tables)})"
