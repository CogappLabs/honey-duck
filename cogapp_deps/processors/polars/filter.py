"""Polars filter processors for high-performance row selection.

Supports lazy evaluation for query optimization when chained.
"""

from __future__ import annotations

import pandas as pd

try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None  # type: ignore[assignment]


class PolarsFilterProcessor:
    """High-performance filter processor using Polars.

    Supports lazy evaluation when used in a Chain for query optimization.

    Example:
        >>> processor = PolarsFilterProcessor("price", 1000, ">=")
        >>> filtered_df = processor.process(df)

        # Chained with optimization:
        >>> chain = Chain([
        ...     PolarsStringProcessor("name", "upper"),
        ...     PolarsFilterProcessor("price", 1000, ">="),
        ... ])
        >>> result = chain.process(df)  # single optimized query
    """

    def __init__(self, column: str, threshold: float, operator: str = ">="):
        if not POLARS_AVAILABLE:
            raise ImportError(
                "polars is required for PolarsFilterProcessor. Install with: pip install polars"
            )
        self.column = column
        self.threshold = threshold
        self.operator = operator

    def _get_filter_expr(self) -> "pl.Expr":
        """Build the Polars filter expression."""
        col = pl.col(self.column)
        if self.operator == ">=":
            return col >= self.threshold
        elif self.operator == ">":
            return col > self.threshold
        elif self.operator == "<=":
            return col <= self.threshold
        elif self.operator == "<":
            return col < self.threshold
        elif self.operator == "==":
            return col == self.threshold
        elif self.operator == "!=":
            return col != self.threshold
        else:
            raise ValueError(f"Unknown operator: {self.operator}")

    def _apply(self, lf: "pl.LazyFrame") -> "pl.LazyFrame":
        """Apply filter to LazyFrame (for chaining optimization)."""
        return lf.filter(self._get_filter_expr())

    def process(
        self, df: pd.DataFrame | "pl.DataFrame" | "pl.LazyFrame"
    ) -> "pl.DataFrame | pl.LazyFrame":
        """Apply the filter.

        Args:
            df: Input DataFrame (pandas, polars DataFrame, or polars LazyFrame)

        Returns:
            Filtered DataFrame/LazyFrame (same type as input for polars types)
        """
        # LazyFrame in -> LazyFrame out (for streaming)
        if isinstance(df, pl.LazyFrame):
            return self._apply(df)

        # Convert pandas to polars if needed
        if isinstance(df, pd.DataFrame):
            pl_df = pl.from_pandas(df)
        else:
            pl_df = df

        # DataFrame in -> DataFrame out
        return self._apply(pl_df.lazy()).collect()

    def __repr__(self) -> str:
        return f"PolarsFilterProcessor({self.column} {self.operator} {self.threshold})"
