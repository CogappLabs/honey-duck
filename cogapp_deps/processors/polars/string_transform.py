"""Polars string transformation processors for high-performance text operations.

Polars string operations are 2-10x faster than pandas due to:
- Arrow-native string representation
- SIMD vectorization
- Zero-copy operations

Use for large datasets where string transformations are a bottleneck.
Supports lazy evaluation for query optimization when chained.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import pandas as pd

if TYPE_CHECKING:
    import polars as pl

try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

TransformType = Literal["upper", "lower", "title", "strip"]


class PolarsStringProcessor:
    """High-performance string processor using Polars.

    Significantly faster than pandas for string operations on large datasets.
    Automatically handles pandas <-> polars conversion.
    Supports lazy evaluation when used in a Chain.

    Example:
        ```python
        processor = PolarsStringProcessor("artist_name", "upper")
        result_df = processor.process(df)  # accepts pandas or polars

        # Chained with optimization:
        chain = Chain([
            PolarsStringProcessor("name", "upper"),
            PolarsFilterProcessor("price", 1000, ">="),
        ])
        result = chain.process(df)  # single optimized query
        ```
    """

    def __init__(self, column: str, transform: TransformType = "upper"):
        if not POLARS_AVAILABLE:
            raise ImportError(
                "polars is required for PolarsStringProcessor. Install with: pip install polars"
            )
        self.column = column
        self.transform = transform

    def _apply(self, lf: "pl.LazyFrame") -> "pl.LazyFrame":
        """Apply transformation to LazyFrame (for chaining optimization)."""
        if self.transform == "upper":
            return lf.with_columns(pl.col(self.column).str.to_uppercase())
        elif self.transform == "lower":
            return lf.with_columns(pl.col(self.column).str.to_lowercase())
        elif self.transform == "title":
            return lf.with_columns(pl.col(self.column).str.to_titlecase())
        elif self.transform == "strip":
            return lf.with_columns(pl.col(self.column).str.strip_chars())
        return lf

    def process(
        self, df: pd.DataFrame | "pl.DataFrame" | "pl.LazyFrame"
    ) -> "pl.DataFrame | pl.LazyFrame":
        """Apply the string transformation.

        Args:
            df: Input DataFrame (pandas, polars DataFrame, or polars LazyFrame)

        Returns:
            Transformed DataFrame/LazyFrame (same type as input for polars types)
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
        return f"PolarsStringProcessor({self.column}, {self.transform})"


def uppercase_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Convert a string column to uppercase using Polars.

    High-performance alternative to pandas str.upper().
    Handles pandas DataFrame input/output automatically.

    Args:
        df: Input pandas DataFrame
        column: Name of the string column to transform

    Returns:
        DataFrame with transformed column

    Example:
        ```python
        df = uppercase_column(df, "artist_name")
        df["artist_name"].iloc[0]  # 'VINCENT VAN GOGH'
        ```
    """
    if not POLARS_AVAILABLE:
        # Fallback to pandas if polars not available
        df = df.copy()
        df[column] = df[column].str.upper()
        return df

    processor = PolarsStringProcessor(column, "upper")
    result = processor.process(df)
    # Convert back to pandas for backwards compatibility
    if hasattr(result, "to_pandas"):
        return result.to_pandas()
    return result  # type: ignore[return-value]
