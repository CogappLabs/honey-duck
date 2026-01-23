"""Processor chaining for Polars with lazy evaluation optimization.

Chain multiple Polars processors together - they are optimized as a single query.
"""

from __future__ import annotations

import pandas as pd
import polars as pl


class Chain:
    """Chain multiple Polars processors with lazy evaluation optimization.

    All processors in the chain must have a `_apply(LazyFrame) -> LazyFrame` method.
    The entire chain is executed as a single optimized Polars query.

    Example:
        from cogapp_libs.processors import Chain
        from cogapp_libs.processors.polars import PolarsStringProcessor, PolarsFilterProcessor

        chain = Chain([
            PolarsStringProcessor("name", "upper"),
            PolarsFilterProcessor("price", 1000, ">="),
        ])
        result = chain.process(df)  # single optimized query
    """

    def __init__(self, processors: list):
        if not processors:
            raise ValueError("Chain requires at least one processor")

        # Validate all processors support lazy evaluation
        for p in processors:
            if not hasattr(p, "_apply"):
                raise TypeError(
                    f"{type(p).__name__} does not support chaining. "
                    "Polars processors must have an _apply(LazyFrame) method."
                )

        self.processors = processors

    def process(
        self, df: pd.DataFrame | pl.DataFrame | pl.LazyFrame
    ) -> pl.DataFrame | pl.LazyFrame:
        """Execute the processor chain with lazy optimization.

        Args:
            df: Input DataFrame (pandas, polars DataFrame, or polars LazyFrame)

        Returns:
            Processed DataFrame/LazyFrame (same type as input for polars types)
        """
        # Track if input was lazy (for return type)
        input_is_lazy = isinstance(df, pl.LazyFrame)

        # Convert to LazyFrame for processing
        if isinstance(df, pd.DataFrame):
            lf = pl.from_pandas(df).lazy()
        elif isinstance(df, pl.LazyFrame):
            lf = df
        else:
            lf = df.lazy()

        # Apply all processors as lazy operations
        for processor in self.processors:
            lf = processor._apply(lf)

        # LazyFrame in -> LazyFrame out (for streaming)
        if input_is_lazy:
            return lf

        # DataFrame in -> DataFrame out
        return lf.collect()

    def __repr__(self) -> str:
        names = [type(p).__name__ for p in self.processors]
        return f"Chain([{', '.join(names)}])"
