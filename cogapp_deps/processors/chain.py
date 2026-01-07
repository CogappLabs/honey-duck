"""Processor chaining for Polars with lazy evaluation optimization.

Chain multiple Polars processors together - they are optimized as a single query.
"""

import pandas as pd
import polars as pl


class Chain:
    """Chain multiple Polars processors with lazy evaluation optimization.

    All processors in the chain must have a `_apply(LazyFrame) -> LazyFrame` method.
    The entire chain is executed as a single optimized Polars query.

    Example:
        from cogapp_deps.processors import Chain
        from cogapp_deps.processors.polars import PolarsStringProcessor, PolarsFilterProcessor

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

    def process(self, df: pd.DataFrame | pl.DataFrame) -> pl.DataFrame:
        """Execute the processor chain with lazy optimization.

        Args:
            df: Input DataFrame (pandas or polars)

        Returns:
            Processed Polars DataFrame
        """
        # Convert pandas to polars LazyFrame
        if isinstance(df, pd.DataFrame):
            lf = pl.from_pandas(df).lazy()
        else:
            lf = df.lazy()

        # Apply all processors as lazy operations
        for processor in self.processors:
            lf = processor._apply(lf)

        # Collect (single optimized execution)
        return lf.collect()

    def __repr__(self) -> str:
        names = [type(p).__name__ for p in self.processors]
        return f"Chain([{', '.join(names)}])"
