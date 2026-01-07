"""Cogapp data processors organized by engine type.

Subpackages:
    - pandas/  : Standard pandas operations (widest compatibility)
    - polars/  : High-performance operations (2-10x faster for strings, filtering)
    - duckdb/  : SQL-based operations (best for joins, aggregations, window functions)

Processor Selection Guide:
    - Simple transformations: Use pandas (most familiar)
    - Large datasets + strings/filtering: Use polars (faster)
    - Joins, aggregations, window functions: Use duckdb (most efficient)
    - Persisted intermediate results: DuckDB via Dagster IO manager

Chaining:
    Use Chain to combine multiple processors with lazy optimization (for Polars):

        from cogapp_deps.processors import Chain
        from cogapp_deps.processors.polars import PolarsStringProcessor, PolarsFilterProcessor

        chain = Chain([
            PolarsStringProcessor("name", "upper"),
            PolarsFilterProcessor("price", 1000, ">="),
        ])
        result = chain.process(df)  # single optimized query
"""

from . import duckdb, pandas
from .chain import Chain

try:
    from . import polars

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

__all__ = [
    "Chain",
    "pandas",
    "duckdb",
    "polars",
    "POLARS_AVAILABLE",
]
