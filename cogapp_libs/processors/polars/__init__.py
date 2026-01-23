"""Polars-based DataFrame processors for high-performance transformations.

Polars provides significant performance improvements over pandas for:
- String operations (2-10x faster)
- Lazy evaluation with query optimization

Use polars processors when:
- Processing large datasets (>100K rows)
- String-heavy transformations
- Memory efficiency matters

For joins and aggregations, prefer DuckDB processors instead.

Chaining processors enables lazy optimization:
    chain = Chain([
        PolarsStringProcessor("name", "upper"),
        PolarsFilterProcessor("price", 1000, ">="),
    ])
    result = chain.process(df)  # single optimized query
"""

from .filter import PolarsFilterProcessor
from .string_transform import PolarsStringProcessor, uppercase_column

__all__ = [
    # Processor classes
    "PolarsFilterProcessor",
    "PolarsStringProcessor",
    # String functions
    "uppercase_column",
]
