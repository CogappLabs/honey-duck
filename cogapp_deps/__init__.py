"""Cogapp data processing utilities.

This package provides reusable DataFrame processors for common
data transformation patterns.

Structure:
    cogapp_deps/
    └── processors/
        ├── pandas/   - Aggregations, conditional replacements
        ├── polars/   - Filtering, string transforms (chainable)
        └── duckdb/   - SQL-based operations

Example:
    >>> from cogapp_deps.processors import Chain
    >>> from cogapp_deps.processors.polars import PolarsStringProcessor, PolarsFilterProcessor
    >>> chain = Chain([
    ...     PolarsStringProcessor("name", "upper"),
    ...     PolarsFilterProcessor("price", 1000, ">="),
    ... ])
    >>> result = chain.process(df)
"""

from .processors import duckdb, pandas

try:
    from .processors import polars

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

__all__ = [
    "processors",
    "pandas",
    "duckdb",
    "polars",
    "POLARS_AVAILABLE",
]
