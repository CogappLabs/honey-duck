"""Cogapp data processing utilities.

This package provides reusable DataFrame processors for common
data transformation patterns.

Structure:
    cogapp_libs/
    └── processors/
        ├── polars/   - Filtering, string transforms (chainable)
        └── duckdb/   - SQL-based operations

Example:
    ```python
    from cogapp_libs.processors import Chain
    from cogapp_libs.processors.polars import PolarsStringProcessor, PolarsFilterProcessor
    chain = Chain([
        PolarsStringProcessor("name", "upper"),
        PolarsFilterProcessor("price", 1000, ">="),
    ])
    result = chain.process(df)
    ```
"""

from .processors import duckdb

try:
    from .processors import polars

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

__all__ = [
    "processors",
    "duckdb",
    "polars",
    "POLARS_AVAILABLE",
]
