"""Dagster helpers for Cogapp ETL pipelines.

Provides reusable patterns for DuckDB-based Dagster pipelines.

Note: Requires dagster to be installed. Install with:
    pip install cogapp-deps[dagster]
"""

try:
    import dagster as _dg  # noqa: F401
except ImportError as e:
    raise ImportError(
        "dagster is required for cogapp_deps.dagster. "
        "Install with: pip install cogapp-deps[dagster]"
    ) from e

from cogapp_deps.dagster.io import (
    DuckDBPandasPolarsIOManager,
    read_table,
    write_json_output,
)

__all__ = ["DuckDBPandasPolarsIOManager", "read_table", "write_json_output"]
