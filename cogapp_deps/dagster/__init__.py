"""Dagster helpers for Cogapp ETL pipelines.

Provides reusable patterns for DuckDB-based Dagster pipelines.
"""

from cogapp_deps.dagster.io import read_table, write_json_output

__all__ = ["read_table", "write_json_output"]
