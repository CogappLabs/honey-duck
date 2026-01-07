"""Dagster IO helpers for DuckDB-based pipelines.

Provides common patterns for reading from DuckDB and writing outputs.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Sequence

import dagster as dg
import pandas as pd
from dagster_duckdb import DuckDBIOManager
from dagster_duckdb.io_manager import DbTypeHandler
from dagster_duckdb_pandas import DuckDBPandasTypeHandler
from dagster_duckdb_polars import DuckDBPolarsTypeHandler

from cogapp_deps.processors.duckdb import get_connection

if TYPE_CHECKING:
    import polars as pl


class DuckDBPandasPolarsIOManager(DuckDBIOManager):
    """DuckDB IO manager that handles both Pandas and Polars DataFrames.

    Use this when your pipeline mixes pandas and polars operations.
    The IO manager will store/load the appropriate type based on type hints.

    Example:
        defs = dg.Definitions(
            resources={
                "io_manager": DuckDBPandasPolarsIOManager(
                    database="path/to/db.duckdb",
                    schema="main",
                ),
            },
            ...
        )
    """

    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [DuckDBPandasTypeHandler(), DuckDBPolarsTypeHandler()]


def read_table(table_name: str, schema: str = "main") -> pd.DataFrame:
    """Read a table from DuckDB.

    Args:
        table_name: Name of the table
        schema: Schema name (default: "main" for transform tables, use "raw" for harvest)

    Returns:
        DataFrame with table contents
    """
    conn = get_connection()
    try:
        return conn.sql(f"SELECT * FROM {schema}.{table_name}").df()
    finally:
        conn.close()


def write_json_output(
    df: pd.DataFrame | "pl.DataFrame",
    output_path: Path,
    context: dg.AssetExecutionContext,
    extra_metadata: dict | None = None,
) -> None:
    """Write DataFrame to JSON and add standard output metadata.

    Creates parent directories if needed. Adds record_count, path, and preview
    to asset metadata automatically. Accepts both pandas and polars DataFrames.

    Args:
        df: DataFrame to write (pandas or polars)
        output_path: Path to write JSON file
        context: Dagster asset execution context
        extra_metadata: Additional metadata to include (optional)
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Convert polars to pandas for consistent JSON output
    if not isinstance(df, pd.DataFrame):
        df = df.to_pandas()

    df.to_json(output_path, orient="records", indent=2)

    context.add_output_metadata({
        "record_count": len(df),
        "json_output": dg.MetadataValue.path(str(output_path)),
        "preview": dg.MetadataValue.md(df.head(10).to_markdown(index=False)),
        **(extra_metadata or {}),
    })
