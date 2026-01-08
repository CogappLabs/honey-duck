"""Dagster IO helpers for DuckDB-based pipelines.

Provides common patterns for reading from DuckDB and writing outputs.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Sequence

import dagster as dg
import duckdb
import pandas as pd
from dagster_duckdb import DuckDBIOManager
from dagster_duckdb.io_manager import DbTypeHandler, DuckDBIOManager as _BaseDuckDBIOManager
from dagster_duckdb_pandas import DuckDBPandasTypeHandler
from dagster_duckdb_polars import DuckDBPolarsTypeHandler

from cogapp_deps.processors.duckdb import get_connection

if TYPE_CHECKING:
    import polars as pl


class DuckDBRelationTypeHandler(DbTypeHandler):
    """Type handler for DuckDB relations (lazy query results).

    EXPERIMENTAL: This type handler has connection lifecycle challenges.
    DuckDB relations hold references to their originating connection, which
    may be closed or conflict with the IO manager's connection. Use with
    caution and ensure connections remain open while relations are in use.

    For production use, returning Polars DataFrames is more reliable.

    Allows assets to return duckdb.DuckDBPyRelation objects, which are
    materialized directly to DuckDB tables without DataFrame conversion.

    On store: Exports relation to Arrow, imports via IO manager connection
    On load: Returns a Polars DataFrame (for downstream compatibility)
    """

    def handle_output(
        self,
        context: dg.OutputContext,
        table_slice: "_BaseDuckDBIOManager.TableSlice",
        obj: duckdb.DuckDBPyRelation,
        connection: duckdb.DuckDBPyConnection,
    ) -> None:
        """Materialize a DuckDB relation to a table.

        Uses Arrow as an intermediate format to transfer data between
        the relation's connection and the IO manager's connection.
        """
        table_name = f"{table_slice.schema}.{table_slice.table}"

        # Export relation to Arrow (connection-independent format)
        arrow_table = obj.arrow()

        # Import Arrow table into IO manager's connection
        connection.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM arrow_table")
        context.log.info(f"Materialized relation ({len(arrow_table)} rows) to {table_name}")

    def load_input(
        self,
        context: dg.InputContext,
        table_slice: "_BaseDuckDBIOManager.TableSlice",
        connection: duckdb.DuckDBPyConnection,
    ):
        """Load a table as a Polars DataFrame for downstream compatibility."""
        import polars as pl

        table_name = f"{table_slice.schema}.{table_slice.table}"
        return connection.sql(f"SELECT * FROM {table_name}").pl()

    @property
    def supported_types(self) -> Sequence[type]:
        return [duckdb.DuckDBPyRelation]


class DuckDBPandasPolarsIOManager(DuckDBIOManager):
    """DuckDB IO manager that handles Pandas, Polars, and DuckDB relations.

    Use this when your pipeline mixes pandas, polars, and native DuckDB operations.
    The IO manager will store/load the appropriate type based on type hints.

    Supported types:
        - pandas.DataFrame: Stored/loaded via DuckDBPandasTypeHandler
        - polars.DataFrame: Stored/loaded via DuckDBPolarsTypeHandler
        - duckdb.DuckDBPyRelation: Materialized directly to table, loaded as Polars

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
        return [
            DuckDBRelationTypeHandler(),
            DuckDBPandasTypeHandler(),
            DuckDBPolarsTypeHandler(),
        ]


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
    """Write DataFrame to JSON using DuckDB's native COPY command.

    Creates parent directories if needed. Adds record_count, path, and preview
    to asset metadata automatically. Accepts both pandas and polars DataFrames.

    Uses DuckDB's native JSON export for better performance - avoids the
    DataFrame → pandas → json serialization path.

    Args:
        df: DataFrame to write (pandas or polars)
        output_path: Path to write JSON file
        context: Dagster asset execution context
        extra_metadata: Additional metadata to include (optional)
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Use DuckDB's native JSON export
    conn = duckdb.connect(":memory:")
    conn.register("_df", df)
    conn.execute(f"COPY (SELECT * FROM _df) TO '{output_path}' (FORMAT JSON, ARRAY true)")

    # Get preview as pandas for markdown rendering
    preview_df = conn.sql("SELECT * FROM _df LIMIT 10").df()
    record_count = conn.sql("SELECT COUNT(*) FROM _df").fetchone()[0]
    conn.close()

    context.add_output_metadata({
        "record_count": record_count,
        "json_output": dg.MetadataValue.path(str(output_path)),
        "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        **(extra_metadata or {}),
    })


def write_json_from_duckdb(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    output_path: Path,
    context: dg.AssetExecutionContext,
    extra_metadata: dict | None = None,
) -> int:
    """Write query results to JSON using DuckDB's native COPY command.

    This avoids DataFrame conversion overhead by using DuckDB's built-in
    JSON export. Creates parent directories if needed.

    Args:
        conn: DuckDB connection to use
        sql: SQL query to export (will be wrapped in COPY)
        output_path: Path to write JSON file
        context: Dagster asset execution context
        extra_metadata: Additional metadata to include (optional)

    Returns:
        Number of records written
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Get record count and preview before export
    result = conn.sql(sql)
    record_count = result.count("*").fetchone()[0]
    preview_df = conn.sql(f"SELECT * FROM ({sql}) LIMIT 10").df()

    # Use DuckDB's native JSON export
    conn.execute(f"COPY ({sql}) TO '{output_path}' (FORMAT JSON, ARRAY true)")

    context.add_output_metadata({
        "record_count": record_count,
        "json_output": dg.MetadataValue.path(str(output_path)),
        "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        **(extra_metadata or {}),
    })

    return record_count
