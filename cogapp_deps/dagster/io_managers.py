"""Custom IO Managers for Dagster pipelines.

Provides JSON IO Manager for writing final outputs using Dagster's IO system.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import dagster as dg
import duckdb
from dagster import InputContext, OutputContext
from dagster._core.storage.upath_io_manager import UPathIOManager

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    from upath import UPath


class JSONIOManager(UPathIOManager):
    """IO Manager that writes DataFrames to JSON files using DuckDB's native export.

    Integrates JSON output writing into Dagster's IO system rather than as a side effect.
    Supports both Polars and Pandas DataFrames.

    Features:
    - Uses DuckDB's native COPY command for performance
    - Handles both pandas and polars DataFrames
    - Automatically creates parent directories
    - Provides standard metadata (record count, preview, path)

    Args:
        base_path: Base directory for JSON outputs (default: "data/output/json")
        extension: File extension (default: ".json")

    Example:
        >>> # In definitions.py:
        >>> defs = dg.Definitions(
        ...     assets=[...],
        ...     resources={
        ...         "json_io_manager": JSONIOManager(base_path="data/output/json"),
        ...     },
        ... )
        >>>
        >>> # In asset:
        >>> @dg.asset(io_manager_key="json_io_manager")
        >>> def sales_output(sales_transform: pl.DataFrame) -> pl.DataFrame:
        ...     # JSON is written automatically by IO manager
        ...     return sales_transform.filter(pl.col("price") > 1000)
    """

    extension: str = ".json"

    def dump_to_path(self, context: OutputContext, obj: pd.DataFrame | pl.DataFrame, path: UPath):
        """Write DataFrame to JSON file using DuckDB's native COPY command.

        Args:
            context: Dagster output context
            obj: DataFrame to write (pandas or polars)
            path: UPath to write JSON file
        """
        # Ensure parent directory exists
        path.parent.mkdir(parents=True, exist_ok=True)

        # Convert UPath to string for DuckDB
        output_path_str = str(path)

        # Use DuckDB's native JSON export for performance
        conn = duckdb.connect(":memory:")
        conn.register("_df", obj)
        conn.execute(f"COPY (SELECT * FROM _df) TO '{output_path_str}' (FORMAT JSON, ARRAY true)")

        # Get preview as pandas for markdown rendering
        preview_df = conn.sql("SELECT * FROM _df LIMIT 10").df()
        record_count = conn.sql("SELECT COUNT(*) FROM _df").fetchone()[0]
        conn.close()

        # Add metadata
        context.add_output_metadata({
            "record_count": record_count,
            "json_output": dg.MetadataValue.path(output_path_str),
            "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        })

        context.log.info(f"Wrote {record_count:,} records to {path}")

    def load_from_path(self, context: InputContext, path: UPath) -> pl.DataFrame:
        """Load DataFrame from JSON file.

        Args:
            context: Dagster input context
            path: UPath to read JSON file

        Returns:
            Polars DataFrame (for downstream compatibility)
        """
        import polars as pl

        # Load JSON file as Polars DataFrame
        df = pl.read_json(path)

        context.log.info(f"Loaded {len(df):,} records from {path}")

        return df


__all__ = ["JSONIOManager"]
