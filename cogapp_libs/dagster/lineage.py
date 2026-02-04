"""Column lineage helpers for Dagster assets.

Provides utilities for generating example row data for column lineage visualization.
"""

from collections.abc import Iterator
from typing import Any

import dagster as dg
import duckdb


def format_value(val: Any) -> str | None:
    """Format a value for display in lineage examples.

    Formats numbers with appropriate units:
    - >= 1M: $1.2M
    - >= 1K: $1.5K
    - floats: $123.45
    - ints: 1,234

    Args:
        val: Any value to format

    Returns:
        Formatted string or None
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        abs_val = abs(val)
        sign = "-" if val < 0 else ""
        if abs_val >= 1_000_000:
            return f"{sign}${abs_val / 1_000_000:.1f}M"
        if abs_val >= 1_000:
            return f"{sign}${abs_val / 1_000:.1f}K"
        if isinstance(val, float):
            return f"{sign}${abs_val:.2f}"
        return f"{val:,}"
    return str(val)


def get_example_row(
    conn: duckdb.DuckDBPyConnection,
    path: str,
    id_field: str | None = None,
    id_value: int | str | None = None,
) -> dict[str, str | None]:
    """Get one example row from a parquet file for lineage display.

    Args:
        conn: DuckDB connection
        path: Path to parquet file (can include globs)
        id_field: Optional field name to filter by (e.g., "sale_id")
        id_value: Optional value to match (e.g., 2)

    Returns:
        Dict mapping column names to formatted values.
        If id_field/id_value provided, returns that specific record.
        Otherwise returns the first row.

    Example:
        >>> examples = get_example_row(conn, "data/sales.parquet", "sale_id", 2)
        >>> # {"sale_id": "2", "price": "$86.3M", "title": "Day Dream"}
    """
    try:
        if id_field and id_value is not None:
            val = f"'{id_value}'" if isinstance(id_value, str) else id_value
            result = conn.sql(f"SELECT * FROM '{path}' WHERE {id_field} = {val} LIMIT 1")
        else:
            result = conn.sql(f"SELECT * FROM '{path}' LIMIT 1")

        row = result.fetchone()
        if not row:
            return {}
        return {col: format_value(val) for col, val in zip(result.columns, row)}
    except Exception:
        return {}


def add_lineage_examples_to_dlt_results(
    results: list[dg.MaterializeResult | dg.AssetMaterialization],
    harvest_dir: str,
    config: dict[str, dict],
) -> Iterator[dg.MaterializeResult | dg.AssetMaterialization]:
    """Add lineage examples to dlt MaterializeResults.

    Simplifies adding example row data to dlt harvest assets.

    Args:
        results: List of MaterializeResult from dlt.run()
        harvest_dir: Base directory for harvest parquet files
        config: Dict mapping asset names to config with keys:
            - path: Relative path/glob to parquet files
            - id_field: Optional field name to filter by
            - id_value: Optional value to match

    Yields:
        MaterializeResult with lineage_examples metadata added

    Example:
        ```python
        ASSET_CONFIG = {
            "dlt_harvest_sales_raw": {
                "path": "raw/sales_raw/**/*.parquet",
                "id_field": "sale_id",
                "id_value": 2,
            },
        }

        @dg.multi_asset(specs=HARVEST_ASSET_SPECS)
        def dlt_harvest_assets(context, dlt, paths, database):
            results = list(dlt.run(...))
            yield from add_lineage_examples_to_dlt_results(
                results, paths.harvest_dir, ASSET_CONFIG
            )
        ```
    """
    with duckdb.connect(":memory:") as conn:
        for result in results:
            asset_key = result.asset_key
            if asset_key:
                asset_name = asset_key.to_user_string()
                cfg = config.get(asset_name)
                if cfg:
                    parquet_path = f"{harvest_dir}/{cfg['path']}"
                    examples = get_example_row(
                        conn,
                        parquet_path,
                        cfg.get("id_field"),
                        cfg.get("id_value"),
                    )
                    if examples:
                        metadata = dict(result.metadata) if result.metadata else {}
                        metadata["lineage_examples"] = examples
                        yield dg.MaterializeResult(asset_key=asset_key, metadata=metadata)
                        continue
            yield result


__all__ = ["format_value", "get_example_row", "add_lineage_examples_to_dlt_results"]
