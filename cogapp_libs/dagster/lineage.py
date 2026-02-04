"""Column lineage helpers for Dagster assets.

Provides utilities for:
- Building column lineage definitions with a compact DSL
- Registering DuckDB views for harvest parquet files
- Collecting metadata for parquet/JSON output assets
- Generating example row data for lineage visualization
"""

from collections.abc import Iterator
from typing import Any

import dagster as dg
import duckdb


# -----------------------------------------------------------------------------
# Lineage DSL Helpers
# -----------------------------------------------------------------------------


def build_lineage(
    passthrough: dict[str, dg.AssetKey] | None = None,
    rename: dict[str, tuple[dg.AssetKey, str]] | None = None,
    computed: dict[str, list[tuple[dg.AssetKey, str]]] | None = None,
) -> dg.TableColumnLineage:
    """Build column lineage from compact definitions.

    Args:
        passthrough: {output_col: source_asset} - same column name in source
        rename: {output_col: (source_asset, source_col)} - renamed columns
        computed: {output_col: [(asset, col), ...]} - derived from multiple columns

    Returns:
        TableColumnLineage for use with dagster/column_lineage metadata

    Example:
        >>> SALES_RAW = dg.AssetKey("dlt_harvest_sales_raw")
        >>> ARTWORKS_RAW = dg.AssetKey("dlt_harvest_artworks_raw")
        >>> lineage = build_lineage(
        ...     passthrough={"sale_id": SALES_RAW, "title": ARTWORKS_RAW},
        ...     rename={"artwork_year": (ARTWORKS_RAW, "year")},
        ...     computed={"price_diff": [(SALES_RAW, "sale_price_usd"), (ARTWORKS_RAW, "price_usd")]},
        ... )
    """
    deps_by_column: dict[str, list[dg.TableColumnDep]] = {}

    # Passthrough: same column name
    if passthrough:
        for col, asset_key in passthrough.items():
            deps_by_column[col] = [dg.TableColumnDep(asset_key=asset_key, column_name=col)]

    # Rename: different column name in source
    if rename:
        for output_col, (asset_key, source_col) in rename.items():
            deps_by_column[output_col] = [
                dg.TableColumnDep(asset_key=asset_key, column_name=source_col)
            ]

    # Computed: derived from multiple columns
    if computed:
        for output_col, deps in computed.items():
            deps_by_column[output_col] = [
                dg.TableColumnDep(asset_key=asset_key, column_name=col) for asset_key, col in deps
            ]

    return dg.TableColumnLineage(deps_by_column=deps_by_column)


def passthrough_lineage(
    source: dg.AssetKey,
    columns: list[str],
    renames: dict[str, str] | None = None,
) -> dg.TableColumnLineage:
    """Build lineage for passthrough assets (output columns match input).

    Args:
        source: Source asset key
        columns: List of column names that pass through unchanged
        renames: {output_col: source_col} for renamed columns

    Returns:
        TableColumnLineage for use with dagster/column_lineage metadata

    Example:
        >>> lineage = passthrough_lineage(
        ...     source=dg.AssetKey("sales_transform"),
        ...     columns=["sale_id", "title", "artist_name"],
        ...     renames={"price_difference": "price_diff"},
        ... )
    """
    deps_by_column: dict[str, list[dg.TableColumnDep]] = {}

    for col in columns:
        deps_by_column[col] = [dg.TableColumnDep(asset_key=source, column_name=col)]

    if renames:
        for output_col, source_col in renames.items():
            deps_by_column[output_col] = [
                dg.TableColumnDep(asset_key=source, column_name=source_col)
            ]

    return dg.TableColumnLineage(deps_by_column=deps_by_column)


# -----------------------------------------------------------------------------
# DuckDB View Registration
# -----------------------------------------------------------------------------

# Mapping of view names to harvest subdirectories
HARVEST_VIEWS = {
    "sales": "sales_raw",
    "artworks": "artworks_raw",
    "artists": "artists_raw",
    "media": "media",
}


def register_harvest_views(
    conn: duckdb.DuckDBPyConnection,
    harvest_dir: str,
    views: list[str] | None = None,
) -> None:
    """Register DuckDB views for harvest parquet files.

    Args:
        conn: DuckDB connection
        harvest_dir: Base harvest directory (e.g., "data/harvest")
        views: View names to register (default: all from HARVEST_VIEWS)

    Example:
        >>> with duckdb.connect() as conn:
        ...     register_harvest_views(conn, "data/harvest", ["sales", "artworks"])
        ...     result = conn.sql("SELECT * FROM sales LIMIT 5")
    """
    views_to_register = views if views is not None else list(HARVEST_VIEWS.keys())
    for view_name in views_to_register:
        subdir = HARVEST_VIEWS.get(view_name, view_name)
        parquet_glob = f"{harvest_dir}/raw/{subdir}/**/*.parquet"
        conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM '{parquet_glob}'")


# -----------------------------------------------------------------------------
# Metadata Collection Helpers
# -----------------------------------------------------------------------------


def collect_parquet_metadata(
    context: dg.AssetExecutionContext,
    conn: duckdb.DuckDBPyConnection,
    output_path: str,
    lineage: dg.TableColumnLineage,
    stats_sql: str,
    example_id: tuple[str, int | str] | None = None,
    extra_metadata: dict | None = None,
    elapsed_ms: float | None = None,
) -> None:
    """Add standard parquet asset metadata to context.

    Args:
        context: Dagster asset execution context
        conn: DuckDB connection (for running stats query and getting examples)
        output_path: Path to the output parquet file
        lineage: Column lineage definition
        stats_sql: SQL returning stat columns (first row used as metadata)
        example_id: (id_field, id_value) for example row lookup
        extra_metadata: Additional metadata to include
        elapsed_ms: Processing time in milliseconds

    Example:
        >>> collect_parquet_metadata(
        ...     context, conn, "data/sales.parquet",
        ...     lineage=SALES_LINEAGE,
        ...     stats_sql="SELECT count(*) as record_count, sum(value) as total FROM 'data/sales.parquet'",
        ...     example_id=("sale_id", 2),
        ... )
    """
    metadata: dict[str, Any] = {
        "dagster/column_lineage": lineage,
    }

    # Get stats from SQL
    result = conn.sql(stats_sql)
    row = result.fetchone()
    if row:
        for col, val in zip(result.columns, row):
            # Convert numeric types for JSON serialization
            if isinstance(val, (int, float)) and val is not None:
                metadata[col] = float(val) if isinstance(val, float) else val
            elif val is not None:
                metadata[col] = val

    # Get example row
    if example_id:
        id_field, id_value = example_id
        examples = get_example_row(conn, output_path, id_field, id_value)
        metadata["lineage_examples"] = examples

    # Add timing
    if elapsed_ms is not None:
        metadata["processing_time_ms"] = round(elapsed_ms, 2)

    # Merge extra metadata
    if extra_metadata:
        metadata.update(extra_metadata)

    context.add_output_metadata(metadata)


def collect_json_output_metadata(
    context: dg.AssetExecutionContext,
    conn: duckdb.DuckDBPyConnection,
    input_path: str,
    output_path: str,
    lineage: dg.TableColumnLineage,
    example_id: tuple[str, int | str] | None = None,
    example_renames: dict[str, str] | None = None,
    extra_metadata: dict | None = None,
    elapsed_ms: float | None = None,
) -> None:
    """Add standard JSON output asset metadata to context.

    Args:
        context: Dagster asset execution context
        conn: DuckDB connection
        input_path: Path to input parquet file (for example row)
        output_path: Path to output JSON file
        lineage: Column lineage definition
        example_id: (id_field, id_value) for example row lookup
        example_renames: {output_col: input_col} for renamed columns in examples
        extra_metadata: Additional metadata to include
        elapsed_ms: Processing time in milliseconds

    Example:
        >>> collect_json_output_metadata(
        ...     context, conn,
        ...     input_path="data/sales_transform.parquet",
        ...     output_path="data/output/sales.json",
        ...     lineage=SALES_OUTPUT_LINEAGE,
        ...     example_id=("sale_id", 2),
        ...     example_renames={"price_difference": "price_diff"},
        ... )
    """
    metadata: dict[str, Any] = {
        "dagster/column_lineage": lineage,
        "json_output": dg.MetadataValue.path(output_path),
    }

    # Get example row from input and apply renames
    if example_id:
        id_field, id_value = example_id
        examples = get_example_row(conn, input_path, id_field, id_value)
        if example_renames:
            for output_col, input_col in example_renames.items():
                examples[output_col] = examples.pop(input_col, None)
        metadata["lineage_examples"] = examples

    # Add timing
    if elapsed_ms is not None:
        metadata["processing_time_ms"] = round(elapsed_ms, 2)

    # Merge extra metadata
    if extra_metadata:
        metadata.update(extra_metadata)

    context.add_output_metadata(metadata)


# -----------------------------------------------------------------------------
# Example Row Helpers
# -----------------------------------------------------------------------------


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


__all__ = [
    # Lineage DSL
    "build_lineage",
    "passthrough_lineage",
    # View registration
    "HARVEST_VIEWS",
    "register_harvest_views",
    # Metadata collection
    "collect_parquet_metadata",
    "collect_json_output_metadata",
    # Example row helpers
    "format_value",
    "get_example_row",
    "add_lineage_examples_to_dlt_results",
]
