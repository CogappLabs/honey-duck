"""dlt-based harvest assets with runtime resource configuration.

This module provides the harvest layer using dlt (data load tool) with
Dagster ConfigurableResource injection for runtime path configuration.

IO Pattern - How dlt Bypasses the Dagster IO Manager
----------------------------------------------------
dlt handles its own IO completely - it does NOT use Dagster's IO manager:

    @dg.multi_asset(...)
    def dlt_harvest_assets(context, dlt, paths, database):
        source = create_honey_duck_source(paths, database)
        pipeline = create_harvest_pipeline(paths)
        yield from dlt.run(...)

What happens inside dlt.run():
    1. dlt connects to source (CSV, SQLite, API, etc.)
    2. dlt extracts and transforms data
    3. dlt WRITES directly to Parquet files in {harvest_dir}/raw/
    4. Yields MaterializeResult with metadata only (no data value)

Since no data is returned/yielded, Dagster's IO manager has nothing to store.
dlt acts as its own specialized "IO manager" for the harvest layer.

Benefits of resource injection:
- Runtime path configuration (not import-time)
- Environment-specific overrides
- Better testability
- Consistent with Dagster patterns

Data Sources (all in single dlt source):
- CSV files (sales, artworks, artists) via filesystem source
- SQLite database (media) via sql_database source
"""

from typing import TYPE_CHECKING, Iterator

import dagster as dg
import duckdb
from dagster_dlt import DagsterDltResource, DagsterDltTranslator

if TYPE_CHECKING:
    from dlt.extract.resource import DltResource

from .sources import create_harvest_pipeline, create_honey_duck_source
from ..shared.resources import DatabaseResource, PathsResource


def _get_example_row(
    parquet_path: str,
    id_field: str | None = None,
    id_value: int | str | None = None,
) -> dict:
    """Get one example row from a parquet file for lineage display.

    Args:
        parquet_path: Glob pattern or path to parquet file(s)
        id_field: Optional field to filter by for specific record
        id_value: Optional value to match for specific record
    """
    try:
        if id_field and id_value is not None:
            val = f"'{id_value}'" if isinstance(id_value, str) else id_value
            result = duckdb.sql(f"SELECT * FROM '{parquet_path}' WHERE {id_field} = {val} LIMIT 1")
        else:
            result = duckdb.sql(f"SELECT * FROM '{parquet_path}' LIMIT 1")

        row = result.fetchone()
        if not row:
            return {}

        def fmt(val):
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

        return {col: fmt(val) for col, val in zip(result.columns, row)}
    except Exception:
        return {}


class HoneyDuckDltTranslator(DagsterDltTranslator):
    """Custom translator to map dlt resources to Dagster asset keys."""

    def get_asset_key(self, resource: "DltResource") -> dg.AssetKey:
        """Map dlt resource names to our asset key convention."""
        return dg.AssetKey(f"dlt_harvest_{resource.name}")


# Define the asset specs for each table produced by dlt
HARVEST_ASSET_SPECS = [
    dg.AssetSpec(
        key="dlt_harvest_sales_raw",
        kinds={"dlt", "parquet"},
        group_name="harvest",
        description="Sales transactions harvested from CSV",
    ),
    dg.AssetSpec(
        key="dlt_harvest_artworks_raw",
        kinds={"dlt", "parquet"},
        group_name="harvest",
        description="Artwork catalog harvested from CSV",
    ),
    dg.AssetSpec(
        key="dlt_harvest_artists_raw",
        kinds={"dlt", "parquet"},
        group_name="harvest",
        description="Artist reference data harvested from CSV",
    ),
    dg.AssetSpec(
        key="dlt_harvest_media",
        kinds={"dlt", "parquet"},
        group_name="harvest",
        description="Media references harvested from SQLite",
    ),
]


# Map asset names to their parquet glob patterns and example record IDs
# Using Day Dream (artwork_id=2, sale_id=2, artist_id=1) as the example record
ASSET_CONFIG = {
    "dlt_harvest_sales_raw": {
        "path": "raw/sales_raw/**/*.parquet",
        "id_field": "sale_id",
        "id_value": 2,
    },
    "dlt_harvest_artworks_raw": {
        "path": "raw/artworks_raw/**/*.parquet",
        "id_field": "artwork_id",
        "id_value": 2,
    },
    "dlt_harvest_artists_raw": {
        "path": "raw/artists_raw/**/*.parquet",
        "id_field": "artist_id",
        "id_value": 1,  # Andrew Wyeth
    },
    "dlt_harvest_media": {
        "path": "raw/media/**/*.parquet",
        "id_field": "artwork_id",
        "id_value": 2,
    },
}


@dg.multi_asset(
    specs=HARVEST_ASSET_SPECS,
    can_subset=False,
)
def dlt_harvest_assets(
    context: dg.AssetExecutionContext,
    dlt: DagsterDltResource,
    paths: PathsResource,
    database: DatabaseResource,
) -> Iterator[dg.MaterializeResult]:
    """Harvest all raw data using dlt with runtime-configured paths.

    This asset materializes:
    - dlt_harvest_sales_raw (from CSV)
    - dlt_harvest_artworks_raw (from CSV)
    - dlt_harvest_artists_raw (from CSV)
    - dlt_harvest_media (from SQLite)

    All resources are loaded in a single dlt.run() call,
    avoiding DuckDB lock conflicts.

    Uses write_disposition="replace" to fully reload tables each run,
    preventing duplicate data accumulation.
    """
    # Create source and pipeline with injected paths
    source = create_honey_duck_source(paths, database)
    pipeline = create_harvest_pipeline(paths)

    # Collect dlt results first
    results = list(
        dlt.run(
            context=context,
            dlt_source=source,
            dlt_pipeline=pipeline,
            dagster_dlt_translator=HoneyDuckDltTranslator(),
            write_disposition="replace",
            loader_file_format="parquet",
        )
    )

    # Add lineage examples to each result
    for result in results:
        asset_key = result.asset_key
        if asset_key:
            asset_name = asset_key.to_user_string()
            config = ASSET_CONFIG.get(asset_name)
            if config:
                parquet_path = f"{paths.harvest_dir}/{config['path']}"
                id_field = config.get("id_field")
                id_value = config.get("id_value")
                examples = _get_example_row(
                    parquet_path,
                    str(id_field) if id_field else None,
                    id_value,
                )
                if examples:
                    # Merge lineage_examples into existing metadata
                    existing_metadata = dict(result.metadata) if result.metadata else {}
                    existing_metadata["lineage_examples"] = examples
                    result = dg.MaterializeResult(
                        asset_key=result.asset_key,
                        metadata=existing_metadata,
                    )
        yield result
