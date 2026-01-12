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

import dagster as dg
from dagster_dlt import DagsterDltResource, DagsterDltTranslator

from .dlt_sources import create_harvest_pipeline, create_honey_duck_source
from .resources import DatabaseResource, PathsResource


class HoneyDuckDltTranslator(DagsterDltTranslator):
    """Custom translator to map dlt resources to Dagster asset keys."""

    def get_asset_key(self, resource) -> dg.AssetKey:
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


@dg.multi_asset(
    specs=HARVEST_ASSET_SPECS,
    can_subset=False,
)
def dlt_harvest_assets(
    context: dg.AssetExecutionContext,
    dlt: DagsterDltResource,
    paths: PathsResource,
    database: DatabaseResource,
):
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

    # Run dlt and yield results
    yield from dlt.run(
        context=context,
        dlt_source=source,
        dlt_pipeline=pipeline,
        dagster_dlt_translator=HoneyDuckDltTranslator(),
        write_disposition="replace",
        loader_file_format="parquet",
    )
