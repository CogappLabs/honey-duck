"""dlt-based harvest assets with runtime resource configuration.

dlt handles its own IO - it writes directly to Parquet files in {harvest_dir}/raw/.
Since no data is returned, Dagster's IO manager has nothing to store.

Data Sources:
- CSV files (sales, artworks, artists) via filesystem source
- SQLite database (media) via sql_database source
"""

from typing import TYPE_CHECKING, Iterator

import dagster as dg
from dagster_dlt import DagsterDltResource, DagsterDltTranslator

from cogapp_libs.dagster.lineage import add_lineage_examples_to_dlt_results

if TYPE_CHECKING:
    from dlt.extract.resource import DltResource
    from dagster_dlt import DltResourceTranslatorData

from ..shared.resources import DatabaseResource, PathsResource
from .sources import create_harvest_pipeline, create_honey_duck_source


class HoneyDuckDltTranslator(DagsterDltTranslator):
    """Map dlt resources to Dagster asset keys."""

    def get_asset_key(self, resource: "DltResource | DltResourceTranslatorData") -> dg.AssetKey:
        resource_name = getattr(resource, "name", None)
        if resource_name is None and hasattr(resource, "resource"):
            resource_name = getattr(resource.resource, "name", None)
        return dg.AssetKey(f"dlt_harvest_{resource_name}")

    def get_asset_spec(self, data: "DltResourceTranslatorData") -> dg.AssetSpec:
        key = self.get_asset_key(data)
        return dg.AssetSpec(key=key, kinds={"dlt", "parquet"}, group_name="harvest")


HARVEST_ASSET_SPECS = [
    dg.AssetSpec(key="dlt_harvest_sales_raw", kinds={"dlt", "parquet"}, group_name="harvest"),
    dg.AssetSpec(key="dlt_harvest_artworks_raw", kinds={"dlt", "parquet"}, group_name="harvest"),
    dg.AssetSpec(key="dlt_harvest_artists_raw", kinds={"dlt", "parquet"}, group_name="harvest"),
    dg.AssetSpec(key="dlt_harvest_media", kinds={"dlt", "parquet"}, group_name="harvest"),
]

# Example record config: Day Dream (artwork_id=2, sale_id=2, artist_id=1)
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
        "id_value": 1,
    },
    "dlt_harvest_media": {
        "path": "raw/media/**/*.parquet",
        "id_field": "artwork_id",
        "id_value": 2,
    },
}


@dg.multi_asset(specs=HARVEST_ASSET_SPECS, can_subset=False)
def dlt_harvest_assets(
    context: dg.AssetExecutionContext,
    dlt: DagsterDltResource,
    paths: PathsResource,
    database: DatabaseResource,
) -> Iterator[dg.MaterializeResult]:
    """Harvest raw data from CSV and SQLite sources."""
    source = create_honey_duck_source(paths, database)
    pipeline = create_harvest_pipeline(paths)

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

    yield from add_lineage_examples_to_dlt_results(results, paths.harvest_dir, ASSET_CONFIG)
