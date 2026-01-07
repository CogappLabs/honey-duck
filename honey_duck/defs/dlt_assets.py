"""dlt-based harvest assets using dagster-dlt integration.

This module provides an alternative harvest layer using dlt.
Benefits over manual CSV loading:
- Schema inference and evolution
- Incremental loading support
- Same pattern for local/cloud files
- Built-in data quality checks
"""

import dagster as dg
from dagster_dlt import DagsterDltResource, dlt_assets

from .dlt_sources import get_harvest_pipeline, honey_duck_source


@dlt_assets(
    dlt_source=honey_duck_source(),
    dlt_pipeline=get_harvest_pipeline(),
    name="dlt_harvest",
    group_name="harvest",
)
def dlt_harvest_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    """Harvest CSV files using dlt filesystem source.

    This asset materializes:
    - raw.sales_raw
    - raw.artworks_raw
    - raw.artists_raw

    into DuckDB using dlt's schema inference and loading.
    """
    yield from dlt.run(context=context)
