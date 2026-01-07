"""dlt-based harvest assets using dagster-dlt integration.

This module provides the harvest layer using dlt (data load tool).

IO Pattern - How dlt Bypasses the Dagster IO Manager
----------------------------------------------------
dlt handles its own IO completely - it does NOT use Dagster's IO manager:

    @dlt_assets(...)
    def harvest(context, dlt: DagsterDltResource):
        yield from dlt.run(context=context)

What happens inside dlt.run():
    1. dlt connects to source (CSV, SQLite, API, etc.)
    2. dlt extracts and transforms data
    3. dlt WRITES directly to DuckDB (raw.* tables)  <-- IO happens here
    4. Yields MaterializeResult with metadata only (no data value)

Since no data is returned/yielded, Dagster's IO manager has nothing to store.
dlt acts as its own specialized "IO manager" for the harvest layer.

Comparison with regular assets:
    - Regular asset: return DataFrame -> IO manager stores in DuckDB
    - dlt asset: dlt writes to DuckDB -> yield MaterializeResult (metadata only)

Benefits over manual CSV loading:
- Schema inference and evolution
- Incremental loading support
- Same pattern for local/cloud/database files
- Built-in data quality checks

Data Sources (all in single dlt source to avoid DuckDB lock conflicts):
- CSV files (sales, artworks, artists) via filesystem source
- SQLite database (media) via sql_database source
"""

import dagster as dg
from dagster_dlt import DagsterDltResource, dlt_assets

from .dlt_sources import get_harvest_pipeline, honey_duck_source


@dlt_assets(
    dlt_source=honey_duck_source(),
    dlt_pipeline=get_harvest_pipeline(),
    name="",
    group_name="harvest",
)
def dlt_harvest_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    """Harvest all raw data using dlt.

    This asset materializes:
    - raw.sales_raw (from CSV)
    - raw.artworks_raw (from CSV)
    - raw.artists_raw (from CSV)
    - raw.media (from SQLite)

    All resources are loaded in a single dlt.run() call,
    avoiding DuckDB lock conflicts.

    Uses write_disposition="replace" to fully reload tables each run,
    preventing duplicate data accumulation.
    """
    yield from dlt.run(context=context, write_disposition="replace")
