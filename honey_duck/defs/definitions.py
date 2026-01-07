"""Combined Dagster definitions for the honey-duck pipeline.

This module creates the single Definitions object that Dagster uses.
All assets, jobs, and resources are combined here.

Asset Graph (dlt harvest)
-------------------------
    dlt_honey_duck_harvest_sales_raw ────────┐
                                             │
    dlt_honey_duck_harvest_artworks_raw ─────┼──→ sales_enriched ──┬──→ sales_output
                                             │                     │
    dlt_honey_duck_harvest_artists_raw ──────┘                     └──→ artworks_output

Groups
------
- harvest: Raw data loaded from CSV into DuckDB via dlt
- transform: Joined and enriched data
- output: Final JSON outputs
"""

import dagster as dg
from dagster_dlt import DagsterDltResource
from dagster_duckdb_pandas import DuckDBPandasIOManager

from .checks import (
    check_no_null_artists,
    check_sales_above_threshold,
    check_valid_price_tiers,
)
from .dlt_assets import dlt_harvest_assets
from .jobs import full_pipeline_job
from .resources import DUCKDB_PATH
from .assets import artworks_output, sales_enriched, sales_output


defs = dg.Definitions(
    assets=[
        # Harvest assets (dlt-based)
        dlt_harvest_assets,
        # Transform assets
        sales_enriched,
        # Output assets
        sales_output,
        artworks_output,
    ],
    jobs=[full_pipeline_job],
    asset_checks=[
        check_no_null_artists,
        check_sales_above_threshold,
        check_valid_price_tiers,
    ],
    resources={
        "io_manager": DuckDBPandasIOManager(
            database=DUCKDB_PATH,
            schema="main",
        ),
        "dlt": DagsterDltResource(),
    },
)
