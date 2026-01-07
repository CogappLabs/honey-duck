"""Combined Dagster definitions for the honey-duck pipeline.

This module creates the single Definitions object that Dagster uses.
All assets, jobs, and resources are combined here.

Asset Graph
-----------
    dlt_harvest_sales_raw ────────┐
    dlt_harvest_artworks_raw ─────┼──→ sales_transform ──[blocking]──→ sales_output
    dlt_harvest_artists_raw ──────┤
    dlt_harvest_media ────────────┼──→ artworks_transform ──[blocking]──→ artworks_output

Groups
------
- harvest: Raw data loaded from CSV/SQLite into DuckDB via dlt
- transform: Joined and enriched data (with Pandera schema validation)
- output: Final JSON outputs (with freshness checks)

Checks
------
- Blocking Pandera checks on transforms prevent bad data from flowing downstream
- Freshness checks alert if outputs haven't been updated in 24 hours
"""

import dagster as dg
from dagster_dlt import DagsterDltResource

from cogapp_deps.dagster import DuckDBPandasPolarsIOManager

from .assets import artworks_output, artworks_transform, sales_output, sales_transform
from .checks import (
    check_artworks_transform_schema,
    check_sales_above_threshold,
    check_sales_transform_schema,
    check_valid_price_tiers,
)
from .dlt_assets import dlt_harvest_assets
from .jobs import full_pipeline_job
from .resources import DUCKDB_PATH


defs = dg.Definitions(
    assets=[
        # Harvest assets (dlt-based: CSV + SQLite in single source)
        dlt_harvest_assets,
        # Transform assets
        sales_transform,
        artworks_transform,
        # Output assets
        sales_output,
        artworks_output,
    ],
    jobs=[full_pipeline_job],
    asset_checks=[
        # Blocking Pandera schema checks (prevent downstream on failure)
        check_sales_transform_schema,
        check_artworks_transform_schema,
        # Output quality checks
        check_sales_above_threshold,
        check_valid_price_tiers,
    ],
    resources={
        "io_manager": DuckDBPandasPolarsIOManager(
            database=DUCKDB_PATH,
            schema="main",
        ),
        "dlt": DagsterDltResource(),
    },
)
