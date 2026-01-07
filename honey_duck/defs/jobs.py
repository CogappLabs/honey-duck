"""Job definitions for the honey-duck pipeline.

Jobs provide named entry points for running asset groups together.
"""

import dagster as dg


# Full pipeline: dlt harvest -> transform -> both outputs
full_pipeline_job = dg.define_asset_job(
    name="full_pipeline",
    selection=dg.AssetSelection.all(),
    description="""
    Complete honey-duck pipeline: dlt Harvest CSV -> Transform -> Both Outputs.

    1. Harvest (dlt): Load sales, artworks, artists from CSV to DuckDB
    2. Transform: Join and enrich with computed metrics
    3. Output: Generate both sales and artworks views
    """,
)
