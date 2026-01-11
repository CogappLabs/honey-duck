"""Job definitions for the honey-duck pipeline.

Jobs provide named entry points for running asset groups together.
"""

import dagster as dg


# Full pipeline: dlt harvest -> transform -> both outputs (original implementation)
full_pipeline_job = dg.define_asset_job(
    name="full_pipeline",
    selection=dg.AssetSelection.groups("harvest", "transform", "output"),
    description="""
    Complete honey-duck pipeline: dlt Harvest CSV -> Transform -> Both Outputs.

    Uses the original implementation with processor classes.

    1. Harvest (dlt): Load sales, artworks, artists from CSV to DuckDB
    2. Transform: Join and enrich with computed metrics
    3. Output: Generate both sales and artworks views
    """,
)


# Pure Polars implementation pipeline
polars_pipeline_job = dg.define_asset_job(
    name="polars_pipeline",
    selection=dg.AssetSelection.groups("harvest", "transform_polars", "output_polars"),
    description="""
    Pure Polars implementation of honey-duck pipeline.

    Uses inline Polars expressions (no processor classes).

    1. Harvest (dlt): Load raw data to DuckDB (shared)
    2. Transform: Join and enrich using pl.join(), with_columns(), etc.
    3. Output: Filter using pl.filter() and write JSON
    """,
)


# Pure DuckDB SQL implementation pipeline
duckdb_pipeline_job = dg.define_asset_job(
    name="duckdb_pipeline",
    selection=dg.AssetSelection.groups("harvest", "transform_duckdb", "output_duckdb"),
    description="""
    Pure DuckDB SQL implementation of honey-duck pipeline.

    Uses inline SQL queries (no processor classes).

    1. Harvest (dlt): Load raw data to DuckDB (shared)
    2. Transform: Join and enrich using CTEs, JOINs, window functions
    3. Output: Filter using WHERE clause and write JSON
    """,
)


# Polars with filesystem IO manager pipeline
polars_fs_pipeline_job = dg.define_asset_job(
    name="polars_fs_pipeline",
    selection=dg.AssetSelection.groups("harvest", "transform_polars_fs", "output_polars_fs"),
    description="""
    Polars implementation using FilesystemIOManager instead of DuckDB IO manager.

    Demonstrates same processing logic with different storage backend.
    DataFrames are pickled to files instead of stored in DuckDB tables.

    1. Harvest (dlt): Load raw data to DuckDB (shared)
    2. Transform: Join and enrich using Polars lazy expressions
    3. Output: Filter and write JSON (intermediate assets stored as pickle files)
    """,
)
