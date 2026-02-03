"""Job definitions for the honey-duck pipeline.

Jobs provide named entry points for running asset groups together.
"""

import dagster as dg


# Processors pipeline: original implementation with processor classes
processors_pipeline_job = dg.define_asset_job(
    name="processors_pipeline",
    selection=dg.AssetSelection.groups("harvest", "transform", "output"),
    description="""
    Original implementation using reusable processor classes.

    Demonstrates the processor pattern from cogapp_libs:
    - PolarsFilterProcessor, PolarsStringProcessor for transformations
    - DuckDBJoinProcessor, DuckDBWindowProcessor for SQL operations

    1. Harvest (dlt): Load sales, artworks, artists from CSV
    2. Transform: Join and enrich using processor classes
    3. Output: Generate both sales and artworks JSON files
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


# Polars with ops (graph-backed assets) pipeline
polars_ops_pipeline_job = dg.define_asset_job(
    name="polars_ops_pipeline",
    selection=dg.AssetSelection.groups("harvest", "transform_polars_ops", "output_polars_ops"),
    description="""
    Graph-backed asset implementation using ops for detailed observability.

    Demonstrates ops pattern for proof-of-concept / larger pipelines.
    Each transformation step is an op with detailed logging and metadata.
    Results in single asset in lineage graph with op-level observability.

    Benefits:
    - Detailed op-level logs and metadata for debugging
    - Single asset in lineage graph (cleaner asset graph)
    - No intermediate persistence (data flows through memory)

    1. Harvest (dlt): Load raw data to DuckDB (shared)
    2. Transform: Graph-backed assets with ops for join, aggregate, enrich
    3. Output: Filter and write JSON
    """,
)


# Polars multi-asset pipeline
polars_multi_pipeline_job = dg.define_asset_job(
    name="polars_multi_pipeline",
    selection=dg.AssetSelection.groups("harvest", "transform_polars_multi", "output_polars_multi"),
    description="""
    Multi-asset implementation for tightly coupled transformation steps.

    Demonstrates the multi-asset pattern where one function produces multiple
    related assets. Each intermediate step is yielded separately but computed
    together in a single function.

    Benefits:
    - Tightly coupled steps run together (can't selectively materialize)
    - Each step persisted independently via IO manager
    - Individual asset tracking in lineage graph
    - Single function for related logic

    Use when:
    - Intermediate steps are tightly coupled and should run together
    - You want each step persisted independently (via IO manager)
    - You need individual asset tracking in the lineage graph
    - Steps must complete together (can't selectively materialize)

    1. Harvest (dlt): Load raw data to DuckDB (shared)
    2. Transform: Multi-asset functions that yield intermediate steps
    3. Output: Filter and write JSON
    """,
)


# DuckDB + Soda implementation pipeline
duckdb_soda_pipeline_job = dg.define_asset_job(
    name="duckdb_soda_pipeline",
    selection=dg.AssetSelection.groups("harvest", "transform_soda", "output_soda"),
    description="""
    DuckDB + Soda implementation of honey-duck pipeline.

    Uses DuckDB for all transformations and Soda Core for data validation.
    Validation runs as SQL queries - no data loaded into Python memory.

    Key Features:
    - Pure SQL transformations via DuckDB
    - Soda contract YAML files define data quality expectations
    - Memory-efficient: DuckDB handles spill-to-disk
    - Blocking checks prevent bad data from flowing downstream

    1. Harvest (dlt): Load raw data to Parquet (shared)
    2. Transform: Join and enrich using DuckDB SQL
    3. Validate: Soda checks run SQL against DuckDB
    4. Output: Filter and write JSON
    """,
)
