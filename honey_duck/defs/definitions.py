"""Combined Dagster definitions for the honey-duck pipeline.

This module creates the single Definitions object that Dagster uses.
All assets, jobs, and resources are combined here.

Asset Graph (Original + 6 Implementation Variants)
--------------------------------------------------
Each implementation follows this pattern:

    dlt_harvest_* (shared) --> sales_transform_<impl> --> sales_output_<impl>
                           └--> artworks_transform_<impl> --> artworks_output_<impl>

Groups
------
- harvest: Raw data loaded from CSV/SQLite into DuckDB via dlt (shared)
- transform/output: Original implementation with processor classes
- transform_polars/output_polars: Pure Polars expressions (split into steps)
- transform_duckdb/output_duckdb: Pure DuckDB SQL queries
- transform_polars_fs/output_polars_fs: Polars with FilesystemIOManager
- transform_polars_ops/output_polars_ops: Graph-backed assets with ops (detailed observability)
- transform_polars_multi/output_polars_multi: Multi-asset implementation (tightly coupled steps)

IO Managers
-----------
- io_manager (default): PolarsParquetIOManager - stores DataFrames as Parquet files
- fs_io_manager: FilesystemIOManager - pickles DataFrames to files

DuckDB Usage
------------
- DuckDB is still used for SQL transformations via processors
- DuckDB resource provides connection to raw schema (dlt harvest data)
- Inter-asset communication now uses Parquet instead of DuckDB tables

Checks
------
- Blocking Pandera checks on transforms prevent bad data from flowing downstream
- Freshness checks alert if outputs haven't been updated in 24 hours
"""

import dagster as dg
from dagster import FilesystemIOManager
from dagster_dlt import DagsterDltResource
from dagster_duckdb import DuckDBResource
from dagster_polars import PolarsParquetIOManager

# Original implementation
from .assets import artworks_output, artworks_transform, sales_output, sales_transform

# Pure Polars implementation
from .assets_polars import (
    artworks_catalog_polars,
    artworks_media_polars,
    artworks_output_polars,
    artworks_sales_agg_polars,
    artworks_transform_polars,
    sales_joined_polars,
    sales_output_polars,
    sales_transform_polars,
)

# Pure DuckDB SQL implementation
from .assets_duckdb import (
    artworks_output_duckdb,
    artworks_transform_duckdb,
    sales_output_duckdb,
    sales_transform_duckdb,
)

# Polars with filesystem IO manager
from .assets_polars_fs import (
    artworks_output_polars_fs,
    artworks_transform_polars_fs,
    sales_output_polars_fs,
    sales_transform_polars_fs,
)

# Polars with ops (graph-backed assets)
from .assets_polars_ops import (
    artworks_output_polars_ops,
    artworks_transform_polars_ops,
    sales_output_polars_ops,
    sales_transform_polars_ops,
)

# Polars multi-asset implementation
from .assets_polars_multi import (
    artworks_output_polars_multi,
    artworks_pipeline_multi,
    sales_output_polars_multi,
    sales_pipeline_multi,
)

from .checks import (
    check_artworks_transform_schema,
    check_sales_above_threshold,
    check_sales_transform_schema,
    check_valid_price_tiers,
)
from .dlt_assets import dlt_harvest_assets
from .jobs import (
    duckdb_pipeline_job,
    full_pipeline_job,
    polars_fs_pipeline_job,
    polars_multi_pipeline_job,
    polars_ops_pipeline_job,
    polars_pipeline_job,
)
from .resources import DUCKDB_PATH, PARQUET_DIR


defs = dg.Definitions(
    assets=[
        # Harvest assets (dlt-based: CSV + SQLite in single source) - shared
        dlt_harvest_assets,
        # Original implementation (with processor classes)
        sales_transform,
        artworks_transform,
        sales_output,
        artworks_output,
        # Pure Polars implementation (split into steps for intermediate persistence)
        sales_joined_polars,
        sales_transform_polars,
        sales_output_polars,
        artworks_catalog_polars,
        artworks_sales_agg_polars,
        artworks_media_polars,
        artworks_transform_polars,
        artworks_output_polars,
        # Pure DuckDB SQL implementation
        sales_transform_duckdb,
        artworks_transform_duckdb,
        sales_output_duckdb,
        artworks_output_duckdb,
        # Polars with filesystem IO manager
        sales_transform_polars_fs,
        artworks_transform_polars_fs,
        sales_output_polars_fs,
        artworks_output_polars_fs,
        # Polars with ops (graph-backed assets for detailed observability)
        sales_transform_polars_ops,
        artworks_transform_polars_ops,
        sales_output_polars_ops,
        artworks_output_polars_ops,
        # Polars multi-asset implementation (tightly coupled steps)
        sales_pipeline_multi,
        artworks_pipeline_multi,
        sales_output_polars_multi,
        artworks_output_polars_multi,
    ],
    jobs=[
        full_pipeline_job,
        polars_pipeline_job,
        duckdb_pipeline_job,
        polars_fs_pipeline_job,
        polars_ops_pipeline_job,
        polars_multi_pipeline_job,
    ],
    asset_checks=[
        # Blocking Pandera schema checks (prevent downstream on failure)
        check_sales_transform_schema,
        check_artworks_transform_schema,
        # Output quality checks
        check_sales_above_threshold,
        check_valid_price_tiers,
    ],
    resources={
        "io_manager": PolarsParquetIOManager(
            base_dir=str(PARQUET_DIR),
        ),
        "fs_io_manager": FilesystemIOManager(),
        "dlt": DagsterDltResource(),
        "duckdb": DuckDBResource(
            database=DUCKDB_PATH,
        ),
    },
)
