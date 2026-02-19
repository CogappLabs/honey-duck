"""Combined Dagster definitions for the honey-duck pipeline.

This module creates the single Definitions object that Dagster uses.
All assets, jobs, and resources are combined here.

Asset Graph (Original + 5 Implementation Variants)
--------------------------------------------------
Each implementation follows this pattern:

    dlt_harvest_* (shared) --> sales_transform_<impl> --> sales_output_<impl>
                           └--> artworks_transform_<impl> --> artworks_output_<impl>

Groups
------
- harvest: Raw data loaded from CSV/SQLite via dlt (shared)
- transform/output: Original implementation with processor classes
- transform_polars/output_polars: Pure Polars expressions (split into steps)
- transform_duckdb/output_duckdb: Pure DuckDB SQL queries
- transform_soda/output_soda: DuckDB SQL + Soda validation (SQL-based checks)
- transform_polars_fs/output_polars_fs: Polars with FilesystemIOManager
- transform_polars_ops/output_polars_ops: Graph-backed assets with ops (detailed observability)
- transform_polars_multi/output_polars_multi: Multi-asset implementation (tightly coupled steps)

IO Managers
-----------
- io_manager (default): PolarsParquetIOManager - stores DataFrames as Parquet files

Storage Pattern (all Parquet)
-----------------------------
- Harvest layer: dlt writes Parquet to data/harvest/raw/
- Transform/Output layers: Parquet files in data/storage/

Data Flow
---------
dlt handles its own IO (bypasses Dagster IO manager). Downstream assets
read dlt-produced Parquet files directly via DuckDB's read_parquet().

Checks
------
- Blocking Pandera checks on transforms prevent bad data from flowing downstream
- Freshness checks alert if outputs haven't been updated in 24 hours
"""

import os

import dagster as dg

# Enable remote debugging when DAGSTER_DEBUG=1
# Start: DAGSTER_DEBUG=1 uv run dg dev
# Then attach VS Code debugger ("Dagster: Attach to dg dev")
if os.environ.get("DAGSTER_DEBUG"):
    os.environ["PYDEVD_DISABLE_FILE_VALIDATION"] = "1"
    import debugpy

    try:
        debugpy.listen(("localhost", 5678))
        print("debugpy listening on port 5678 — attach your IDE debugger")
    except RuntimeError:
        # Subprocess: pause briefly so debugpy can sync breakpoints from VS Code
        debugpy.trace_this_thread(True)
        import time

        time.sleep(0.5)
from dagster_dlt import DagsterDltResource
from dagster_duckdb import DuckDBResource
from dagster_polars import PolarsParquetIOManager

from cogapp_libs.dagster.io_managers import ParquetPathIOManager

# Original implementation
from .original.assets import artworks_output, artworks_transform, sales_output, sales_transform

# Pure Polars implementation (split steps for intermediate persistence)
from .polars.assets import (
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
from .duckdb.assets import (
    artworks_output_duckdb,
    artworks_transform_duckdb,
    sales_output_duckdb,
    sales_transform_duckdb,
)

# Polars with filesystem IO manager
from .polars.assets_fs import (
    artworks_output_polars_fs,
    artworks_transform_polars_fs,
    sales_output_polars_fs,
    sales_transform_polars_fs,
)

# Polars with ops (graph-backed assets)
from .polars.assets_ops import (
    artworks_output_polars_ops,
    artworks_transform_polars_ops,
    sales_output_polars_ops,
    sales_transform_polars_ops,
)

# Polars multi-asset implementation
from .polars.assets_multi import (
    artworks_output_polars_multi,
    artworks_pipeline_multi,
    sales_output_polars_multi,
    sales_pipeline_multi,
)

# DuckDB + Soda implementation
from .duckdb_soda import (
    artworks_output_soda,
    artworks_transform_soda,
    check_artworks_transform_soda,
    check_sales_transform_soda,
    sales_output_soda,
    sales_transform_soda,
)

from .shared.checks import (
    check_artworks_transform_schema,
    check_sales_above_threshold,
    check_sales_transform_schema,
    check_valid_price_tiers,
)
from .harvest.assets import dlt_harvest_assets
from .shared.jobs import (
    duckdb_pipeline_job,
    duckdb_soda_pipeline_job,
    processors_pipeline_job,
    polars_fs_pipeline_job,
    polars_multi_pipeline_job,
    polars_ops_pipeline_job,
    polars_pipeline_job,
)
from .shared.resources import (
    DatabaseResource,
    OUTPUT_DIR,
    OutputPathsResource,
    PathsResource,
    STORAGE_DIR,
)

# Path constant for DuckDB database - shared across resources
DUCKDB_PATH = str(OUTPUT_DIR / "dagster.duckdb")


# Note: For new projects, consider using dg.load_from_defs_folder() for auto-discovery.
# This project uses explicit definitions for backwards compatibility with the dg CLI.
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
        # DuckDB + Soda implementation
        sales_transform_soda,
        artworks_transform_soda,
        sales_output_soda,
        artworks_output_soda,
    ],
    jobs=[
        processors_pipeline_job,
        polars_pipeline_job,
        duckdb_pipeline_job,
        duckdb_soda_pipeline_job,
        polars_fs_pipeline_job,
        polars_ops_pipeline_job,
        polars_multi_pipeline_job,
    ],
    asset_checks=[
        # Blocking Pandera schema checks (prevent downstream on failure)
        check_sales_transform_schema,
        check_artworks_transform_schema,
        # Blocking Soda schema checks (for DuckDB + Soda pipeline)
        check_sales_transform_soda,
        check_artworks_transform_soda,
        # Output quality checks
        check_sales_above_threshold,
        check_valid_price_tiers,
    ],
    resources={
        # ConfigurableResource instances for path configuration
        "paths": PathsResource(),
        "output_paths": OutputPathsResource(),
        "database": DatabaseResource(),
        # IO managers
        "io_manager": PolarsParquetIOManager(
            base_dir=str(STORAGE_DIR),
        ),
        "parquet_path_io_manager": ParquetPathIOManager(
            base_dir=str(STORAGE_DIR / "parquet_paths"),
        ),
        # External resources
        "dlt": DagsterDltResource(),
        "duckdb": DuckDBResource(
            database=DUCKDB_PATH,
            connection_config={
                "memory_limit": "4GB",
                "threads": 4,
            },
        ),
    },
)
