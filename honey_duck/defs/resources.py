"""Resource registration for the honey-duck pipeline.

This module configures storage paths for the pipeline:
- PolarsParquetIOManager stores DataFrames as Parquet files between assets
- DuckDB is used for SQL transformations and dlt harvest storage
- Final outputs are written as JSON files

Directory structure under data/output/:
- json/     - Asset JSON outputs
- storage/  - IO manager Parquet files (inter-asset communication)
- dlt/      - DuckDB database for dlt harvest

All paths can be overridden via environment variables.
"""

import os
from pathlib import Path

import dagster as dg

# Resolve paths relative to project root
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

# Output subdirectories for organized storage
JSON_OUTPUT_DIR = OUTPUT_DIR / "json"
STORAGE_DIR = OUTPUT_DIR / "storage"
DLT_DIR = OUTPUT_DIR / "dlt"

# Parquet IO Manager storage directory (can be overridden via environment variable)
PARQUET_DIR = Path(
    os.environ.get(
        "HONEY_DUCK_PARQUET_DIR",
        str(STORAGE_DIR),
    )
)

# DuckDB database path for dlt harvest and SQL transformations (can be overridden via environment variable)
DUCKDB_PATH = os.environ.get(
    "HONEY_DUCK_DB_PATH",
    str(DLT_DIR / "dagster.duckdb"),
)

# Parquet harvest directory for dlt output
HARVEST_PARQUET_DIR = Path(
    os.environ.get(
        "HONEY_DUCK_HARVEST_PARQUET_DIR",
        str(DLT_DIR / "harvest_parquet"),
    )
)

# SQLite media database path
MEDIA_DB_PATH = INPUT_DIR / "media.db"

# JSON output file paths (configurable via environment variables)
SALES_OUTPUT_PATH = Path(
    os.environ.get("HONEY_DUCK_SALES_OUTPUT", str(JSON_OUTPUT_DIR / "sales_output.json"))
)
ARTWORKS_OUTPUT_PATH = Path(
    os.environ.get("HONEY_DUCK_ARTWORKS_OUTPUT", str(JSON_OUTPUT_DIR / "artworks_output.json"))
)

# Polars implementation output paths
SALES_OUTPUT_PATH_POLARS = Path(
    os.environ.get("HONEY_DUCK_SALES_OUTPUT_POLARS", str(JSON_OUTPUT_DIR / "sales_output_polars.json"))
)
ARTWORKS_OUTPUT_PATH_POLARS = Path(
    os.environ.get("HONEY_DUCK_ARTWORKS_OUTPUT_POLARS", str(JSON_OUTPUT_DIR / "artworks_output_polars.json"))
)

# DuckDB SQL implementation output paths
SALES_OUTPUT_PATH_DUCKDB = Path(
    os.environ.get("HONEY_DUCK_SALES_OUTPUT_DUCKDB", str(JSON_OUTPUT_DIR / "sales_output_duckdb.json"))
)
ARTWORKS_OUTPUT_PATH_DUCKDB = Path(
    os.environ.get("HONEY_DUCK_ARTWORKS_OUTPUT_DUCKDB", str(JSON_OUTPUT_DIR / "artworks_output_duckdb.json"))
)

# Polars filesystem IO manager implementation output paths
SALES_OUTPUT_PATH_POLARS_FS = Path(
    os.environ.get("HONEY_DUCK_SALES_OUTPUT_POLARS_FS", str(JSON_OUTPUT_DIR / "sales_output_polars_fs.json"))
)
ARTWORKS_OUTPUT_PATH_POLARS_FS = Path(
    os.environ.get("HONEY_DUCK_ARTWORKS_OUTPUT_POLARS_FS", str(JSON_OUTPUT_DIR / "artworks_output_polars_fs.json"))
)

# Polars ops (graph-backed assets) implementation output paths
SALES_OUTPUT_PATH_POLARS_OPS = Path(
    os.environ.get("HONEY_DUCK_SALES_OUTPUT_POLARS_OPS", str(JSON_OUTPUT_DIR / "sales_output_polars_ops.json"))
)
ARTWORKS_OUTPUT_PATH_POLARS_OPS = Path(
    os.environ.get("HONEY_DUCK_ARTWORKS_OUTPUT_POLARS_OPS", str(JSON_OUTPUT_DIR / "artworks_output_polars_ops.json"))
)

# Polars multi-asset implementation output paths
SALES_OUTPUT_PATH_POLARS_MULTI = Path(
    os.environ.get("HONEY_DUCK_SALES_OUTPUT_POLARS_MULTI", str(JSON_OUTPUT_DIR / "sales_output_polars_multi.json"))
)
ARTWORKS_OUTPUT_PATH_POLARS_MULTI = Path(
    os.environ.get("HONEY_DUCK_ARTWORKS_OUTPUT_POLARS_MULTI", str(JSON_OUTPUT_DIR / "artworks_output_polars_multi.json"))
)


# -----------------------------------------------------------------------------
# External Source Assets (CSV files)
# -----------------------------------------------------------------------------
# These AssetSpecs represent external data files that Dagster doesn't control.
# They appear in the asset graph as upstream dependencies of harvest assets.

csv_sales = dg.AssetSpec(
    key="csv_sales",
    description="Sales records CSV file",
    metadata={"path": str(INPUT_DIR / "sales.csv")},
    kinds={"csv"},
    group_name="source",
)

csv_artworks = dg.AssetSpec(
    key="csv_artworks",
    description="Artworks catalog CSV file",
    metadata={"path": str(INPUT_DIR / "artworks.csv")},
    kinds={"csv"},
    group_name="source",
)

csv_artists = dg.AssetSpec(
    key="csv_artists",
    description="Artists reference CSV file",
    metadata={"path": str(INPUT_DIR / "artists.csv")},
    kinds={"csv"},
    group_name="source",
)

sqlite_media = dg.AssetSpec(
    key="sqlite_media",
    description="Media references SQLite database",
    metadata={"path": str(MEDIA_DB_PATH)},
    kinds={"sqlite"},
    group_name="source",
)


# Note: Resources are registered in __init__.py to avoid duplication
# when using load_from_defs_folder
