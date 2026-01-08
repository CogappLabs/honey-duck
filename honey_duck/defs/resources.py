"""Resource registration for the honey-duck pipeline.

This module configures the DuckDBPandasPolarsIOManager which handles:
- Storing DataFrames as DuckDB tables between assets
- Loading upstream asset data automatically
- Persisting final outputs

The DuckDB database file is stored in data/output/ alongside other outputs.
"""

import os
from pathlib import Path

import dagster as dg

# Resolve paths relative to project root
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

# IO Manager database path (can be overridden via environment variable)
DUCKDB_PATH = os.environ.get(
    "HONEY_DUCK_DB_PATH",
    str(OUTPUT_DIR / "dagster.duckdb"),
)

# SQLite media database path
MEDIA_DB_PATH = INPUT_DIR / "media.db"

# Output file paths (configurable via environment variables)
SALES_OUTPUT_PATH = Path(
    os.environ.get("HONEY_DUCK_SALES_OUTPUT", str(OUTPUT_DIR / "sales_output.json"))
)
ARTWORKS_OUTPUT_PATH = Path(
    os.environ.get("HONEY_DUCK_ARTWORKS_OUTPUT", str(OUTPUT_DIR / "artworks_output.json"))
)

# Polars implementation output paths
SALES_OUTPUT_PATH_POLARS = Path(
    os.environ.get("HONEY_DUCK_SALES_OUTPUT_POLARS", str(OUTPUT_DIR / "sales_output_polars.json"))
)
ARTWORKS_OUTPUT_PATH_POLARS = Path(
    os.environ.get("HONEY_DUCK_ARTWORKS_OUTPUT_POLARS", str(OUTPUT_DIR / "artworks_output_polars.json"))
)

# Pandas implementation output paths
SALES_OUTPUT_PATH_PANDAS = Path(
    os.environ.get("HONEY_DUCK_SALES_OUTPUT_PANDAS", str(OUTPUT_DIR / "sales_output_pandas.json"))
)
ARTWORKS_OUTPUT_PATH_PANDAS = Path(
    os.environ.get("HONEY_DUCK_ARTWORKS_OUTPUT_PANDAS", str(OUTPUT_DIR / "artworks_output_pandas.json"))
)

# DuckDB SQL implementation output paths
SALES_OUTPUT_PATH_DUCKDB = Path(
    os.environ.get("HONEY_DUCK_SALES_OUTPUT_DUCKDB", str(OUTPUT_DIR / "sales_output_duckdb.json"))
)
ARTWORKS_OUTPUT_PATH_DUCKDB = Path(
    os.environ.get("HONEY_DUCK_ARTWORKS_OUTPUT_DUCKDB", str(OUTPUT_DIR / "artworks_output_duckdb.json"))
)

# Polars filesystem IO manager implementation output paths
SALES_OUTPUT_PATH_POLARS_FS = Path(
    os.environ.get("HONEY_DUCK_SALES_OUTPUT_POLARS_FS", str(OUTPUT_DIR / "sales_output_polars_fs.json"))
)
ARTWORKS_OUTPUT_PATH_POLARS_FS = Path(
    os.environ.get("HONEY_DUCK_ARTWORKS_OUTPUT_POLARS_FS", str(OUTPUT_DIR / "artworks_output_polars_fs.json"))
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
