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

# Output file paths (configurable via environment variables)
SALES_OUTPUT_PATH = Path(
    os.environ.get("HONEY_DUCK_SALES_OUTPUT", str(OUTPUT_DIR / "sales_output.json"))
)
ARTWORKS_OUTPUT_PATH = Path(
    os.environ.get("HONEY_DUCK_ARTWORKS_OUTPUT", str(OUTPUT_DIR / "artworks_output.json"))
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


# Note: Resources are registered in __init__.py to avoid duplication
# when using load_from_defs_folder
