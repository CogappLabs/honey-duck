"""Resource registration for the honey-duck pipeline.

This module configures the DuckDBPandasIOManager which handles:
- Storing DataFrames as DuckDB tables between assets
- Loading upstream asset data automatically
- Persisting final outputs

The DuckDB database file is stored in data/output/ alongside other outputs.

Processor DuckDB:
- By default, inline DuckDB SQL in assets uses in-memory mode
- Set HONEY_DUCK_PROCESSOR_DB_PATH to use a file-based DB for debugging
- This allows inspecting intermediate query results
"""

import os
from pathlib import Path

import dagster as dg
import duckdb

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

# Processor database path - for inline DuckDB SQL in transforms
# Set to a file path to persist intermediate results, or leave unset for in-memory
PROCESSOR_DB_PATH = os.environ.get("HONEY_DUCK_PROCESSOR_DB_PATH", ":memory:")

# Output file paths (configurable via environment variables)
SALES_OUTPUT_PATH = Path(
    os.environ.get("HONEY_DUCK_SALES_OUTPUT", str(OUTPUT_DIR / "sales_output.json"))
)
ARTWORKS_OUTPUT_PATH = Path(
    os.environ.get("HONEY_DUCK_ARTWORKS_OUTPUT", str(OUTPUT_DIR / "artworks_output.json"))
)


def get_processor_connection() -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection for processor operations.

    Returns a connection to either:
    - In-memory database (default, fast but transient)
    - File-based database (if HONEY_DUCK_PROCESSOR_DB_PATH is set)

    Using a file-based database allows debugging intermediate results.
    """
    return duckdb.connect(PROCESSOR_DB_PATH)


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
