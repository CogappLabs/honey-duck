"""Resource configuration for the honey-duck pipeline.

This module defines ConfigurableResource classes for runtime configuration:
- PathsResource: Core directory paths (input, output, storage, harvest)
- OutputPathsResource: JSON output file paths for each implementation
- DatabaseResource: Database file paths (DuckDB, SQLite)

These resources are injected into assets at runtime, enabling:
- Type-safe configuration
- Environment-specific overrides
- Launch-time configuration
- Better testability
"""

from pathlib import Path

import dagster as dg

# -----------------------------------------------------------------------------
# Base Path Constants (used as defaults for ConfigurableResource)
# -----------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
STORAGE_DIR = DATA_DIR / "storage"  # IO manager intermediate storage
HARVEST_DIR = DATA_DIR / "harvest"  # dlt raw Parquet data


# -----------------------------------------------------------------------------
# ConfigurableResource Classes
# -----------------------------------------------------------------------------


class PathsResource(dg.ConfigurableResource):
    """Core directory paths for the pipeline.

    All paths can be overridden at launch time or via environment variables.
    """

    input_dir: str = str(INPUT_DIR)
    output_dir: str = str(OUTPUT_DIR)
    storage_dir: str = str(STORAGE_DIR)
    harvest_dir: str = str(HARVEST_DIR)


class OutputPathsResource(dg.ConfigurableResource):
    """JSON output file paths for each implementation variant.

    Each implementation (original, polars, duckdb, polars_fs, polars_ops, polars_multi)
    has its own output paths to avoid conflicts when running pipelines in parallel.
    """

    # Original implementation
    sales: str = str(OUTPUT_DIR / "sales_output.json")
    artworks: str = str(OUTPUT_DIR / "artworks_output.json")

    # Polars implementation (split steps)
    sales_polars: str = str(OUTPUT_DIR / "sales_output_polars.json")
    artworks_polars: str = str(OUTPUT_DIR / "artworks_output_polars.json")

    # DuckDB SQL implementation
    sales_duckdb: str = str(OUTPUT_DIR / "sales_output_duckdb.json")
    artworks_duckdb: str = str(OUTPUT_DIR / "artworks_output_duckdb.json")

    # Polars filesystem IO manager implementation
    sales_polars_fs: str = str(OUTPUT_DIR / "sales_output_polars_fs.json")
    artworks_polars_fs: str = str(OUTPUT_DIR / "artworks_output_polars_fs.json")

    # Polars ops (graph-backed assets) implementation
    sales_polars_ops: str = str(OUTPUT_DIR / "sales_output_polars_ops.json")
    artworks_polars_ops: str = str(OUTPUT_DIR / "artworks_output_polars_ops.json")

    # Polars multi-asset implementation
    sales_polars_multi: str = str(OUTPUT_DIR / "sales_output_polars_multi.json")
    artworks_polars_multi: str = str(OUTPUT_DIR / "artworks_output_polars_multi.json")


class DatabaseResource(dg.ConfigurableResource):
    """Database file paths for DuckDB and SQLite."""

    duckdb_path: str = str(OUTPUT_DIR / "dagster.duckdb")
    media_db_path: str = str(INPUT_DIR / "media.db")


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
    metadata={"path": str(INPUT_DIR / "media.db")},
    kinds={"sqlite"},
    group_name="source",
)
