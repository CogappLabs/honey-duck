"""Pytest fixtures for honey-duck tests.

Provides mock resources and utilities for testing Dagster assets without
requiring external services or full data materialization.

Key fixtures:
- smoke_paths: Temporary PathsResource with empty harvest data
- noop_io_manager: IOManager that accepts but doesn't persist data
- mock_resources: Complete resource dict for materialize()
"""

from pathlib import Path
from typing import Any

import polars as pl
import pytest
from dagster import IOManager, InputContext, OutputContext

from honey_duck.defs.resources import OutputPathsResource, PathsResource


class NoOpIOManager(IOManager):
    """IO Manager that accepts output but doesn't persist it.

    Useful for smoke tests where we want to verify asset logic
    without writing to disk or external systems.

    Handles both DataFrame and LazyFrame outputs/inputs.
    """

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Accept output and log stats without persisting."""
        if obj is None:
            context.log.info("NoOp: Received None output")
            return

        # Handle LazyFrames by collecting for count
        if hasattr(obj, "collect"):
            # It's a LazyFrame
            context.log.info("NoOp: Would persist LazyFrame")
            # Don't collect - just note it's a LazyFrame
            context.add_output_metadata({"type": "LazyFrame"})
        elif hasattr(obj, "__len__"):
            context.log.info(f"NoOp: Would persist {len(obj):,} records")
            context.add_output_metadata({"record_count": len(obj)})
        else:
            context.log.info(f"NoOp: Would persist {type(obj).__name__}")

    def load_input(self, context: InputContext) -> pl.LazyFrame:
        """Return empty LazyFrame with expected schema structure."""
        asset_key = context.asset_key.path[-1]
        context.log.info(f"NoOp: Returning empty LazyFrame for {asset_key}")

        # Return empty LazyFrame - assets should handle empty inputs gracefully
        # Using LazyFrame since many assets in this codebase return LazyFrame
        return pl.DataFrame().lazy()


@pytest.fixture
def temp_harvest_dir(tmp_path: Path) -> Path:
    """Create temporary harvest directory with empty Parquet files.

    Creates the minimum required directory structure for assets that
    read from harvest tables.
    """
    harvest_dir = tmp_path / "harvest" / "raw"

    # Create empty Parquet files for each harvest table
    # These have the correct schema but no data
    tables = {
        "sales_raw": pl.DataFrame(
            {
                "sale_id": pl.Series([], dtype=pl.Int64),
                "artwork_id": pl.Series([], dtype=pl.Int64),
                "sale_date": pl.Series([], dtype=pl.Utf8),
                "sale_price_usd": pl.Series([], dtype=pl.Int64),
                "buyer_country": pl.Series([], dtype=pl.Utf8),
            }
        ),
        "artworks_raw": pl.DataFrame(
            {
                "artwork_id": pl.Series([], dtype=pl.Int64),
                "title": pl.Series([], dtype=pl.Utf8),
                "artist_id": pl.Series([], dtype=pl.Int64),
                "year": pl.Series([], dtype=pl.Float64),
                "medium": pl.Series([], dtype=pl.Utf8),
                "price_usd": pl.Series([], dtype=pl.Int64),
            }
        ),
        "artists_raw": pl.DataFrame(
            {
                "artist_id": pl.Series([], dtype=pl.Int64),
                "name": pl.Series([], dtype=pl.Utf8),
                "nationality": pl.Series([], dtype=pl.Utf8),
            }
        ),
        "media_raw": pl.DataFrame(
            {
                "artwork_id": pl.Series([], dtype=pl.Int64),
                "image_url": pl.Series([], dtype=pl.Utf8),
                "alt_text": pl.Series([], dtype=pl.Utf8),
                "is_primary": pl.Series([], dtype=pl.Boolean),
            }
        ),
    }

    for table_name, df in tables.items():
        table_dir = harvest_dir / table_name
        table_dir.mkdir(parents=True, exist_ok=True)
        df.write_parquet(table_dir / "data.parquet")

    return tmp_path / "harvest"


@pytest.fixture
def smoke_paths(temp_harvest_dir: Path, tmp_path: Path) -> PathsResource:
    """PathsResource pointing to temporary directories with empty data."""
    return PathsResource(
        input_dir=str(tmp_path / "input"),
        output_dir=str(tmp_path / "output"),
        storage_dir=str(tmp_path / "storage"),
        harvest_dir=str(temp_harvest_dir),
    )


@pytest.fixture
def smoke_output_paths(tmp_path: Path) -> OutputPathsResource:
    """OutputPathsResource pointing to temporary output files."""
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    return OutputPathsResource(
        sales=str(output_dir / "sales.json"),
        artworks=str(output_dir / "artworks.json"),
        sales_polars=str(output_dir / "sales_polars.json"),
        artworks_polars=str(output_dir / "artworks_polars.json"),
        sales_duckdb=str(output_dir / "sales_duckdb.json"),
        artworks_duckdb=str(output_dir / "artworks_duckdb.json"),
        sales_polars_fs=str(output_dir / "sales_polars_fs.json"),
        artworks_polars_fs=str(output_dir / "artworks_polars_fs.json"),
        sales_polars_ops=str(output_dir / "sales_polars_ops.json"),
        artworks_polars_ops=str(output_dir / "artworks_polars_ops.json"),
        sales_polars_multi=str(output_dir / "sales_polars_multi.json"),
        artworks_polars_multi=str(output_dir / "artworks_polars_multi.json"),
    )


@pytest.fixture
def noop_io_manager() -> NoOpIOManager:
    """NoOpIOManager for testing without persistence."""
    return NoOpIOManager()


@pytest.fixture
def mock_resources(
    smoke_paths: PathsResource,
    smoke_output_paths: OutputPathsResource,
    noop_io_manager: NoOpIOManager,
) -> dict:
    """Complete resource dict for materialize() in smoke tests.

    Usage:
        result = materialize(
            assets=[my_asset],
            resources=mock_resources,
        )
    """
    return {
        "paths": smoke_paths,
        "output_paths": smoke_output_paths,
        "io_manager": noop_io_manager,
    }


# -----------------------------------------------------------------------------
# Sample Data Fixtures (for tests that need non-empty data)
# -----------------------------------------------------------------------------


@pytest.fixture
def sample_sales_df() -> pl.DataFrame:
    """Sample sales DataFrame for unit tests."""
    return pl.DataFrame(
        {
            "sale_id": [1, 2, 3],
            "artwork_id": [101, 102, 103],
            "sale_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "sale_price_usd": [1500, 2500, 3500],
            "buyer_country": ["USA", "UK", "France"],
            "title": ["Artwork A", "Artwork B", "Artwork C"],
            "artist_id": [1, 1, 2],
            "artwork_year": [2020.0, 2021.0, 2022.0],
            "medium": ["Oil", "Acrylic", "Watercolor"],
            "list_price_usd": [1000, 2000, 3000],
            "artist_name": ["Artist One", "Artist One", "Artist Two"],
            "nationality": ["American", "American", "French"],
        }
    )


@pytest.fixture
def sample_artworks_df() -> pl.DataFrame:
    """Sample artworks DataFrame for unit tests."""
    return pl.DataFrame(
        {
            "artwork_id": [101, 102, 103],
            "title": ["Artwork A", "Artwork B", "Artwork C"],
            "year": [2020.0, 2021.0, 2022.0],
            "medium": ["Oil", "Acrylic", "Watercolor"],
            "list_price_usd": [1000, 2000, 3000],
            "artist_name": ["Artist One", "Artist One", "Artist Two"],
            "nationality": ["American", "American", "French"],
            "sale_count": [1, 1, 1],
            "total_sales_value": [1500.0, 2500.0, 3500.0],
            "avg_sale_price": [1500.0, 2500.0, 3500.0],
            "first_sale_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "last_sale_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "has_sold": [True, True, True],
            "price_tier": ["mid", "mid", "premium"],
            "sales_rank": [3, 2, 1],
            "primary_image": [None, None, None],
            "primary_image_alt": [None, None, None],
            "media_count": [0, 0, 0],
        }
    )
