"""dlt sources for harvesting data from various sources.

This module provides generic dlt sources that can be configured
to load data from filesystems, APIs, and other sources.

The filesystem source supports:
- Local files (file:// or absolute paths)
- S3 (s3://)
- GCS (gs://)
- Azure Blob (az://)
"""

import dlt
from dlt.sources.filesystem import filesystem, read_csv

from .resources import DUCKDB_PATH, INPUT_DIR


@dlt.source(name="honey_duck_harvest")
def honey_duck_source():
    """Harvest source for honey-duck CSV files.

    Creates named resources for each CSV file:
    - sales_raw: Sales transactions
    - artworks_raw: Artwork catalog
    - artists_raw: Artist reference data

    The resources can be run selectively or all together.
    """
    bucket_url = f"file://{INPUT_DIR}"

    # Sales data
    sales = filesystem(
        bucket_url=bucket_url,
        file_glob="sales.csv",
    ) | read_csv()

    # Artworks catalog
    artworks = filesystem(
        bucket_url=bucket_url,
        file_glob="artworks.csv",
    ) | read_csv()

    # Artists reference
    artists = filesystem(
        bucket_url=bucket_url,
        file_glob="artists.csv",
    ) | read_csv()

    return [
        sales.with_name("sales_raw"),
        artworks.with_name("artworks_raw"),
        artists.with_name("artists_raw"),
    ]


def get_harvest_pipeline() -> dlt.Pipeline:
    """Get the dlt pipeline for harvesting to DuckDB.

    Returns:
        Configured dlt pipeline pointing to honey-duck DuckDB.
    """
    return dlt.pipeline(
        pipeline_name="honey_duck_harvest",
        destination=dlt.destinations.duckdb(DUCKDB_PATH),
        dataset_name="raw",
    )
