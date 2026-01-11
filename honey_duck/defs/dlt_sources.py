"""dlt sources for harvesting data from various sources.

This module provides generic dlt sources that can be configured
to load data from filesystems, APIs, databases, and other sources.

Sources:
- CSV files via filesystem source
- SQLite database via sql_database source

The filesystem source supports:
- Local files (file:// or absolute paths)
- S3 (s3://)
- GCS (gs://)
- Azure Blob (az://)
"""

import dlt
from dlt.sources.filesystem import filesystem, read_csv
from dlt.sources.sql_database import sql_database

from .resources import DUCKDB_PATH, HARVEST_PARQUET_DIR, INPUT_DIR, MEDIA_DB_PATH


@dlt.source(name="harvest")
def honey_duck_source():
    """Harvest source combining CSV files and SQLite database.

    Creates named resources:
    - sales_raw: Sales transactions (CSV)
    - artworks_raw: Artwork catalog (CSV)
    - artists_raw: Artist reference data (CSV)
    - media: Media references (SQLite)

    All resources are loaded in a single dlt.run() call,
    avoiding DuckDB lock conflicts.
    """
    bucket_url = f"file://{INPUT_DIR}"

    # CSV resources
    sales = filesystem(
        bucket_url=bucket_url,
        file_glob="sales.csv",
    ) | read_csv()

    artworks = filesystem(
        bucket_url=bucket_url,
        file_glob="artworks.csv",
    ) | read_csv()

    artists = filesystem(
        bucket_url=bucket_url,
        file_glob="artists.csv",
    ) | read_csv()

    # SQLite resource - extract the media table resource
    media_source = sql_database(
        credentials=f"sqlite:///{MEDIA_DB_PATH}",
    )
    media = media_source.resources["media"]

    return [
        sales.with_name("sales_raw"),
        artworks.with_name("artworks_raw"),
        artists.with_name("artists_raw"),
        media,
    ]


def get_harvest_pipeline() -> dlt.Pipeline:
    """Get the dlt pipeline for harvesting to Parquet files.

    Returns:
        Configured dlt pipeline pointing to Parquet filesystem destination.
    """
    # Ensure harvest directory exists
    HARVEST_PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    return dlt.pipeline(
        pipeline_name="honey_duck_harvest_parquet",
        destination=dlt.destinations.filesystem(
            bucket_url=str(HARVEST_PARQUET_DIR),
            layout="{table_name}/{load_id}.{file_id}.{ext}",
        ),
        dataset_name="raw",
    )
