"""dlt source factories for harvesting data from various sources.

This module provides factory functions that create dlt sources and pipelines
with injected path configuration. This enables runtime configuration via
Dagster's ConfigurableResource pattern.

Sources:
- CSV files via filesystem source
- SQLite database via sql_database source

The filesystem source supports:
- Local files (file:// or absolute paths)
- S3 (s3://)
- GCS (gs://)
- Azure Blob (az://)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import dlt
from dlt.extract.source import DltSource
from dlt.pipeline.pipeline import Pipeline
from dlt.sources.filesystem import filesystem, read_csv
from dlt.sources.sql_database import sql_database

if TYPE_CHECKING:
    from dlt.extract import DltResource

    from ..shared.resources import DatabaseResource, PathsResource


def create_honey_duck_source(
    paths: PathsResource,
    database: DatabaseResource,
) -> DltSource:
    """Create harvest source combining CSV files and SQLite database.

    Args:
        paths: PathsResource with input_dir for CSV files
        database: DatabaseResource with media_db_path for SQLite

    Returns:
        dlt source with named resources:
        - sales_raw: Sales transactions (CSV)
        - artworks_raw: Artwork catalog (CSV)
        - artists_raw: Artist reference data (CSV)
        - media: Media references (SQLite)
    """

    @dlt.source(name="harvest")
    def _source() -> list["DltResource"]:
        bucket_url = f"file://{paths.input_dir}"

        # CSV resources
        sales = (
            filesystem(
                bucket_url=bucket_url,
                file_glob="sales.csv",
            )
            | read_csv()
        )

        artworks = (
            filesystem(
                bucket_url=bucket_url,
                file_glob="artworks.csv",
            )
            | read_csv()
        )

        artists = (
            filesystem(
                bucket_url=bucket_url,
                file_glob="artists.csv",
            )
            | read_csv()
        )

        # SQLite resource - extract the media table resource
        media_source = sql_database(
            credentials=f"sqlite:///{database.media_db_path}",
        )
        media = media_source.resources["media"]

        return [
            sales.with_name("sales_raw"),
            artworks.with_name("artworks_raw"),
            artists.with_name("artists_raw"),
            media,
        ]

    return _source()


def create_harvest_pipeline(paths: PathsResource) -> Pipeline:
    """Create the dlt pipeline for harvesting to Parquet files.

    Args:
        paths: PathsResource with harvest_dir for output location

    Returns:
        Configured dlt pipeline pointing to Parquet filesystem destination.
        Data is written to {harvest_dir}/raw/{table_name}/
        State is stored in {harvest_dir}/ (same directory)
    """
    return dlt.pipeline(
        pipeline_name="honey_duck_harvest",
        destination=dlt.destinations.filesystem(
            bucket_url=paths.harvest_dir,
        ),
        dataset_name="raw",
        pipelines_dir=paths.harvest_dir,
    )
