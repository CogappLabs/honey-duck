"""Generic DLT pipeline helpers for Dagster projects.

Provides reusable factory functions for creating DLT pipelines with
common destinations (Parquet, DuckDB, etc.).
"""

from pathlib import Path

import dlt


def create_parquet_pipeline(
    pipeline_name: str,
    destination_dir: str | Path,
    dataset_name: str = "raw",
    layout: str = "{table_name}/{load_id}.{file_id}.{ext}",
) -> dlt.Pipeline:
    """Create DLT pipeline with Parquet filesystem destination.

    Args:
        pipeline_name: Unique name for the pipeline
        destination_dir: Directory where Parquet files will be written
        dataset_name: Dataset name (used as subdirectory)
        layout: File layout pattern (default: table_name/load_id.file_id.ext)

    Returns:
        Configured dlt.Pipeline

    Example:
        >>> pipeline = create_parquet_pipeline(
        ...     "my_harvest",
        ...     "/data/harvest_parquet",
        ...     dataset_name="raw"
        ... )
        >>> pipeline.run(my_source(), loader_file_format="parquet")
    """
    destination_dir = Path(destination_dir)
    destination_dir.mkdir(parents=True, exist_ok=True)

    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.filesystem(
            bucket_url=str(destination_dir),
            layout=layout,
        ),
        dataset_name=dataset_name,
    )


def create_duckdb_pipeline(
    pipeline_name: str,
    db_path: str | Path,
    dataset_name: str = "raw",
) -> dlt.Pipeline:
    """Create DLT pipeline with DuckDB destination.

    Args:
        pipeline_name: Unique name for the pipeline
        db_path: Path to DuckDB database file
        dataset_name: Schema name in DuckDB (default: "raw")

    Returns:
        Configured dlt.Pipeline

    Example:
        >>> pipeline = create_duckdb_pipeline(
        ...     "my_harvest",
        ...     "/data/pipeline.duckdb",
        ...     dataset_name="raw"
        ... )
        >>> pipeline.run(my_source())
    """
    # Ensure parent directory exists
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name=dataset_name,
    )
