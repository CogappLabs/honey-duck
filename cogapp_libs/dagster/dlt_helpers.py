"""Generic DLT pipeline helpers for Dagster projects.

Provides reusable factory functions for creating DLT pipelines with
common destinations (Parquet, DuckDB, etc.).
"""

from pathlib import Path

import dlt
from dlt.pipeline.pipeline import Pipeline

# Optional DuckDB support
try:
    import duckdb  # noqa: F401 - import used for availability check

    _HAS_DUCKDB = True
except ImportError:
    _HAS_DUCKDB = False


def create_parquet_pipeline(
    pipeline_name: str,
    destination_dir: str | Path,
    dataset_name: str = "raw",
    layout: str = "{table_name}/{load_id}.{file_id}.{ext}",
) -> Pipeline:
    """Create DLT pipeline with Parquet filesystem destination.

    Args:
        pipeline_name: Unique name for the pipeline
        destination_dir: Directory where Parquet files will be written
        dataset_name: Dataset name (used as subdirectory)
        layout: File layout pattern (default: table_name/load_id.file_id.ext)

    Returns:
        Configured dlt.Pipeline

    Example:
        ```python
        pipeline = create_parquet_pipeline(
            "my_harvest",
            "/data/harvest_parquet",
            dataset_name="raw"
        )
        pipeline.run(my_source(), loader_file_format="parquet")
        ```
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
) -> Pipeline:
    """Create DLT pipeline with DuckDB destination.

    Args:
        pipeline_name: Unique name for the pipeline
        db_path: Path to DuckDB database file
        dataset_name: Schema name in DuckDB (default: "raw")

    Returns:
        Configured dlt.Pipeline

    Example:
        ```python
        pipeline = create_duckdb_pipeline(
            "my_harvest",
            "/data/pipeline.duckdb",
            dataset_name="raw"
        )
        pipeline.run(my_source())
        ```
    """
    # Ensure parent directory exists
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name=dataset_name,
    )


def setup_harvest_parquet_views(
    conn,
    harvest_dir: str | Path,
    schema: str = "raw",
    tables: list[str] | None = None,
) -> None:
    """Create DuckDB views pointing to harvest Parquet files.

    Creates views that enable SQL queries over Parquet files written by DLT.
    This is useful for pipelines that need to query harvest data using SQL
    while keeping the harvest layer in Parquet format.

    Args:
        conn: DuckDB connection object
        harvest_dir: Path to harvest Parquet directory (e.g., data/output/dlt/harvest_parquet)
        schema: Schema name for views (default: "raw")
        tables: List of table names to create views for.
                If None, will create views for common tables:
                ["sales_raw", "artworks_raw", "artists_raw", "media"]

    Raises:
        ValueError: If harvest directory path is invalid
        ImportError: If duckdb is not installed

    Example:
        ```python
        import duckdb
        from pathlib import Path
        conn = duckdb.connect("pipeline.duckdb")
        setup_harvest_parquet_views(
            conn,
            Path("data/output/dlt/harvest_parquet"),
            schema="raw",
            tables=["sales_raw", "artworks_raw"]
        )
        result = conn.sql("SELECT * FROM raw.sales_raw").pl()
        ```
    """
    if not _HAS_DUCKDB:
        raise ImportError(
            "duckdb is required for setup_harvest_parquet_views. Install with: pip install duckdb"
        )

    # Validate and resolve harvest directory path to prevent injection
    harvest_dir = Path(harvest_dir).resolve()
    if not str(harvest_dir).startswith(str(Path.cwd().resolve())):
        raise ValueError(f"Invalid harvest directory outside project: {harvest_dir}")

    # Default tables if not specified
    if tables is None:
        tables = ["sales_raw", "artworks_raw", "artists_raw", "media"]

    # Create schema
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    # Create view for each table
    for table in tables:
        parquet_path = harvest_dir / schema / table
        conn.execute(f"""
            CREATE OR REPLACE VIEW {schema}.{table} AS
            SELECT * FROM read_parquet('{parquet_path}/*.parquet')
        """)
