"""Dagster helpers for Cogapp ETL pipelines.

Provides reusable patterns for DuckDB-based Dagster pipelines.

Note: Requires dagster to be installed. Install with:
    pip install cogapp-deps[dagster]
"""

try:
    import dagster as _dg  # noqa: F401
except ImportError as e:
    raise ImportError(
        "dagster is required for cogapp_deps.dagster. "
        "Install with: pip install cogapp-deps[dagster]"
    ) from e

from cogapp_deps.dagster.exceptions import (
    ConfigurationError,
    DataValidationError,
    MissingColumnError,
    MissingTableError,
    PipelineError,
    raise_as_dagster_failure,
)
from cogapp_deps.dagster.helpers import (
    add_dataframe_metadata,
    read_tables_from_duckdb,
    track_timing,
)
from cogapp_deps.dagster.io import (
    DuckDBPandasPolarsIOManager,
    DuckDBRelationTypeHandler,
    read_table,
    write_json_and_return,
    write_json_from_duckdb,
    write_json_output,
)
from cogapp_deps.dagster.io_managers import (
    ElasticsearchIOManager,
    JSONIOManager,
    OpenSearchIOManager,
)
from cogapp_deps.dagster.notifications import (
    create_email_notification_asset,
    create_pipeline_status_notification,
    create_slack_notification_asset,
)
from cogapp_deps.dagster.sitemap import (
    create_image_sitemap_asset,
    create_sitemap_asset,
    generate_image_sitemap_xml,
    generate_sitemap_index_xml,
    generate_sitemap_xml,
)
from cogapp_deps.dagster.validation import (
    read_duckdb_table_lazy,
    read_harvest_table_lazy,
    read_harvest_tables_lazy,
    read_parquet_table_lazy,
    validate_dataframe,
)

try:
    import dlt as _dlt  # noqa: F401

    from cogapp_deps.dagster.api_sources import (
        claude_message_batches,
        voyage_embeddings_batch,
    )
    from cogapp_deps.dagster.dlt_helpers import (
        create_duckdb_pipeline,
        create_parquet_pipeline,
        setup_harvest_parquet_views,
    )

    _HAS_DLT = True
except ImportError:
    _HAS_DLT = False
    create_duckdb_pipeline = None
    create_parquet_pipeline = None
    setup_harvest_parquet_views = None
    claude_message_batches = None
    voyage_embeddings_batch = None

__all__ = [
    # IO utilities
    "DuckDBPandasPolarsIOManager",
    "DuckDBRelationTypeHandler",
    "JSONIOManager",
    "ElasticsearchIOManager",
    "OpenSearchIOManager",
    "read_table",
    "write_json_and_return",
    "write_json_from_duckdb",
    "write_json_output",
    # Exceptions
    "PipelineError",
    "ConfigurationError",
    "DataValidationError",
    "MissingTableError",
    "MissingColumnError",
    "raise_as_dagster_failure",
    # Validation
    "read_duckdb_table_lazy",
    "read_harvest_table_lazy",
    "read_harvest_tables_lazy",
    "read_parquet_table_lazy",
    "validate_dataframe",
    # Helpers
    "read_tables_from_duckdb",
    "add_dataframe_metadata",
    "track_timing",
    # Notifications
    "create_slack_notification_asset",
    "create_email_notification_asset",
    "create_pipeline_status_notification",
    # Sitemaps
    "create_sitemap_asset",
    "create_image_sitemap_asset",
    "generate_sitemap_xml",
    "generate_image_sitemap_xml",
    "generate_sitemap_index_xml",
    # DLT helpers (optional)
    "create_parquet_pipeline",
    "create_duckdb_pipeline",
    "setup_harvest_parquet_views",
    # API sources (optional, requires dlt)
    "claude_message_batches",
    "voyage_embeddings_batch",
]
