"""Dagster helpers for Cogapp ETL pipelines.

Provides reusable patterns for DuckDB-based Dagster pipelines.

Note: Requires dagster to be installed. Install with:
    pip install cogapp-deps[dagster]
"""

try:
    import dagster as _dg  # noqa: F401
except ImportError as e:
    raise ImportError(
        "dagster is required for cogapp_libs.dagster. "
        "Install with: pip install cogapp-deps[dagster]"
    ) from e

from cogapp_libs.dagster.exceptions import (
    ConfigurationError,
    DataValidationError,
    MissingColumnError,
    MissingTableError,
    PipelineError,
    raise_as_dagster_failure,
)
from cogapp_libs.dagster.helpers import (
    add_dataframe_metadata,
    altair_to_metadata,
    read_tables_from_duckdb,
    table_preview_to_metadata,
    track_timing,
)
from cogapp_libs.dagster.io import (
    DuckDBPandasPolarsIOManager,
    DuckDBRelationTypeHandler,
    read_table,
    write_json_and_return,
    write_json_from_duckdb,
    write_json_output,
)
from cogapp_libs.dagster.io_managers import (
    ElasticsearchIOManager,
    JSONIOManager,
    OpenSearchIOManager,
)
from cogapp_libs.dagster.duckdb import (
    DuckDBContext,
    DuckDBExecutor,
    duckdb_output_asset,
    duckdb_transform_asset,
)
from cogapp_libs.dagster.lineage import (
    HARVEST_VIEWS,
    add_lineage_examples_to_dlt_results,
    build_lineage,
    collect_json_output_metadata,
    collect_parquet_metadata,
    format_value,
    get_example_row,
    passthrough_lineage,
    register_harvest_views,
)

# Components (for YAML-based configuration)
try:
    from cogapp_libs.dagster.components import ElasticsearchIOManagerComponent

    _HAS_COMPONENTS = True
except ImportError:
    _HAS_COMPONENTS = False
    ElasticsearchIOManagerComponent = None  # type: ignore[assignment, misc]
from cogapp_libs.dagster.notifications import (
    create_email_notification_asset,
    create_pipeline_status_notification,
    create_slack_notification_asset,
)
from cogapp_libs.dagster.sitemap import (
    create_image_sitemap_asset,
    create_sitemap_asset,
    generate_image_sitemap_xml,
    generate_sitemap_index_xml,
    generate_sitemap_xml,
)
from cogapp_libs.dagster.validation import (
    read_duckdb_table_lazy,
    read_harvest_table_lazy,
    read_harvest_tables_lazy,
    read_parquet_table_lazy,
    validate_dataframe,
)

# XML parsing (no dlt dependency)
from cogapp_libs.dagster.xml_sources import (
    parse_xml_file,
    parse_xml_http,
    parse_xml_s3,
    parse_xml_streaming,
    parse_xml_string,
)

try:
    import dlt as _dlt  # noqa: F401

    from cogapp_libs.dagster.api_sources import (
        voyage_embeddings_batch,
    )
    from cogapp_libs.dagster.dlt_helpers import (
        create_duckdb_pipeline,
        create_parquet_pipeline,
        setup_harvest_parquet_views,
    )

    _HAS_DLT = True
except ImportError:
    _HAS_DLT = False
    create_duckdb_pipeline = None  # type: ignore[assignment]
    create_parquet_pipeline = None  # type: ignore[assignment]
    setup_harvest_parquet_views = None  # type: ignore[assignment]
    voyage_embeddings_batch = None  # type: ignore[assignment]

__all__ = [
    # IO utilities
    "DuckDBPandasPolarsIOManager",
    "DuckDBRelationTypeHandler",
    "JSONIOManager",
    "ElasticsearchIOManager",
    "OpenSearchIOManager",
    # Components
    "ElasticsearchIOManagerComponent",
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
    # Visualization helpers
    "altair_to_metadata",
    "table_preview_to_metadata",
    # DuckDB asset factories
    "duckdb_transform_asset",
    "duckdb_output_asset",
    "DuckDBExecutor",
    "DuckDBContext",
    # Lineage helpers
    "build_lineage",
    "passthrough_lineage",
    "HARVEST_VIEWS",
    "register_harvest_views",
    "collect_parquet_metadata",
    "collect_json_output_metadata",
    "format_value",
    "get_example_row",
    "add_lineage_examples_to_dlt_results",
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
    "voyage_embeddings_batch",
    # XML parsing
    "parse_xml_file",
    "parse_xml_string",
    "parse_xml_streaming",
    "parse_xml_http",
    "parse_xml_s3",
]
