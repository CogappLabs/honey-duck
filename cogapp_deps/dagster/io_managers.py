"""Custom IO Managers for Dagster pipelines.

Provides JSON IO Manager for writing final outputs using Dagster's IO system.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import dagster as dg
import duckdb
from dagster import InputContext, OutputContext
from dagster._core.storage.upath_io_manager import UPathIOManager

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    from upath import UPath


class JSONIOManager(UPathIOManager):
    """IO Manager that writes DataFrames to JSON files using DuckDB's native export.

    Integrates JSON output writing into Dagster's IO system rather than as a side effect.
    Supports both Polars and Pandas DataFrames.

    Features:
    - Uses DuckDB's native COPY command for performance
    - Handles both pandas and polars DataFrames
    - Automatically creates parent directories
    - Provides standard metadata (record count, preview, path)

    Attributes:
        extension: File extension (default: ".json")

    Example:
        >>> # In definitions.py:
        >>> defs = dg.Definitions(
        ...     assets=[...],
        ...     resources={
        ...         "json_io_manager": JSONIOManager(base_path="data/output/json"),
        ...     },
        ... )
        >>>
        >>> # In asset:
        >>> @dg.asset(io_manager_key="json_io_manager")
        >>> def sales_output(sales_transform: pl.DataFrame) -> pl.DataFrame:
        ...     # JSON is written automatically by IO manager
        ...     return sales_transform.filter(pl.col("price") > 1000)
    """

    extension: str = ".json"

    def dump_to_path(self, context: OutputContext, obj: pd.DataFrame | pl.DataFrame, path: UPath):
        """Write DataFrame to JSON file using DuckDB's native COPY command.

        Args:
            context: Dagster output context
            obj: DataFrame to write (pandas or polars)
            path: UPath to write JSON file
        """
        # Ensure parent directory exists
        path.parent.mkdir(parents=True, exist_ok=True)

        # Convert UPath to string for DuckDB
        output_path_str = str(path)

        # Use DuckDB's native JSON export for performance
        conn = duckdb.connect(":memory:")
        conn.register("_df", obj)
        conn.execute(f"COPY (SELECT * FROM _df) TO '{output_path_str}' (FORMAT JSON, ARRAY true)")

        # Get preview as pandas for markdown rendering
        preview_df = conn.sql("SELECT * FROM _df LIMIT 10").df()
        result = conn.sql("SELECT COUNT(*) FROM _df").fetchone()
        record_count = result[0] if result else 0
        conn.close()

        # Add metadata
        context.add_output_metadata(
            {
                "record_count": record_count,
                "json_output": dg.MetadataValue.path(output_path_str),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )

        context.log.info(f"Wrote {record_count:,} records to {path}")

    def load_from_path(self, context: InputContext, path: UPath) -> pl.DataFrame:
        """Load DataFrame from JSON file.

        Args:
            context: Dagster input context
            path: UPath to read JSON file

        Returns:
            Polars DataFrame (for downstream compatibility)
        """
        import polars as pl

        # Load JSON file as Polars DataFrame (convert UPath to str for polars compatibility)
        df = pl.read_json(str(path))

        context.log.info(f"Loaded {len(df):,} records from {path}")

        return df


class ElasticsearchIOManager(dg.IOManager):
    """IO Manager for writing DataFrames to Elasticsearch 8/9 and reading them back.

    Provides integration with Elasticsearch for storing and querying pipeline outputs.
    Supports both Polars and Pandas DataFrames with efficient bulk indexing.

    Features:
    - Bulk indexing for high performance (configurable batch size)
    - Automatic index creation with dynamic or custom mappings
    - Support for both Elasticsearch 8 and 9
    - Connection pooling and retry logic
    - Metadata tracking (document count, index name, response time)
    - Handles both write (dump) and read (load) operations

    Args:
        hosts (list[str]): Elasticsearch host(s) (e.g., ["http://localhost:9200"])
        index_prefix (str): Prefix for index names (e.g., "dagster_")
        api_key (str | None): Elasticsearch API key (recommended for security)
        basic_auth (tuple[str, str] | None): Tuple of (username, password) if not using API key
        verify_certs (bool): Whether to verify SSL certificates (default: True)
        bulk_size (int): Number of documents per bulk request (default: 500)
        custom_mappings (dict | None): Optional dict of {asset_name: mapping_dict}

    Environment Variables (recommended for credentials):
        ELASTICSEARCH_HOST: Comma-separated hosts
        ELASTICSEARCH_API_KEY: API key for authentication
        ELASTICSEARCH_USER: Username for basic auth
        ELASTICSEARCH_PASSWORD: Password for basic auth

    Example:
        >>> # In definitions.py:
        >>> import os
        >>>
        >>> defs = dg.Definitions(
        ...     assets=[sales_output, artworks_output],
        ...     resources={
        ...         "elasticsearch_io_manager": ElasticsearchIOManager(
        ...             hosts=[os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")],
        ...             index_prefix="honey_duck_",
        ...             api_key=os.getenv("ELASTICSEARCH_API_KEY"),
        ...             bulk_size=1000,
        ...         ),
        ...     },
        ... )
        >>>
        >>> # In asset:
        >>> @dg.asset(io_manager_key="elasticsearch_io_manager")
        >>> def sales_output(sales_transform: pl.DataFrame) -> pl.DataFrame:
        ...     # DataFrame automatically indexed to Elasticsearch
        ...     return sales_transform

    Index Naming:
        Indices are named: {index_prefix}{asset_name}_{timestamp}
        Example: "honey_duck_sales_output_20240115"

    Custom Mappings Example:
        >>> custom_mappings = {
        ...     "sales_output": {
        ...         "properties": {
        ...             "sale_id": {"type": "keyword"},
        ...             "sale_price_usd": {"type": "float"},
        ...             "sale_date": {"type": "date"},
        ...             "artwork_title": {"type": "text"},
        ...         }
        ...     }
        ... }
        >>>
        >>> ElasticsearchIOManager(
        ...     hosts=["http://localhost:9200"],
        ...     custom_mappings=custom_mappings,
        ... )
    """

    def __init__(
        self,
        hosts: list[str],
        index_prefix: str = "dagster_",
        api_key: str | None = None,
        basic_auth: tuple[str, str] | None = None,
        verify_certs: bool = True,
        bulk_size: int = 500,
        custom_mappings: dict[str, dict] | None = None,
        **es_kwargs,
    ):
        self.hosts = hosts
        self.index_prefix = index_prefix
        self.api_key = api_key
        self.basic_auth = basic_auth
        self.verify_certs = verify_certs
        self.bulk_size = bulk_size
        self.custom_mappings = custom_mappings or {}
        self.es_kwargs = es_kwargs
        self._client = None

    def _get_client(self):
        """Get or create Elasticsearch client with connection pooling."""
        if self._client is None:
            try:
                from elasticsearch import Elasticsearch
            except ImportError:
                raise ImportError(
                    "elasticsearch package not found. Install with: "
                    "pip install elasticsearch>=8.0.0"
                )

            # Build authentication
            auth_kwargs = {}
            if self.api_key:
                auth_kwargs["api_key"] = self.api_key
            elif self.basic_auth:
                auth_kwargs["basic_auth"] = self.basic_auth

            self._client = Elasticsearch(
                self.hosts,
                verify_certs=self.verify_certs,
                **auth_kwargs,
                **self.es_kwargs,
            )

        return self._client

    def _get_index_name(self, context: OutputContext | InputContext) -> str:
        """Generate index name from asset key.

        Format: {prefix}{asset_name}

        Args:
            context: Dagster context

        Returns:
            Index name string
        """
        asset_name = context.asset_key.path[-1]
        return f"{self.index_prefix}{asset_name}"

    def _create_index_if_not_exists(
        self,
        client,
        index_name: str,
        asset_name: str,
        context: OutputContext,
    ):
        """Create index with mappings if it doesn't exist.

        Args:
            client: Elasticsearch client
            index_name: Name of index to create
            asset_name: Asset name for custom mapping lookup
            context: Dagster output context
        """
        if not client.indices.exists(index=index_name):
            # Use custom mapping if provided, otherwise use dynamic mapping
            index_body = {}

            if asset_name in self.custom_mappings:
                index_body["mappings"] = self.custom_mappings[asset_name]
                context.log.info(f"Creating index '{index_name}' with custom mapping")
            else:
                context.log.info(f"Creating index '{index_name}' with dynamic mapping")

            client.indices.create(index=index_name, body=index_body)

    def handle_output(self, context: OutputContext, obj: pd.DataFrame | pl.DataFrame):
        """Write DataFrame to Elasticsearch using bulk API.

        Args:
            context: Dagster output context
            obj: DataFrame to index (pandas or polars)
        """
        import time
        import polars as pl

        client = self._get_client()
        index_name = self._get_index_name(context)
        asset_name = context.asset_key.path[-1]

        # Create index if needed
        self._create_index_if_not_exists(client, index_name, asset_name, context)

        # Convert to records (list of dicts)
        if isinstance(obj, pl.DataFrame):
            records = obj.to_dicts()
        else:  # pandas
            records = cast(list[dict[str, Any]], obj.to_dict("records"))

        context.log.info(f"Indexing {len(records):,} documents to '{index_name}'")

        # Bulk index in batches
        from elasticsearch.helpers import bulk

        start_time = time.perf_counter()

        # Generate bulk actions
        def generate_actions():
            for record in records:
                # Use _id if present, otherwise let ES generate
                action = {
                    "_index": index_name,
                    "_source": record,
                }
                if "id" in record:
                    action["_id"] = record["id"]
                elif "_id" in record:
                    action["_id"] = record["_id"]

                yield action

        # Perform bulk indexing
        success_count, failed_items = bulk(
            client,
            generate_actions(),
            chunk_size=self.bulk_size,
            raise_on_error=False,
            raise_on_exception=False,
        )

        elapsed_ms = (time.perf_counter() - start_time) * 1000

        # Refresh index to make documents searchable
        client.indices.refresh(index=index_name)

        # Handle any errors
        if failed_items:
            error_sample = failed_items[:5]  # First 5 errors
            context.log.warning(
                f"Failed to index {len(failed_items)} documents. Sample errors: {error_sample}"
            )

        # Add metadata
        context.add_output_metadata(
            {
                "index_name": index_name,
                "document_count": success_count,
                "failed_count": len(failed_items) if failed_items else 0,
                "bulk_size": self.bulk_size,
                "index_time_ms": round(elapsed_ms, 2),
                "elasticsearch_host": self.hosts[0],
            }
        )

        context.log.info(
            f"Successfully indexed {success_count:,} documents to '{index_name}' "
            f"in {elapsed_ms:.0f}ms"
        )

    def load_input(self, context: InputContext) -> pl.DataFrame:
        """Load DataFrame from Elasticsearch index.

        Retrieves all documents from the index and returns as Polars DataFrame.
        For large indices, consider using scroll API or filtering.

        Args:
            context: Dagster input context

        Returns:
            Polars DataFrame with all documents from index
        """
        import polars as pl

        client = self._get_client()
        index_name = self._get_index_name(context)

        # Check if index exists
        if not client.indices.exists(index=index_name):
            raise ValueError(
                f"Index '{index_name}' does not exist. Materialize the upstream asset first."
            )

        # Get all documents (for large indices, use scan/scroll instead)
        # Get document count first
        count_response = client.count(index=index_name)
        doc_count = count_response["count"]

        context.log.info(f"Loading {doc_count:,} documents from '{index_name}'")

        # Fetch all documents
        # Note: For large datasets (>10k docs), use elasticsearch.helpers.scan
        if doc_count > 10000:
            context.log.warning(
                f"Large index ({doc_count:,} docs). Consider using scan API for better performance."
            )

        response = client.search(
            index=index_name,
            size=min(doc_count, 10000),  # Elasticsearch max size
            query={"match_all": {}},
        )

        # Extract _source from hits
        records = [hit["_source"] for hit in response["hits"]["hits"]]

        # Convert to Polars DataFrame
        if records:
            df = pl.DataFrame(records)
        else:
            df = pl.DataFrame()  # Empty DataFrame

        context.log.info(f"Loaded {len(df):,} records from '{index_name}'")

        return df


class OpenSearchIOManager(dg.IOManager):
    """IO Manager for writing DataFrames to OpenSearch (AWS fork of Elasticsearch).

    OpenSearch is AWS's fork of Elasticsearch 7.x. This IO Manager provides the same
    functionality as ElasticsearchIOManager but uses the opensearch-py client.

    Features:
    - Bulk indexing for high performance (configurable batch size)
    - Automatic index creation with dynamic or custom mappings
    - Support for OpenSearch 1.x, 2.x
    - Connection pooling and retry logic
    - Metadata tracking (document count, index name, response time)
    - Handles both write (dump) and read (load) operations

    Args:
        hosts (list[str]): OpenSearch host(s) (e.g., ["https://search-domain.us-east-1.es.amazonaws.com"])
        index_prefix (str): Prefix for index names (e.g., "dagster_")
        http_auth (tuple[str, str] | None): Tuple of (username, password) for basic auth
        use_ssl (bool): Whether to use SSL (default: True)
        verify_certs (bool): Whether to verify SSL certificates (default: True)
        bulk_size (int): Number of documents per bulk request (default: 500)
        custom_mappings (dict | None): Optional dict of {asset_name: mapping_dict}
        aws_auth (bool): Whether to use AWS SigV4 auth (requires boto3)
        region (str | None): AWS region (required if aws_auth=True)

    Environment Variables:
        OPENSEARCH_HOST: OpenSearch endpoint URL
        OPENSEARCH_USER: Username for basic auth
        OPENSEARCH_PASSWORD: Password for basic auth
        AWS_REGION: AWS region for SigV4 auth

    Example (Basic Auth):
        >>> defs = dg.Definitions(
        ...     resources={
        ...         "opensearch_io_manager": OpenSearchIOManager(
        ...             hosts=["https://localhost:9200"],
        ...             http_auth=("admin", "admin"),
        ...             verify_certs=False,  # For local dev
        ...         ),
        ...     },
        ... )

    Example (AWS SigV4 Auth):
        >>> defs = dg.Definitions(
        ...     resources={
        ...         "opensearch_io_manager": OpenSearchIOManager(
        ...             hosts=["https://search-domain.us-east-1.es.amazonaws.com"],
        ...             aws_auth=True,
        ...             region="us-east-1",
        ...         ),
        ...     },
        ... )

    Example (Asset):
        >>> @dg.asset(io_manager_key="opensearch_io_manager")
        >>> def sales_output(sales_transform: pl.DataFrame) -> pl.DataFrame:
        ...     return sales_transform
    """

    def __init__(
        self,
        hosts: list[str],
        index_prefix: str = "dagster_",
        http_auth: tuple[str, str] | None = None,
        use_ssl: bool = True,
        verify_certs: bool = True,
        bulk_size: int = 500,
        custom_mappings: dict[str, dict] | None = None,
        aws_auth: bool = False,
        region: str | None = None,
        **opensearch_kwargs,
    ):
        self.hosts = hosts
        self.index_prefix = index_prefix
        self.http_auth = http_auth
        self.use_ssl = use_ssl
        self.verify_certs = verify_certs
        self.bulk_size = bulk_size
        self.custom_mappings = custom_mappings or {}
        self.aws_auth = aws_auth
        self.region = region
        self.opensearch_kwargs = opensearch_kwargs
        self._client = None

    def _get_client(self):
        """Get or create OpenSearch client with connection pooling."""
        if self._client is None:
            try:
                from opensearchpy import OpenSearch
            except ImportError:
                raise ImportError(
                    "opensearch-py package not found. Install with: pip install opensearch-py"
                )

            client_kwargs = {
                "hosts": self.hosts,
                "use_ssl": self.use_ssl,
                "verify_certs": self.verify_certs,
                **self.opensearch_kwargs,
            }

            # Add authentication
            if self.aws_auth:
                # AWS SigV4 authentication
                try:
                    from opensearchpy import RequestsHttpConnection, AWSV4SignerAuth
                    import boto3
                except ImportError:
                    raise ImportError(
                        "AWS authentication requires boto3 and requests. "
                        "Install with: pip install boto3 requests"
                    )

                credentials = boto3.Session().get_credentials()
                client_kwargs["http_auth"] = AWSV4SignerAuth(credentials, self.region)
                client_kwargs["connection_class"] = RequestsHttpConnection

            elif self.http_auth:
                # Basic authentication
                client_kwargs["http_auth"] = self.http_auth

            self._client = OpenSearch(**client_kwargs)

        return self._client

    def _get_index_name(self, context: OutputContext | InputContext) -> str:
        """Generate index name from asset key."""
        asset_name = context.asset_key.path[-1]
        return f"{self.index_prefix}{asset_name}"

    def _create_index_if_not_exists(
        self,
        client,
        index_name: str,
        asset_name: str,
        context: OutputContext,
    ):
        """Create index with mappings if it doesn't exist."""
        if not client.indices.exists(index=index_name):
            index_body = {}

            if asset_name in self.custom_mappings:
                index_body["mappings"] = self.custom_mappings[asset_name]
                context.log.info(f"Creating index '{index_name}' with custom mapping")
            else:
                context.log.info(f"Creating index '{index_name}' with dynamic mapping")

            client.indices.create(index=index_name, body=index_body)

    def handle_output(self, context: OutputContext, obj: pd.DataFrame | pl.DataFrame):
        """Write DataFrame to OpenSearch using bulk API."""
        import time
        import polars as pl

        client = self._get_client()
        index_name = self._get_index_name(context)
        asset_name = context.asset_key.path[-1]

        self._create_index_if_not_exists(client, index_name, asset_name, context)

        # Convert to records
        if isinstance(obj, pl.DataFrame):
            records = obj.to_dicts()
        else:  # pandas
            records = cast(list[dict[str, Any]], obj.to_dict("records"))

        context.log.info(f"Indexing {len(records):,} documents to '{index_name}'")

        # Bulk index
        from opensearchpy.helpers import bulk

        start_time = time.perf_counter()

        def generate_actions():
            for record in records:
                action = {
                    "_index": index_name,
                    "_source": record,
                }
                if "id" in record:
                    action["_id"] = record["id"]
                elif "_id" in record:
                    action["_id"] = record["_id"]
                yield action

        success_count, failed_items = bulk(
            client,
            generate_actions(),
            chunk_size=self.bulk_size,
            raise_on_error=False,
            raise_on_exception=False,
        )

        elapsed_ms = (time.perf_counter() - start_time) * 1000

        # Refresh index
        client.indices.refresh(index=index_name)

        if failed_items:
            error_sample = failed_items[:5]
            context.log.warning(
                f"Failed to index {len(failed_items)} documents. Sample errors: {error_sample}"
            )

        context.add_output_metadata(
            {
                "index_name": index_name,
                "document_count": success_count,
                "failed_count": len(failed_items) if failed_items else 0,
                "bulk_size": self.bulk_size,
                "index_time_ms": round(elapsed_ms, 2),
                "opensearch_host": self.hosts[0],
            }
        )

        context.log.info(
            f"Successfully indexed {success_count:,} documents to '{index_name}' "
            f"in {elapsed_ms:.0f}ms"
        )

    def load_input(self, context: InputContext) -> pl.DataFrame:
        """Load DataFrame from OpenSearch index."""
        import polars as pl

        client = self._get_client()
        index_name = self._get_index_name(context)

        if not client.indices.exists(index=index_name):
            raise ValueError(
                f"Index '{index_name}' does not exist. Materialize the upstream asset first."
            )

        count_response = client.count(index=index_name)
        doc_count = count_response["count"]

        context.log.info(f"Loading {doc_count:,} documents from '{index_name}'")

        if doc_count > 10000:
            context.log.warning(
                f"Large index ({doc_count:,} docs). Consider using scan helper for better performance."
            )

        response = client.search(
            index=index_name,
            size=min(doc_count, 10000),
            body={"query": {"match_all": {}}},
        )

        records = [hit["_source"] for hit in response["hits"]["hits"]]

        if records:
            df = pl.DataFrame(records)
        else:
            df = pl.DataFrame()

        context.log.info(f"Loaded {len(df):,} records from '{index_name}'")

        return df


__all__ = ["JSONIOManager", "ElasticsearchIOManager", "OpenSearchIOManager"]
