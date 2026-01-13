"""Elasticsearch IO Manager Component for Dagster.

Provides a YAML-configurable component for bulk indexing DataFrames to Elasticsearch.

Usage in component.yaml:
    type: cogapp_deps.dagster.components.ElasticsearchIOManagerComponent
    params:
      hosts:
        - ${ELASTICSEARCH_HOST}
      index_prefix: "my_project_"
      api_key: ${ELASTICSEARCH_API_KEY}
      bulk_size: 1000
"""

from __future__ import annotations

import dagster as dg


class ElasticsearchIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Elasticsearch IO Manager for bulk indexing DataFrames.

    Memory-efficient streaming via generator-based bulk indexing.
    Supports both Polars and Pandas DataFrames.

    Attributes:
        hosts: List of Elasticsearch host URLs
        index_prefix: Prefix for index names (default: "dagster_")
        api_key: API key for authentication (optional)
        bulk_size: Number of documents per bulk request (default: 500)
        verify_certs: Whether to verify SSL certificates (default: True)
        resource_key: Key for the IO manager in resources dict (default: "elasticsearch_io_manager")
    """

    hosts: list[str]
    index_prefix: str = "dagster_"
    api_key: str | None = None
    bulk_size: int = 500
    verify_certs: bool = True
    resource_key: str = "elasticsearch_io_manager"

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build Dagster Definitions with the Elasticsearch IO manager.

        Args:
            context: Component load context from Dagster

        Returns:
            Definitions object with the IO manager resource
        """
        from cogapp_deps.dagster.io_managers import ElasticsearchIOManager

        io_manager = ElasticsearchIOManager(
            hosts=self.hosts,
            index_prefix=self.index_prefix,
            api_key=self.api_key,
            bulk_size=self.bulk_size,
            verify_certs=self.verify_certs,
        )

        return dg.Definitions(resources={self.resource_key: io_manager})

    @classmethod
    def get_spec(cls) -> dg.ComponentTypeSpec:
        """Return component specification with metadata.

        Returns:
            ComponentTypeSpec with owners and tags
        """
        return dg.ComponentTypeSpec(
            owners=["team:data-platform"],
            tags=["elasticsearch", "io-manager"],
        )
