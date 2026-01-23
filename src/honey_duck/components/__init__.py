"""Component registry for honey-duck project.

This module registers component types available for use with `dg component add`.

Components from cogapp_libs are re-exported here for convenience, allowing
them to be used in YAML-based component configurations.

Usage:
    dg component add cogapp_libs.dagster.components.ElasticsearchIOManagerComponent

Or in component.yaml:
    type: cogapp_libs.dagster.components.ElasticsearchIOManagerComponent
    params:
      hosts: ["http://localhost:9200"]
"""

# Re-export components from cogapp_libs for discovery
from cogapp_libs.dagster.components import ElasticsearchIOManagerComponent

__all__ = ["ElasticsearchIOManagerComponent"]
