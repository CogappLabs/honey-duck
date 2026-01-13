"""Component registry for honey-duck project.

This module registers component types available for use with `dg component add`.

Components from cogapp_deps are re-exported here for convenience, allowing
them to be used in YAML-based component configurations.

Usage:
    dg component add cogapp_deps.dagster.components.ElasticsearchIOManagerComponent

Or in component.yaml:
    type: cogapp_deps.dagster.components.ElasticsearchIOManagerComponent
    params:
      hosts: ["http://localhost:9200"]
"""

# Re-export components from cogapp_deps for discovery
from cogapp_deps.dagster.components import ElasticsearchIOManagerComponent

__all__ = ["ElasticsearchIOManagerComponent"]
