"""Dagster Components for reusable pipeline patterns.

Components provide YAML-configurable building blocks for Dagster projects.
"""

from cogapp_deps.dagster.components.elasticsearch import ElasticsearchIOManagerComponent

__all__ = ["ElasticsearchIOManagerComponent"]
