"""Tests for Dagster Components.

Tests component instantiation, build_defs(), and integration with IO managers.
"""

from unittest.mock import MagicMock

import dagster as dg

from cogapp_libs.dagster import ElasticsearchIOManagerComponent
from cogapp_libs.dagster.io_managers import ElasticsearchIOManager


class TestElasticsearchIOManagerComponent:
    """Tests for ElasticsearchIOManagerComponent."""

    def test_component_instantiation(self) -> None:
        """Component can be instantiated with required parameters."""
        component = ElasticsearchIOManagerComponent(
            hosts=["http://localhost:9200"],
            index_prefix="test_",
        )

        assert component.hosts == ["http://localhost:9200"]
        assert component.index_prefix == "test_"
        assert component.bulk_size == 500  # default
        assert component.verify_certs is True  # default
        assert component.resource_key == "elasticsearch_io_manager"  # default

    def test_component_with_all_parameters(self) -> None:
        """Component accepts all optional parameters."""
        component = ElasticsearchIOManagerComponent(
            hosts=["http://es1:9200", "http://es2:9200"],
            index_prefix="custom_",
            api_key="test-api-key",
            bulk_size=1000,
            verify_certs=False,
            resource_key="custom_io_manager",
        )

        assert component.hosts == ["http://es1:9200", "http://es2:9200"]
        assert component.index_prefix == "custom_"
        assert component.api_key == "test-api-key"
        assert component.bulk_size == 1000
        assert component.verify_certs is False
        assert component.resource_key == "custom_io_manager"

    def test_build_defs_returns_definitions(self) -> None:
        """build_defs() returns valid Definitions with IO manager."""
        component = ElasticsearchIOManagerComponent(
            hosts=["http://localhost:9200"],
            index_prefix="test_",
        )

        # Mock the context
        context = MagicMock(spec=dg.ComponentLoadContext)

        # Build definitions
        definitions = component.build_defs(context)

        # Verify it's a Definitions object
        assert isinstance(definitions, dg.Definitions)

    def test_build_defs_contains_io_manager(self) -> None:
        """build_defs() includes ElasticsearchIOManager resource."""
        component = ElasticsearchIOManagerComponent(
            hosts=["http://localhost:9200"],
            index_prefix="test_",
            bulk_size=250,
        )

        context = MagicMock(spec=dg.ComponentLoadContext)
        definitions = component.build_defs(context)

        # Get the resources from definitions
        # The resource should be accessible via the resource_key
        resources = definitions.resources
        assert resources is not None
        assert "elasticsearch_io_manager" in resources

        # Verify the resource is an ElasticsearchIOManager
        io_manager = resources["elasticsearch_io_manager"]
        assert isinstance(io_manager, ElasticsearchIOManager)

    def test_build_defs_custom_resource_key(self) -> None:
        """build_defs() uses custom resource_key."""
        component = ElasticsearchIOManagerComponent(
            hosts=["http://localhost:9200"],
            index_prefix="test_",
            resource_key="my_custom_es",
        )

        context = MagicMock(spec=dg.ComponentLoadContext)
        definitions = component.build_defs(context)

        resources = definitions.resources
        assert resources is not None
        assert "my_custom_es" in resources
        assert "elasticsearch_io_manager" not in resources

    def test_io_manager_receives_correct_config(self) -> None:
        """IO manager receives configuration from component."""
        component = ElasticsearchIOManagerComponent(
            hosts=["http://es1:9200", "http://es2:9200"],
            index_prefix="pipeline_",
            bulk_size=750,
            verify_certs=False,
        )

        context = MagicMock(spec=dg.ComponentLoadContext)
        definitions = component.build_defs(context)

        assert definitions.resources is not None
        io_manager = definitions.resources["elasticsearch_io_manager"]
        assert isinstance(io_manager, ElasticsearchIOManager)
        assert io_manager.hosts == ["http://es1:9200", "http://es2:9200"]
        assert io_manager.index_prefix == "pipeline_"
        assert io_manager.bulk_size == 750
        assert io_manager.verify_certs is False

    def test_get_spec_returns_component_type_spec(self) -> None:
        """get_spec() returns valid ComponentTypeSpec."""
        spec = ElasticsearchIOManagerComponent.get_spec()

        assert isinstance(spec, dg.ComponentTypeSpec)
        assert "team:data-platform" in spec.owners
        assert "elasticsearch" in spec.tags
        assert "io-manager" in spec.tags

    def test_component_is_dagster_component(self) -> None:
        """Component inherits from Dagster Component base classes."""
        assert issubclass(ElasticsearchIOManagerComponent, dg.Component)
        assert issubclass(ElasticsearchIOManagerComponent, dg.Model)
        assert issubclass(ElasticsearchIOManagerComponent, dg.Resolvable)


class TestComponentImports:
    """Test that components are properly exported."""

    def test_import_from_components_module(self) -> None:
        """Component can be imported from components submodule."""
        from cogapp_libs.dagster.components import ElasticsearchIOManagerComponent as Component

        assert Component is not None

    def test_import_from_main_package(self) -> None:
        """Component can be imported from main dagster package."""
        from cogapp_libs.dagster import ElasticsearchIOManagerComponent as Component

        assert Component is not None
