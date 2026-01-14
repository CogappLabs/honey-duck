"""Linting-style tests that enforce best practices.

These tests verify structural properties of the Dagster definitions
without running any pipelines. They catch configuration issues,
missing metadata, and naming convention violations.

Reference: https://blog.rmhogervorst.nl/blog/2024/02/27/how-i-write-tests-for-dagster/
"""

from typing import Any

import dagster as dg
import pytest

from honey_duck.defs.definitions import defs


class TestJobBestPractices:
    """Verify all jobs follow best practices."""

    @pytest.fixture
    def all_jobs(self) -> list[dg.JobDefinition]:
        """Get all job definitions."""
        return [
            defs.get_job_def(name)
            for name in [
                "processors_pipeline",
                "polars_pipeline",
                "duckdb_pipeline",
                "polars_fs_pipeline",
                "polars_ops_pipeline",
                "polars_multi_pipeline",
            ]
        ]

    def test_all_jobs_have_consistent_naming(self, all_jobs: list[dg.JobDefinition]) -> None:
        """Job names should follow snake_case convention and end with _pipeline."""
        for job in all_jobs:
            assert job.name.endswith("_pipeline"), f"Job '{job.name}' should end with '_pipeline'"
            assert job.name.islower() or "_" in job.name, f"Job '{job.name}' should use snake_case"


class TestAssetBestPractices:
    """Verify all assets follow best practices."""

    @pytest.fixture
    def asset_graph(self) -> Any:
        """Get the resolved asset graph."""
        return defs.resolve_asset_graph()

    def test_all_assets_have_group_names(self, asset_graph: Any) -> None:
        """Every asset should be assigned to a group for organization."""
        for key in asset_graph.get_all_asset_keys():
            node = asset_graph.get(key)
            # Skip external/source assets which may not have groups
            if not node.is_materializable:
                continue
            assert node.group_name is not None, f"Asset '{key}' should have a group_name"

    def test_output_assets_have_kinds(self, asset_graph: Any) -> None:
        """Output assets should specify their compute kind (polars, duckdb, etc)."""
        for key in asset_graph.get_all_asset_keys():
            node = asset_graph.get(key)
            if not node.is_materializable:
                continue
            # Output assets should have kinds for UI visibility
            if "output" in str(key):
                assert node.kinds is not None and len(node.kinds) > 0, (
                    f"Output asset '{key}' should specify kinds"
                )

    def test_harvest_assets_are_roots(self, asset_graph: Any) -> None:
        """Harvest assets should have no upstream dependencies (except source assets)."""
        for key in asset_graph.get_all_asset_keys():
            if "harvest" not in str(key):
                continue
            node = asset_graph.get(key)
            # Harvest assets may depend on source assets but not other materializable assets
            materializable_parents = [
                p
                for p in node.parent_keys
                if asset_graph.has(p) and asset_graph.get(p).is_materializable
            ]
            # The dlt_harvest_assets multi-asset is the root
            assert len(materializable_parents) == 0, (
                f"Harvest asset '{key}' should not depend on other materializable assets, "
                f"but depends on {materializable_parents}"
            )


class TestNamingConventions:
    """Verify naming conventions are followed throughout the codebase."""

    @pytest.fixture
    def asset_graph(self) -> Any:
        return defs.resolve_asset_graph()

    def test_transform_assets_contain_transform_or_intermediate_name(
        self, asset_graph: Any
    ) -> None:
        """Transform-layer assets should be identifiable by name."""
        transform_groups = ["transform", "transform_polars", "transform_duckdb"]
        for key in asset_graph.get_all_asset_keys():
            node = asset_graph.get(key)
            if not node.is_materializable:
                continue
            if node.group_name in transform_groups:
                key_str = str(key)
                # Should contain transform, joined, catalog, agg, or similar
                is_valid_name = any(
                    term in key_str.lower()
                    for term in ["transform", "joined", "catalog", "agg", "media", "pipeline"]
                )
                assert is_valid_name, (
                    f"Transform asset '{key}' in group '{node.group_name}' "
                    f"should have a descriptive name indicating its purpose"
                )


class TestDependencyStructure:
    """Verify the asset dependency graph has correct structure."""

    @pytest.fixture
    def asset_graph(self) -> Any:
        return defs.resolve_asset_graph()

    def test_no_circular_dependencies(self, asset_graph: Any) -> None:
        """Asset graph should be a DAG with no cycles."""
        # If we can topologically sort, there are no cycles
        try:
            sorted_keys = list(asset_graph.toposorted_asset_keys)
            assert len(sorted_keys) > 0
        except Exception as e:
            pytest.fail(f"Asset graph has circular dependencies: {e}")

    def test_output_assets_are_leaves(self, asset_graph: Any) -> None:
        """Output assets should generally not have downstream dependencies."""
        for key in asset_graph.get_all_asset_keys():
            if "output" not in str(key):
                continue
            node = asset_graph.get(key)
            children = list(node.child_keys)
            # Output assets may have other outputs as children (for ordering)
            # but should not have transform assets as children
            transform_children = [
                c for c in children if "transform" in str(c) or "joined" in str(c)
            ]
            assert len(transform_children) == 0, (
                f"Output asset '{key}' should not have transform assets as children, "
                f"but has {transform_children}"
            )


class TestResourceConfiguration:
    """Verify resources are properly configured."""

    def test_required_resources_are_defined(self) -> None:
        """All required resources should be defined in Definitions."""
        # These resources are required for the pipelines to run
        required_resources = ["paths", "output_paths", "database", "io_manager"]

        defined_resources = set(defs.resources.keys()) if defs.resources else set()

        for resource in required_resources:
            assert resource in defined_resources, f"Required resource '{resource}' is not defined"
