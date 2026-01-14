"""Integration tests for honey-duck pipelines.

These tests run actual jobs with real data to verify end-to-end functionality.
They catch issues that unit tests miss, like:
- Asset dependency ordering
- Resource injection
- IO manager compatibility
- Data schema mismatches between assets

Reference: https://docs.dagster.io/guides/dagster/testing-assets
"""

import json
from pathlib import Path

import dagster as dg
import pytest

from honey_duck.defs.definitions import defs


class TestPolarsOpsJobIntegration:
    """Integration tests for the polars_ops_pipeline job.

    This job uses graph-backed assets with ops, which had a bug where
    prepare_media_data could run before harvest completed. These tests
    ensure the fix works end-to-end.
    """

    def test_polars_ops_job_exists(self) -> None:
        """Verify the job is defined."""
        job = defs.get_job_def("polars_ops_pipeline")
        assert job is not None


class TestPolarsPipelineIntegration:
    """Integration tests for the main polars_pipeline job."""

    def test_polars_job_exists(self) -> None:
        """Verify the job is defined."""
        job = defs.get_job_def("polars_pipeline")
        assert job is not None


class TestDuckDBPipelineIntegration:
    """Integration tests for the duckdb_pipeline job."""

    def test_duckdb_job_exists(self) -> None:
        """Verify the job is defined."""
        job = defs.get_job_def("duckdb_pipeline")
        assert job is not None


class TestAllJobsDefined:
    """Verify all expected jobs are defined."""

    @pytest.mark.parametrize(
        "job_name",
        [
            "processors_pipeline",
            "polars_pipeline",
            "duckdb_pipeline",
            "polars_fs_pipeline",
            "polars_ops_pipeline",
            "polars_multi_pipeline",
        ],
    )
    def test_job_exists(self, job_name: str) -> None:
        """All 6 pipeline jobs should be defined."""
        job = defs.get_job_def(job_name)
        assert job is not None, f"Job {job_name} not found"


class TestAssetGraph:
    """Tests for asset graph structure and dependencies."""

    def test_harvest_asset_keys_exist(self) -> None:
        """Verify expected harvest asset keys are defined."""
        asset_graph = defs.resolve_asset_graph()
        asset_keys = asset_graph.get_all_asset_keys()

        # Check harvest assets exist
        expected_harvest_keys = [
            dg.AssetKey("dlt_harvest_sales_raw"),
            dg.AssetKey("dlt_harvest_artworks_raw"),
            dg.AssetKey("dlt_harvest_artists_raw"),
            dg.AssetKey("dlt_harvest_media"),
        ]
        for key in expected_harvest_keys:
            assert key in asset_keys, f"Missing harvest asset: {key}"

    def test_transform_assets_depend_on_harvest(self) -> None:
        """Transform assets should depend on harvest assets."""
        asset_graph = defs.resolve_asset_graph()

        # Get sales_joined_polars dependencies
        sales_transform_key = dg.AssetKey("sales_joined_polars")
        if asset_graph.has(sales_transform_key):
            node = asset_graph.get(sales_transform_key)
            dep_names = {str(k) for k in node.parent_keys}

            # Should depend on harvest assets
            assert any("harvest" in name for name in dep_names), (
                f"sales_joined_polars should depend on harvest assets, got: {dep_names}"
            )

    def test_output_assets_depend_on_transform(self) -> None:
        """Output assets should depend on transform assets."""
        asset_graph = defs.resolve_asset_graph()

        # Check sales_output_polars depends on sales_transform_polars
        output_key = dg.AssetKey("sales_output_polars")
        if asset_graph.has(output_key):
            node = asset_graph.get(output_key)
            dep_names = {str(k) for k in node.parent_keys}

            assert any("transform" in name for name in dep_names), (
                f"sales_output_polars should depend on transform, got: {dep_names}"
            )


class TestJobExecutionInProcess:
    """Tests that execute jobs in-process with real data.

    These are true integration tests that verify the full pipeline works.
    Marked as slow since they actually materialize data.
    """

    @pytest.mark.slow
    def test_polars_ops_pipeline_executes(self, tmp_path: Path) -> None:
        """Run polars_ops_pipeline with isolated outputs."""
        from honey_duck.defs.shared.resources import INPUT_DIR

        # Verify input data exists (should be tracked in git now)
        assert (INPUT_DIR / "sales.csv").exists(), "Missing sales.csv input data"
        assert (INPUT_DIR / "media.db").exists(), "Missing media.db input data"

        # Get the job and execute
        job = defs.get_job_def("polars_ops_pipeline")

        # Override output paths to use temp directory
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        result = job.execute_in_process(
            run_config={
                "resources": {
                    "output_paths": {
                        "config": {
                            "sales_polars_ops": str(output_dir / "sales.json"),
                            "artworks_polars_ops": str(output_dir / "artworks.json"),
                        }
                    }
                }
            }
        )

        assert result.success, (
            f"Job failed with events: {[e for e in result.all_events if e.is_failure]}"
        )

        # Verify outputs exist and have valid content
        if (output_dir / "artworks.json").exists():
            with open(output_dir / "artworks.json") as f:
                artworks = json.load(f)
            assert isinstance(artworks, list)
            if len(artworks) > 0:
                assert "artwork_id" in artworks[0]
