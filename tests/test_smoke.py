"""Smoke tests for honey-duck pipeline structure.

Tests that verify asset definitions, dependencies, and basic execution
without requiring external services or full data materialization.

These tests use mock resources (NoOpIOManager, empty harvest data) to:
- Verify asset graph structure is valid
- Test asset logic with empty/minimal data
- Ensure imports and dependencies are correct
- Run quickly in CI without external services
"""

import polars as pl
from dagster import materialize

from honey_duck.defs.assets_polars import (
    artworks_catalog_polars,
    sales_joined_polars,
)


class TestAssetImports:
    """Test that all asset modules import correctly."""

    def test_assets_polars_imports(self) -> None:
        """Verify assets_polars module imports without errors."""
        from honey_duck.defs import assets_polars

        assert hasattr(assets_polars, "sales_joined_polars")
        assert hasattr(assets_polars, "sales_transform_polars")
        assert hasattr(assets_polars, "artworks_transform_polars")

    def test_assets_original_imports(self) -> None:
        """Verify assets module imports without errors."""
        from honey_duck.defs import assets

        assert hasattr(assets, "sales_transform")
        assert hasattr(assets, "artworks_transform")

    def test_assets_duckdb_imports(self) -> None:
        """Verify assets_duckdb module imports without errors."""
        from honey_duck.defs import assets_duckdb

        assert hasattr(assets_duckdb, "sales_transform_duckdb")
        assert hasattr(assets_duckdb, "artworks_transform_duckdb")

    def test_definitions_imports(self) -> None:
        """Verify definitions module imports and creates valid Definitions."""
        from honey_duck.defs.definitions import defs

        assert defs is not None
        # Use resolve_asset_graph() and get_all_asset_keys() which is the current API
        asset_graph = defs.resolve_asset_graph()
        assert len(asset_graph.get_all_asset_keys()) > 0


class TestPolarsAssetsSmokeWithEmptyData:
    """Smoke tests for Polars pipeline with empty data.

    These tests verify that the first-level assets (that read directly
    from harvest with no other asset dependencies) can execute with
    empty inputs and produce valid (empty) outputs.

    Note: Assets with inter-asset dependencies require proper IO managers
    that preserve schema. Use integration tests for full pipeline testing.
    """

    def test_sales_joined_polars_empty(self, mock_resources: dict) -> None:
        """Sales join asset handles empty harvest data."""
        result = materialize(
            assets=[sales_joined_polars],
            resources=mock_resources,
        )
        assert result.success

    def test_artworks_catalog_polars_empty(self, mock_resources: dict) -> None:
        """Artworks catalog asset handles empty harvest data."""
        result = materialize(
            assets=[artworks_catalog_polars],
            resources=mock_resources,
        )
        assert result.success


class TestResourceConfiguration:
    """Test resource configuration and injection."""

    def test_paths_resource_defaults(self) -> None:
        """PathsResource has sensible defaults."""
        from honey_duck.defs.resources import PathsResource

        paths = PathsResource()
        assert "data" in paths.harvest_dir
        assert "data" in paths.output_dir

    def test_output_paths_resource_all_variants(self) -> None:
        """OutputPathsResource includes all implementation variants."""
        from honey_duck.defs.resources import OutputPathsResource

        output_paths = OutputPathsResource()
        # Check all 6 implementations have output paths
        assert output_paths.sales  # original
        assert output_paths.sales_polars
        assert output_paths.sales_duckdb
        assert output_paths.sales_polars_fs
        assert output_paths.sales_polars_ops
        assert output_paths.sales_polars_multi


class TestSchemaValidation:
    """Test Pandera schema definitions."""

    def test_sales_schema_valid(self, sample_sales_df: pl.DataFrame) -> None:
        """SalesTransformSchema validates correct data."""
        import polars as pl

        from honey_duck.defs.schemas import SalesTransformSchema

        # Add required columns that the fixture might be missing
        df = sample_sales_df.with_columns(
            (pl.col("sale_price_usd") - pl.col("list_price_usd")).alias("price_diff"),
            (
                (pl.col("sale_price_usd") - pl.col("list_price_usd"))
                / pl.col("list_price_usd")
                * 100
            ).alias("pct_change"),
        )

        # Should not raise
        SalesTransformSchema.validate(df)

    def test_artworks_schema_valid(self, sample_artworks_df: pl.DataFrame) -> None:
        """ArtworksTransformSchema validates correct data."""
        from honey_duck.defs.schemas import ArtworksTransformSchema

        # Should not raise
        ArtworksTransformSchema.validate(sample_artworks_df)


class TestIOManagerRegistry:
    """Test IO manager implementations."""

    def test_json_io_manager_exists(self) -> None:
        """JSONIOManager is properly exported."""
        from cogapp_deps.dagster.io_managers import JSONIOManager

        assert JSONIOManager is not None

    def test_elasticsearch_io_manager_exists(self) -> None:
        """ElasticsearchIOManager is properly exported."""
        from cogapp_deps.dagster.io_managers import ElasticsearchIOManager

        assert ElasticsearchIOManager is not None

    def test_opensearch_io_manager_exists(self) -> None:
        """OpenSearchIOManager is properly exported."""
        from cogapp_deps.dagster.io_managers import OpenSearchIOManager

        assert OpenSearchIOManager is not None


class TestProcessorImports:
    """Test that processor classes import correctly."""

    def test_polars_processors(self) -> None:
        """Polars processors import correctly."""
        from cogapp_deps.processors.polars import (
            PolarsFilterProcessor,
            PolarsStringProcessor,
        )

        assert PolarsFilterProcessor is not None
        assert PolarsStringProcessor is not None

    def test_duckdb_processors(self) -> None:
        """DuckDB processors import correctly."""
        from cogapp_deps.processors.duckdb import (
            DuckDBAggregateProcessor,
            DuckDBJoinProcessor,
            DuckDBQueryProcessor,
            DuckDBWindowProcessor,
        )

        assert DuckDBQueryProcessor is not None
        assert DuckDBJoinProcessor is not None
        assert DuckDBWindowProcessor is not None
        assert DuckDBAggregateProcessor is not None

    def test_chain_processor(self) -> None:
        """Chain processor imports correctly."""
        from cogapp_deps.processors import Chain

        assert Chain is not None
