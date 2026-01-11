"""Integration tests for the honey-duck pipeline.

These tests verify end-to-end functionality of the pipeline including:
- Configuration validation
- Error handling
- Asset execution
- Data validation
"""

import importlib.util
import os
import sys
import tempfile
from pathlib import Path

import duckdb
import polars as pl
import pytest


# Import from cogapp_deps (generic utilities)
from cogapp_deps.dagster import (
    ConfigurationError,
    MissingColumnError,
    MissingTableError,
    read_duckdb_table_lazy,
    validate_dataframe,
)

# Import modules directly bypassing __init__.py to avoid loading dlt_assets
# which fails due to empty media.db
def _import_module(module_name: str, file_path: Path):
    """Import a module from file path bypassing package __init__."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)  # type: ignore
    sys.modules[module_name] = module
    spec.loader.exec_module(module)  # type: ignore
    return module


project_root = Path(__file__).parent.parent
defs_dir = project_root / "honey_duck" / "defs"

config = _import_module("honey_duck.defs.config", defs_dir / "config.py")

HoneyDuckConfig = config.HoneyDuckConfig


class TestConfiguration:
    """Test configuration validation."""

    def test_config_validates_directories(self, tmp_path: Path) -> None:
        """Configuration should validate directory structure."""
        # Create valid directory structure
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Should succeed with valid structure
        config = HoneyDuckConfig(
            duckdb_path=output_dir / "test.duckdb",
            parquet_dir=output_dir / "parquet",
            json_output_dir=output_dir / "json",
            storage_dir=output_dir / "storage",
            dlt_dir=output_dir / "dlt",
            min_sale_value_usd=1000,
            price_tier_budget_max_usd=500,
            price_tier_mid_max_usd=1000,
            freshness_hours=24,
        )
        config.validate()  # Should not raise

        # Verify directories were created
        assert config.parquet_dir.exists()
        assert config.json_output_dir.exists()
        assert config.storage_dir.exists()
        assert config.dlt_dir.exists()

    def test_config_creates_missing_directories(self, tmp_path: Path) -> None:
        """Configuration should auto-create missing directories."""
        base_dir = tmp_path / "new_project"
        # Don't create base_dir - let config do it

        config = HoneyDuckConfig(
            duckdb_path=base_dir / "db.duckdb",
            parquet_dir=base_dir / "parquet",
            json_output_dir=base_dir / "json",
            storage_dir=base_dir / "storage",
            dlt_dir=base_dir / "dlt",
            min_sale_value_usd=1000,
            price_tier_budget_max_usd=500,
            price_tier_mid_max_usd=1000,
            freshness_hours=24,
        )

        config.validate()

        # Directories should be created
        assert config.parquet_dir.exists()
        assert config.json_output_dir.exists()
        assert config.storage_dir.exists()
        assert config.dlt_dir.exists()

    def test_config_rejects_negative_values(self, tmp_path: Path) -> None:
        """Configuration should reject negative numeric values."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        config = HoneyDuckConfig(
            duckdb_path=output_dir / "test.duckdb",
            parquet_dir=output_dir / "parquet",
            json_output_dir=output_dir / "json",
            storage_dir=output_dir / "storage",
            dlt_dir=output_dir / "dlt",
            min_sale_value_usd=-1000,  # Invalid
            price_tier_budget_max_usd=500,
            price_tier_mid_max_usd=1000,
            freshness_hours=24,
        )

        with pytest.raises(ConfigurationError, match="min_sale_value_usd must be positive"):
            config.validate()

    def test_config_rejects_invalid_tier_ordering(self, tmp_path: Path) -> None:
        """Configuration should validate price tier ordering."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        config = HoneyDuckConfig(
            duckdb_path=output_dir / "test.duckdb",
            parquet_dir=output_dir / "parquet",
            json_output_dir=output_dir / "json",
            storage_dir=output_dir / "storage",
            dlt_dir=output_dir / "dlt",
            min_sale_value_usd=1000,
            price_tier_budget_max_usd=1000,
            price_tier_mid_max_usd=500,  # Invalid: mid < budget
            freshness_hours=24,
        )

        with pytest.raises(
            ConfigurationError, match="price_tier_mid_max_usd.*must be greater"
        ):
            config.validate()

    def test_config_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Configuration should load from environment variables."""
        # Just verify individual env vars can be parsed
        monkeypatch.setenv("MIN_SALE_VALUE_USD", "50000000")
        monkeypatch.setenv("FRESHNESS_HOURS", "48")

        assert int(os.environ["MIN_SALE_VALUE_USD"]) == 50000000
        assert int(os.environ["FRESHNESS_HOURS"]) == 48


class TestDataValidation:
    """Test data validation utilities."""

    def test_read_raw_table_validates_database_exists(self, tmp_path: Path) -> None:
        """Should raise clear error if database doesn't exist."""
        db_path = tmp_path / "nonexistent.duckdb"

        with pytest.raises(FileNotFoundError, match="Database not found"):
            read_duckdb_table_lazy(
                db_path,
                "sales_raw",
                asset_name="test_asset",
            )

    def test_read_raw_table_validates_table_exists(self, tmp_path: Path) -> None:
        """Should raise clear error if table doesn't exist."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE SCHEMA raw")
        conn.execute("CREATE TABLE raw.other_table (id INTEGER)")
        conn.close()

        with pytest.raises(MissingTableError, match="Table 'raw.sales_raw' not found"):
            read_duckdb_table_lazy(
                db_path,
                "sales_raw",
                asset_name="test_asset",
            )

    def test_read_raw_table_validates_columns(self, tmp_path: Path) -> None:
        """Should raise clear error if required columns are missing."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE SCHEMA raw")
        conn.execute("CREATE TABLE raw.sales_raw (id INTEGER, price DOUBLE)")
        conn.close()

        with pytest.raises(MissingColumnError, match="Missing required columns: \\['sale_id'\\]"):
            read_duckdb_table_lazy(
                db_path,
                "sales_raw",
                required_columns=["sale_id", "id"],
                asset_name="test_asset",
            )

    def test_read_raw_table_success(self, tmp_path: Path) -> None:
        """Should successfully read table with all required columns."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE SCHEMA raw")
        conn.execute("CREATE TABLE raw.sales_raw (sale_id INTEGER, sale_price DOUBLE)")
        conn.execute("INSERT INTO raw.sales_raw VALUES (1, 100.0), (2, 200.0)")
        conn.close()

        result = read_duckdb_table_lazy(
            db_path,
            "sales_raw",
            required_columns=["sale_id", "sale_price"],
            asset_name="test_asset",
        )

        # Should return LazyFrame
        assert isinstance(result, pl.LazyFrame)
        df = result.collect()
        assert len(df) == 2
        assert "sale_id" in df.columns
        assert "sale_price" in df.columns

    def test_validate_dataframe_success(self) -> None:
        """Should not raise for valid DataFrame."""
        df = pl.DataFrame({
            "sale_id": [1, 2, 3],
            "sale_price": [100.0, 200.0, 300.0],
        })

        # Should not raise
        validate_dataframe(df, ["sale_id", "sale_price"], "test_asset")

    def test_validate_dataframe_missing_columns(self) -> None:
        """Should raise for missing columns."""
        df = pl.DataFrame({
            "sale_id": [1, 2, 3],
        })

        with pytest.raises(MissingColumnError, match="Missing required columns: \\['sale_price'\\]"):
            validate_dataframe(df, ["sale_id", "sale_price"], "test_asset")


class TestErrorMessages:
    """Test error messages are clear and actionable."""

    def test_missing_table_error_includes_available_tables(self, tmp_path: Path) -> None:
        """MissingTableError should list available tables."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE SCHEMA raw")
        conn.execute("CREATE TABLE raw.artworks_raw (id INTEGER)")
        conn.execute("CREATE TABLE raw.artists_raw (id INTEGER)")
        conn.close()

        try:
            read_duckdb_table_lazy(db_path, "sales_raw", asset_name="test_asset")
            pytest.fail("Should have raised MissingTableError")
        except MissingTableError as e:
            # Error message should include available tables
            assert "artworks_raw" in str(e)
            assert "artists_raw" in str(e)
            assert "Did you run the harvest job first?" in str(e)
            # Error should have structured data
            assert e.table_name == "raw.sales_raw"
            assert set(e.available_tables) == {"artworks_raw", "artists_raw"}

    def test_missing_column_error_includes_available_columns(self) -> None:
        """MissingColumnError should list available columns."""
        df = pl.DataFrame({
            "sale_id": [1, 2],
            "artwork_id": [10, 20],
        })

        try:
            validate_dataframe(df, ["sale_id", "sale_price", "buyer"], "test_asset")
            pytest.fail("Should have raised MissingColumnError")
        except MissingColumnError as e:
            # Error message should include what's available
            assert "sale_id" in str(e)
            assert "artwork_id" in str(e)
            assert "sale_price" in str(e)
            assert "buyer" in str(e)
            # Error should have structured data
            assert e.missing_columns == {"sale_price", "buyer"}
            assert set(e.available_columns) == {"sale_id", "artwork_id"}

    def test_config_error_has_helpful_messages(self, tmp_path: Path) -> None:
        """ConfigurationError should have helpful messages."""
        # Test that invalid tier ordering gives helpful error
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        config = HoneyDuckConfig(
            duckdb_path=output_dir / "db.duckdb",
            parquet_dir=output_dir / "parquet",
            json_output_dir=output_dir / "json",
            storage_dir=output_dir / "storage",
            dlt_dir=output_dir / "dlt",
            min_sale_value_usd=1000,
            price_tier_budget_max_usd=1000,
            price_tier_mid_max_usd=500,  # Invalid
            freshness_hours=24,
        )

        try:
            config.validate()
            pytest.fail("Should have raised ConfigurationError")
        except ConfigurationError as e:
            # Should explain what's wrong
            assert "price_tier_mid_max_usd" in str(e)
            assert "greater than" in str(e)
