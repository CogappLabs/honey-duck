"""Tests for cogapp_deps.dagster.validation module.

These tests verify the validation functions that read harvest tables
and validate DataFrame schemas. Critical for catching issues like
missing data directories on fresh clones.
"""

from pathlib import Path

import dagster as dg
import polars as pl
import pytest

from cogapp_deps.dagster import (
    read_harvest_table_lazy,
    read_harvest_tables_lazy,
    read_parquet_table_lazy,
    validate_dataframe,
)
from cogapp_deps.dagster.exceptions import MissingColumnError, MissingTableError


class TestReadParquetTableLazy:
    """Tests for read_parquet_table_lazy function."""

    def test_reads_valid_parquet_table(self, temp_harvest_dir: Path) -> None:
        """Successfully reads a valid Parquet table."""
        result = read_parquet_table_lazy(
            temp_harvest_dir / "raw",
            "sales_raw",
            asset_name="test_asset",
        )
        assert isinstance(result, pl.LazyFrame)
        # Should be able to collect without error
        df = result.collect()
        assert isinstance(df, pl.DataFrame)

    def test_raises_on_missing_directory(self, tmp_path: Path) -> None:
        """Raises Failure when parquet directory doesn't exist."""
        missing_dir = tmp_path / "nonexistent"

        with pytest.raises(dg.Failure) as exc_info:
            read_parquet_table_lazy(
                missing_dir,
                "sales_raw",
                asset_name="test_asset",
            )

        assert "Parquet directory not found" in str(exc_info.value)
        assert "Did you run the harvest job first?" in str(exc_info.value)

    def test_raises_on_missing_table(self, temp_harvest_dir: Path) -> None:
        """Raises Failure when table doesn't exist, listing available tables."""
        with pytest.raises(dg.Failure) as exc_info:
            read_parquet_table_lazy(
                temp_harvest_dir / "raw",
                "nonexistent_table",
                asset_name="test_asset",
            )

        # Should mention available tables
        error_str = str(exc_info.value)
        assert "sales_raw" in error_str or "artworks_raw" in error_str

    def test_validates_required_columns(self, temp_harvest_dir: Path) -> None:
        """Raises Failure when required columns are missing."""
        with pytest.raises(dg.Failure) as exc_info:
            read_parquet_table_lazy(
                temp_harvest_dir / "raw",
                "sales_raw",
                required_columns=["sale_id", "nonexistent_column"],
                asset_name="test_asset",
            )

        assert "nonexistent_column" in str(exc_info.value)

    def test_passes_with_valid_required_columns(self, temp_harvest_dir: Path) -> None:
        """Succeeds when all required columns exist."""
        result = read_parquet_table_lazy(
            temp_harvest_dir / "raw",
            "sales_raw",
            required_columns=["sale_id", "artwork_id"],
            asset_name="test_asset",
        )
        assert isinstance(result, pl.LazyFrame)


class TestReadHarvestTableLazy:
    """Tests for read_harvest_table_lazy function."""

    def test_reads_with_default_schema(self, temp_harvest_dir: Path) -> None:
        """Reads from 'raw' schema by default."""
        result = read_harvest_table_lazy(
            temp_harvest_dir,
            "sales_raw",
            asset_name="test_asset",
        )
        assert isinstance(result, pl.LazyFrame)

    def test_reads_with_custom_schema(self, tmp_path: Path) -> None:
        """Reads from custom schema subdirectory."""
        # Create custom schema directory
        custom_dir = tmp_path / "harvest" / "custom_schema" / "my_table"
        custom_dir.mkdir(parents=True)
        pl.DataFrame({"id": [1, 2, 3]}).write_parquet(custom_dir / "data.parquet")

        result = read_harvest_table_lazy(
            tmp_path / "harvest",
            "my_table",
            schema="custom_schema",
            asset_name="test_asset",
        )
        df = result.collect()
        assert len(df) == 3


class TestReadHarvestTablesLazy:
    """Tests for read_harvest_tables_lazy batch function."""

    def test_reads_multiple_tables(self, temp_harvest_dir: Path) -> None:
        """Reads multiple tables in a single call."""
        tables = read_harvest_tables_lazy(
            temp_harvest_dir,
            ("sales_raw", None),
            ("artworks_raw", None),
            asset_name="test_asset",
        )

        assert "sales_raw" in tables
        assert "artworks_raw" in tables
        assert isinstance(tables["sales_raw"], pl.LazyFrame)
        assert isinstance(tables["artworks_raw"], pl.LazyFrame)

    def test_validates_columns_per_table(self, temp_harvest_dir: Path) -> None:
        """Validates required columns for each table independently."""
        with pytest.raises(dg.Failure) as exc_info:
            read_harvest_tables_lazy(
                temp_harvest_dir,
                ("sales_raw", ["sale_id"]),  # Valid
                ("artworks_raw", ["nonexistent_column"]),  # Invalid
                asset_name="test_asset",
            )

        assert "nonexistent_column" in str(exc_info.value)


class TestValidateDataframe:
    """Tests for validate_dataframe function."""

    def test_passes_with_all_required_columns(self) -> None:
        """No error when all required columns exist."""
        df = pl.DataFrame({"a": [1], "b": [2], "c": [3]})
        # Should not raise
        validate_dataframe(df, ["a", "b"], "test_asset")

    def test_raises_on_missing_columns(self) -> None:
        """Raises Failure listing missing and available columns."""
        df = pl.DataFrame({"a": [1], "b": [2]})

        with pytest.raises(dg.Failure) as exc_info:
            validate_dataframe(df, ["a", "c", "d"], "test_asset")

        error_str = str(exc_info.value)
        # Should mention missing columns
        assert "c" in error_str or "d" in error_str


class TestExceptionTypes:
    """Tests for custom exception classes."""

    def test_missing_table_error_message(self) -> None:
        """MissingTableError includes helpful context."""
        error = MissingTableError(
            asset_name="my_asset",
            table_name="missing_table",
            available_tables=["sales", "artworks"],
        )
        message = str(error)
        assert "missing_table" in message
        assert "my_asset" in message
        assert "sales" in message or "artworks" in message

    def test_missing_column_error_message(self) -> None:
        """MissingColumnError includes helpful context."""
        error = MissingColumnError(
            asset_name="my_asset",
            missing_columns={"col_a", "col_b"},
            available_columns=["id", "name", "value"],
        )
        message = str(error)
        assert "col_a" in message or "col_b" in message
        assert "my_asset" in message
