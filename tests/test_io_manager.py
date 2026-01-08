"""Tests for DuckDBPandasPolarsIOManager."""

import pandas as pd
import polars as pl
import pytest

from cogapp_deps.dagster import DuckDBPandasPolarsIOManager


class TestDuckDBPandasPolarsIOManager:
    """Tests for the combined Pandas/Polars IO manager."""

    def test_type_handlers_include_polars(self) -> None:
        """Verify the IO manager has both Pandas and Polars type handlers."""
        io_manager = DuckDBPandasPolarsIOManager(
            database=":memory:",
            schema="main",
        )

        # Get type handlers
        handlers = io_manager.type_handlers()
        handler_types = [type(h).__name__ for h in handlers]

        assert "DuckDBPandasTypeHandler" in handler_types
        assert "DuckDBPolarsTypeHandler" in handler_types

    def test_type_handlers_includes_relation(self) -> None:
        """Verify the IO manager includes DuckDB relation handler."""
        io_manager = DuckDBPandasPolarsIOManager(
            database=":memory:",
            schema="main",
        )

        handlers = io_manager.type_handlers()
        handler_types = [type(h).__name__ for h in handlers]

        # Should have all three handlers
        assert "DuckDBRelationTypeHandler" in handler_types
        assert "DuckDBPandasTypeHandler" in handler_types
        assert "DuckDBPolarsTypeHandler" in handler_types
        assert len(handlers) == 3


class TestWriteJsonOutputPolars:
    """Tests for write_json_output with Polars DataFrames."""

    def test_writes_polars_dataframe(self, tmp_path) -> None:
        """Test that Polars DataFrame is written correctly to JSON."""
        from unittest.mock import MagicMock

        from cogapp_deps.dagster import write_json_output

        output_path = tmp_path / "output.json"
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "value": [10.5, 20.5, 30.5],
        })

        mock_context = MagicMock()
        write_json_output(df, output_path, mock_context)

        assert output_path.exists()

        import json

        with open(output_path) as f:
            data = json.load(f)
        assert len(data) == 3
        assert data[0]["name"] == "A"

    def test_polars_and_pandas_produce_same_json(self, tmp_path) -> None:
        """Test that Polars and Pandas produce equivalent JSON output."""
        from unittest.mock import MagicMock

        from cogapp_deps.dagster import write_json_output

        # Same data in both formats
        pandas_df = pd.DataFrame({
            "id": [1, 2],
            "name": ["X", "Y"],
        })
        polars_df = pl.DataFrame({
            "id": [1, 2],
            "name": ["X", "Y"],
        })

        pandas_path = tmp_path / "pandas.json"
        polars_path = tmp_path / "polars.json"

        write_json_output(pandas_df, pandas_path, MagicMock())
        write_json_output(polars_df, polars_path, MagicMock())

        import json

        with open(pandas_path) as f:
            pandas_data = json.load(f)
        with open(polars_path) as f:
            polars_data = json.load(f)

        assert pandas_data == polars_data
