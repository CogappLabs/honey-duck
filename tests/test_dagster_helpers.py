"""Tests for cogapp_libs.dagster helpers."""

from unittest.mock import MagicMock

import pandas as pd

from cogapp_libs.dagster import read_table, write_json_output
from cogapp_libs.processors.duckdb import configure


class TestReadTable:
    """Tests for read_table helper."""

    def test_read_table_from_main_schema(self, tmp_path) -> None:
        """Test reading a table from the main schema."""
        import duckdb

        # Create test database with a table
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE test_data (id INT, name VARCHAR)")
        conn.execute("INSERT INTO test_data VALUES (1, 'Alice'), (2, 'Bob')")
        conn.close()

        # Configure and read
        configure(db_path=str(db_path), read_only=True)
        result = read_table("test_data")

        assert len(result) == 2
        assert "name" in result.columns
        assert result.iloc[0]["name"] == "Alice"

    def test_read_table_from_custom_schema(self, tmp_path) -> None:
        """Test reading a table from a custom schema."""
        import duckdb

        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE SCHEMA raw")
        conn.execute("CREATE TABLE raw.sales (id INT, amount DECIMAL)")
        conn.execute("INSERT INTO raw.sales VALUES (1, 100.50), (2, 200.75)")
        conn.close()

        configure(db_path=str(db_path), read_only=True)
        result = read_table("sales", schema="raw")

        assert len(result) == 2
        assert float(result.iloc[0]["amount"]) == 100.50


class TestWriteJsonOutput:
    """Tests for write_json_output helper."""

    def test_writes_json_file(self, tmp_path) -> None:
        """Test that JSON file is written correctly."""
        output_path = tmp_path / "output" / "test.json"
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["A", "B", "C"],
                "value": [10.5, 20.5, 30.5],
            }
        )

        # Mock Dagster context
        mock_context = MagicMock()

        write_json_output(df, output_path, mock_context)

        # Check file was created
        assert output_path.exists()

        # Check content
        import json

        with open(output_path) as f:
            data = json.load(f)
        assert len(data) == 3
        assert data[0]["name"] == "A"

    def test_creates_parent_directories(self, tmp_path) -> None:
        """Test that parent directories are created if needed."""
        output_path = tmp_path / "deep" / "nested" / "path" / "output.json"
        df = pd.DataFrame({"x": [1, 2]})
        mock_context = MagicMock()

        write_json_output(df, output_path, mock_context)

        assert output_path.exists()

    def test_adds_metadata_to_context(self, tmp_path) -> None:
        """Test that metadata is added to context."""
        output_path = tmp_path / "output.json"
        df = pd.DataFrame({"id": [1, 2, 3, 4, 5]})
        mock_context = MagicMock()

        write_json_output(df, output_path, mock_context)

        # Verify add_output_metadata was called
        mock_context.add_output_metadata.assert_called_once()
        call_args = mock_context.add_output_metadata.call_args[0][0]

        assert "record_count" in call_args
        assert call_args["record_count"] == 5

    def test_extra_metadata_included(self, tmp_path) -> None:
        """Test that extra metadata is included."""
        output_path = tmp_path / "output.json"
        df = pd.DataFrame({"id": [1]})
        mock_context = MagicMock()

        write_json_output(
            df,
            output_path,
            mock_context,
            extra_metadata={"custom_key": "custom_value", "count": 42},
        )

        call_args = mock_context.add_output_metadata.call_args[0][0]
        assert call_args["custom_key"] == "custom_value"
        assert call_args["count"] == 42
