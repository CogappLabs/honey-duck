"""Tests for cogapp_deps processors."""

import pandas as pd
import pytest

from cogapp_deps.processors import Chain
from cogapp_deps.processors.duckdb import (
    DuckDBAggregateProcessor,
    DuckDBJoinProcessor,
    DuckDBQueryProcessor,
    DuckDBSQLProcessor,
    DuckDBWindowProcessor,
    configure,
)
from cogapp_deps.processors.polars import PolarsFilterProcessor, PolarsStringProcessor


# -----------------------------------------------------------------------------
# Test Data Fixtures
# -----------------------------------------------------------------------------


@pytest.fixture
def sales_df() -> pd.DataFrame:
    """Sample sales data for testing."""
    return pd.DataFrame(
        {
            "sale_id": [1, 2, 3, 4],
            "artwork_id": [101, 102, 101, 103],
            "sale_price_usd": [100000, 250000, 150000, 50000],
            "sale_date": ["2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01"],
            "buyer_country": ["USA", "UK", "USA", "France"],
        }
    )


@pytest.fixture
def artworks_df() -> pd.DataFrame:
    """Sample artworks data for testing."""
    return pd.DataFrame(
        {
            "artwork_id": [101, 102, 103],
            "title": ["Painting A", "Sculpture B", "Drawing C"],
            "artist_id": [1, 1, 2],
            "price_usd": [80000, 200000, 40000],
        }
    )


@pytest.fixture
def artists_df() -> pd.DataFrame:
    """Sample artists data for testing."""
    return pd.DataFrame(
        {
            "artist_id": [1, 2],
            "name": ["  Alice Smith  ", "Bob Jones"],
            "nationality": ["American", "British"],
        }
    )


# -----------------------------------------------------------------------------
# DuckDB Processor Tests
# -----------------------------------------------------------------------------


class TestDuckDBJoinProcessor:
    """Tests for DuckDBJoinProcessor (deprecated)."""

    def test_emits_deprecation_warning(self) -> None:
        """Test that deprecation warning is emitted."""
        with pytest.warns(DeprecationWarning, match="DuckDBJoinProcessor is deprecated"):
            DuckDBJoinProcessor(joins=[("table", "id", "id")])

    def test_join_with_dataframe_input(
        self, sales_df: pd.DataFrame, artworks_df: pd.DataFrame
    ) -> None:
        """Test joining a DataFrame with a registered table."""
        import duckdb

        conn = duckdb.connect(":memory:")
        conn.register("artworks", artworks_df)

        with pytest.warns(DeprecationWarning):
            processor = DuckDBJoinProcessor(
                joins=[("artworks", "artwork_id", "artwork_id")],
                select_cols=["a.sale_id", "a.artwork_id", "b.title"],
            )
        result = processor.process(sales_df, conn=conn)

        assert len(result) == 4
        assert "title" in result.columns
        assert result.loc[result["sale_id"] == 1, "title"].iloc[0] == "Painting A"

    def test_join_with_base_table(self) -> None:
        """Test joining from a base table in the database."""
        import duckdb

        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE sales (id INT, value INT)")
        conn.execute("INSERT INTO sales VALUES (1, 100), (2, 200)")
        conn.execute("CREATE TABLE items (id INT, name VARCHAR)")
        conn.execute("INSERT INTO items VALUES (1, 'Item A'), (2, 'Item B')")

        with pytest.warns(DeprecationWarning):
            processor = DuckDBJoinProcessor(
                joins=[("items", "id", "id")],
                select_cols=["a.id", "a.value", "b.name"],
                base_table="sales",
            )
        result = processor.process(conn=conn)

        assert len(result) == 2
        assert "name" in result.columns

    def test_raises_without_input_or_base_table(self) -> None:
        """Test that error is raised when neither df nor base_table provided."""
        with pytest.warns(DeprecationWarning):
            processor = DuckDBJoinProcessor(
                joins=[("other_table", "id", "id")],
            )
        with pytest.raises(ValueError, match="Either provide df argument"):
            processor.process()


class TestDuckDBWindowProcessor:
    """Tests for DuckDBWindowProcessor."""

    def test_window_functions(self, sales_df: pd.DataFrame) -> None:
        """Test adding window function columns."""
        processor = DuckDBWindowProcessor(
            exprs={
                "running_total": "SUM(sale_price_usd) OVER (ORDER BY sale_date)",
                "sale_rank": "ROW_NUMBER() OVER (ORDER BY sale_price_usd DESC)",
            }
        )
        result = processor.process(sales_df)

        assert "running_total" in result.columns
        assert "sale_rank" in result.columns
        assert len(result) == 4

    def test_partition_by(self, sales_df: pd.DataFrame) -> None:
        """Test window functions with PARTITION BY."""
        processor = DuckDBWindowProcessor(
            exprs={
                "artwork_sale_count": "COUNT(*) OVER (PARTITION BY artwork_id)",
            }
        )
        result = processor.process(sales_df)

        # artwork_id 101 appears twice
        assert result.loc[result["artwork_id"] == 101, "artwork_sale_count"].iloc[0] == 2
        # artwork_id 102 appears once
        assert result.loc[result["artwork_id"] == 102, "artwork_sale_count"].iloc[0] == 1


class TestDuckDBAggregateProcessor:
    """Tests for DuckDBAggregateProcessor."""

    def test_simple_aggregation(self, sales_df: pd.DataFrame) -> None:
        """Test basic GROUP BY aggregation."""
        processor = DuckDBAggregateProcessor(
            group_cols=["artwork_id"],
            agg_exprs={
                "total_sales": "SUM(sale_price_usd)",
                "sale_count": "COUNT(*)",
            },
        )
        result = processor.process(sales_df)

        assert len(result) == 3  # 3 unique artwork_ids
        row_101 = result[result["artwork_id"] == 101].iloc[0]
        assert row_101["total_sales"] == 250000  # 100000 + 150000
        assert row_101["sale_count"] == 2


class TestDuckDBSQLProcessor:
    """Tests for DuckDBSQLProcessor."""

    def test_computed_columns(self, sales_df: pd.DataFrame) -> None:
        """Test adding computed columns via SQL."""
        processor = DuckDBSQLProcessor(
            sql="""
                SELECT *,
                    sale_price_usd * 2 AS doubled_price,
                    sale_price_usd / 1000 AS price_thousands
                FROM _input
            """
        )
        result = processor.process(sales_df)

        assert "doubled_price" in result.columns
        assert "price_thousands" in result.columns
        assert result["doubled_price"][0] == 200000

    def test_filtering(self, sales_df: pd.DataFrame) -> None:
        """Test filtering via SQL."""
        processor = DuckDBSQLProcessor(sql="SELECT * FROM _input WHERE sale_price_usd > 100000")
        result = processor.process(sales_df)

        assert len(result) == 2  # Only 250000 and 150000

    def test_multi_table_join(self, sales_df: pd.DataFrame, artworks_df: pd.DataFrame) -> None:
        """Test joining multiple DataFrames via tables parameter."""
        processor = DuckDBSQLProcessor(
            sql="""
                SELECT a.sale_id, a.artwork_id, b.title
                FROM _input a
                LEFT JOIN artworks b ON a.artwork_id = b.artwork_id
            """
        )
        result = processor.process(sales_df, tables={"artworks": artworks_df})

        assert len(result) == 4
        assert "title" in result.columns


class TestDuckDBQueryProcessor:
    """Tests for DuckDBQueryProcessor."""

    def test_query_database_tables(self, tmp_path) -> None:
        """Test querying tables from configured database."""
        import duckdb

        # Create a test database with some data
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE test_table (id INT, value VARCHAR)")
        conn.execute("INSERT INTO test_table VALUES (1, 'one'), (2, 'two')")
        conn.close()

        # Configure and query
        configure(db_path=str(db_path), read_only=True)
        processor = DuckDBQueryProcessor(sql="SELECT * FROM test_table ORDER BY id")
        result = processor.process()

        assert len(result) == 2
        assert result["value"][0] == "one"

    def test_query_with_joins(self, tmp_path) -> None:
        """Test querying with joins from configured database."""
        import duckdb

        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE orders (id INT, product_id INT)")
        conn.execute("INSERT INTO orders VALUES (1, 10), (2, 20)")
        conn.execute("CREATE TABLE products (id INT, name VARCHAR)")
        conn.execute("INSERT INTO products VALUES (10, 'Widget'), (20, 'Gadget')")
        conn.close()

        configure(db_path=str(db_path), read_only=True)
        processor = DuckDBQueryProcessor(
            sql="""
            SELECT o.id AS order_id, p.name AS product_name
            FROM orders o
            JOIN products p ON o.product_id = p.id
            ORDER BY o.id
        """
        )
        result = processor.process()

        assert len(result) == 2
        assert result["product_name"][0] == "Widget"


# -----------------------------------------------------------------------------
# Polars Processor Tests
# -----------------------------------------------------------------------------


class TestPolarsStringProcessor:
    """Tests for PolarsStringProcessor."""

    def test_strip(self, artists_df: pd.DataFrame) -> None:
        """Test stripping whitespace from strings."""
        import polars as pl

        processor = PolarsStringProcessor("name", "strip")
        result = processor.process(artists_df)

        assert isinstance(result, pl.DataFrame)
        assert result["name"][0] == "Alice Smith"

    def test_upper(self, artists_df: pd.DataFrame) -> None:
        """Test converting to uppercase."""
        import polars as pl

        processor = PolarsStringProcessor("name", "upper")
        result = processor.process(artists_df)

        assert isinstance(result, pl.DataFrame)
        assert "ALICE" in result["name"][0]

    def test_lower(self, artists_df: pd.DataFrame) -> None:
        """Test converting to lowercase."""
        import polars as pl

        processor = PolarsStringProcessor("name", "lower")
        result = processor.process(artists_df)

        assert isinstance(result, pl.DataFrame)
        assert "alice" in result["name"][0]

    def test_always_returns_polars(self, artists_df: pd.DataFrame) -> None:
        """Test that output is always Polars regardless of input type."""
        import polars as pl

        processor = PolarsStringProcessor("name", "upper")

        # Pandas input -> Polars output
        result_from_pandas = processor.process(artists_df)
        assert isinstance(result_from_pandas, pl.DataFrame)

        # Polars input -> Polars output
        pl_df = pl.from_pandas(artists_df)
        result_from_polars = processor.process(pl_df)
        assert isinstance(result_from_polars, pl.DataFrame)


class TestPolarsFilterProcessor:
    """Tests for PolarsFilterProcessor."""

    def test_greater_than(self, sales_df: pd.DataFrame) -> None:
        """Test filtering with > operator."""
        import polars as pl

        processor = PolarsFilterProcessor("sale_price_usd", 100000, ">")
        result = processor.process(sales_df)

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 2
        assert all(result["sale_price_usd"] > 100000)

    def test_greater_than_or_equal(self, sales_df: pd.DataFrame) -> None:
        """Test filtering with >= operator."""
        import polars as pl

        processor = PolarsFilterProcessor("sale_price_usd", 100000, ">=")
        result = processor.process(sales_df)

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 3

    def test_less_than(self, sales_df: pd.DataFrame) -> None:
        """Test filtering with < operator."""
        import polars as pl

        processor = PolarsFilterProcessor("sale_price_usd", 150000, "<")
        result = processor.process(sales_df)

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 2

    def test_equal(self, sales_df: pd.DataFrame) -> None:
        """Test filtering with == operator."""
        import polars as pl

        processor = PolarsFilterProcessor("sale_price_usd", 100000, "==")
        result = processor.process(sales_df)

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 1


# -----------------------------------------------------------------------------
# Chain Tests
# -----------------------------------------------------------------------------


class TestChain:
    """Tests for Chain processor composition."""

    def test_chain_polars_processors(self, artists_df: pd.DataFrame) -> None:
        """Test chaining multiple Polars processors."""
        import polars as pl

        chain = Chain(
            [
                PolarsStringProcessor("name", "strip"),
                PolarsStringProcessor("name", "upper"),
            ]
        )
        result = chain.process(artists_df)

        assert isinstance(result, pl.DataFrame)
        assert result["name"][0] == "ALICE SMITH"

    def test_chain_filter_and_string(self, sales_df: pd.DataFrame) -> None:
        """Test chaining filter and string processors."""
        import polars as pl

        # Add a string column for testing
        sales_df["status"] = ["  pending  ", "completed", "  pending  ", "completed"]

        chain = Chain(
            [
                PolarsFilterProcessor("sale_price_usd", 100000, ">="),
                PolarsStringProcessor("status", "strip"),
            ]
        )
        result = chain.process(sales_df)

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 3
        # Find the row with sale_id == 1 and check status
        row = result.filter(pl.col("sale_id") == 1)
        assert row["status"][0] == "pending"
