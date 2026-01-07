"""
Full pipeline - enriches sale records with artist and nationality stats.

Uses 5 processor types: DuckDBLookupProcessor, DuckDBSQLProcessor,
PandasUppercaseProcessor, PandasFilterProcessor, PolarsWindowProcessor.

Note: DuckDBAggregateProcessor and PolarsAggregateProcessor collapse rows,
so this pipeline uses SQL window functions to add aggregate stats while
keeping individual sale records.
"""

from pathlib import Path

from honey_duck import (
    DuckDBLookupProcessor,
    DuckDBSQLProcessor,
    PandasFilterProcessor,
    PandasUppercaseProcessor,
    PolarsWindowProcessor,
)

DATA_DIR = Path(__file__).parent.parent / "data"
INPUT = DATA_DIR / "input"

SOURCES = {
    "pipeline_data": INPUT / "sales.csv",
    "artworks": INPUT / "artworks.csv",
    "artists": INPUT / "artists.csv",
}

PROCESSORS = [
    # 1. DuckDBLookupProcessor - join sales with artworks
    DuckDBLookupProcessor(
        lookup_table="artworks",
        left_on="artwork_id",
        right_on="artwork_id",
        columns=["title", "artist_id", "price_usd"],
    ),
    # 2. DuckDBLookupProcessor - join with artists
    DuckDBLookupProcessor(
        lookup_table="artists",
        left_on="artist_id",
        right_on="artist_id",
        columns=["name", "nationality"],
    ),
    # 3. DuckDBSQLProcessor - calculate price metrics + aggregate stats via window functions
    DuckDBSQLProcessor("""
        SELECT *,
            sale_price_usd - price_usd as price_diff,
            ROUND((sale_price_usd - price_usd) * 100.0 / price_usd, 1) as pct_change,
            SUM(sale_price_usd) OVER (PARTITION BY name) as artist_total,
            COUNT(*) OVER (PARTITION BY name) as artist_sales,
            SUM(sale_price_usd) OVER (PARTITION BY nationality) as nationality_total
        FROM pipeline_data
    """),
    # 4. PandasUppercaseProcessor - uppercase artist name
    PandasUppercaseProcessor("name"),
    # 5. PandasFilterProcessor - high value sales only ($30M+)
    PandasFilterProcessor("sale_price_usd", min_value=30_000_000),
    # 6. PolarsWindowProcessor - rank sales within each artist
    PolarsWindowProcessor(partition_by="name", order_by="sale_price_usd"),
]

OUTPUT = DATA_DIR / "output" / "full.json"
