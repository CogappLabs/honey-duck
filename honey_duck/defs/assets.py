"""Transform assets that read from dlt-harvested tables.

These assets depend on dlt harvest and read data from the raw schema
in DuckDB, then output to the main schema.

Asset Graph:
    dlt_harvest_sales_raw ────────┐
                                  │
    dlt_harvest_artworks_raw ─────┼──→ sales_enriched ──┬──→ sales_output
                                  │                     │
    dlt_harvest_artists_raw ──────┘                     └──→ artworks_output
"""

import duckdb

import dagster as dg
import pandas as pd

from cogapp_deps.processors import Chain
from cogapp_deps.processors.duckdb import (
    DuckDBAggregateProcessor,
    DuckDBJoinProcessor,
    DuckDBWindowProcessor,
)
from cogapp_deps.processors.polars import PolarsFilterProcessor, PolarsStringProcessor

from .resources import (
    ARTWORKS_OUTPUT_PATH,
    DUCKDB_PATH,
    OUTPUT_DIR,
    SALES_OUTPUT_PATH,
)


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Get connection to the main DuckDB database (shared with dlt)."""
    return duckdb.connect(DUCKDB_PATH)


# Asset keys for dlt harvest assets
DLT_SALES_KEY = dg.AssetKey("dlt_honey_duck_harvest_sales_raw")
DLT_ARTWORKS_KEY = dg.AssetKey("dlt_honey_duck_harvest_artworks_raw")
DLT_ARTISTS_KEY = dg.AssetKey("dlt_honey_duck_harvest_artists_raw")


@dg.asset(
    kinds={"duckdb"},
    deps=[DLT_SALES_KEY, DLT_ARTWORKS_KEY, DLT_ARTISTS_KEY],
    group_name="transform",
)
def sales_enriched(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """Join sales with artworks and artists, add computed metrics.

    Reads from dlt-harvested tables in raw schema.

    Demonstrates composing DuckDB processors:
    1. DuckDBJoinProcessor - chained joins (sales -> artworks -> artists)
    2. DuckDBWindowProcessor - window aggregations (artist totals, rankings)
    3. DuckDBSQLProcessor - combines join + window + computed columns
    """
    conn = get_duckdb_connection()

    # 1. Define the chained join structure (reading from raw schema)
    join_processor = DuckDBJoinProcessor(
        base_table="raw.sales_raw",
        joins=[
            ("raw.artworks_raw", "a.artwork_id", "artwork_id"),
            ("raw.artists_raw", "b.artist_id", "artist_id"),
        ],
        select_cols=[
            "a.sale_id",
            "a.artwork_id",
            "a.sale_date",
            "a.sale_price_usd",
            "a.buyer_country",
            "b.title",
            "b.artist_id",
            "b.year AS artwork_year",
            "b.medium",
            "b.price_usd AS list_price_usd",
            "c.name AS artist_name",
            "c.nationality",
            "c.birth_year",
        ],
    )

    # 2. Define window aggregations
    window_processor = DuckDBWindowProcessor(
        exprs={
            "artist_total_sales": "SUM(sale_price_usd) OVER (PARTITION BY artist_name)",
            "artist_sale_count": "COUNT(*) OVER (PARTITION BY artist_name)",
            "nationality_total_sales": "SUM(sale_price_usd) OVER (PARTITION BY nationality)",
            "rank_in_artist": "ROW_NUMBER() OVER (PARTITION BY artist_name ORDER BY sale_price_usd DESC)",
        }
    )

    # 3. Compose into final SQL
    join_sql = join_processor.generate_sql()
    window_exprs = ", ".join(window_processor.generate_sql())

    sql = f"""
        SELECT
            *,
            sale_price_usd - list_price_usd AS price_diff,
            ROUND((sale_price_usd - list_price_usd) * 100.0 / list_price_usd, 1) AS pct_change,
            {window_exprs}
        FROM ({join_sql}) AS joined
        ORDER BY sale_date DESC
    """

    result = conn.sql(sql).df()

    # String transformations using chained Polars processors
    chain = Chain([
        PolarsStringProcessor("artist_name", "strip"),
        PolarsStringProcessor("artist_name", "upper"),
    ])
    result = chain.process(result)

    context.add_output_metadata(
        {
            "record_count": dg.MetadataValue.int(len(result)),
            "columns": dg.MetadataValue.json(result.columns.tolist()),
            "preview": dg.MetadataValue.md(result.head(5).to_markdown(index=False)),
            "unique_artists": dg.MetadataValue.int(result["artist_name"].nunique()),
            "unique_artworks": dg.MetadataValue.int(result["artwork_id"].nunique()),
            "total_sales_value": dg.MetadataValue.float(float(result["sale_price_usd"].sum())),
            "date_range": dg.MetadataValue.text(
                f"{result['sale_date'].min()} to {result['sale_date'].max()}"
            ),
        }
    )
    context.log.info(f"Enriched {len(result)} sales records from dlt tables")
    return result


@dg.asset(
    kinds={"duckdb", "json"},
    group_name="output",
)
def sales_output(
    context: dg.AssetExecutionContext,
    sales_enriched: pd.DataFrame,  # Received via IO manager
) -> pd.DataFrame:
    """Filter high-value sales and output to JSON.

    Demonstrates PolarsFilterProcessor for filtering.
    Receives sales_enriched DataFrame via IO manager.
    """
    total_count = len(sales_enriched)

    # Filter high-value sales using PolarsFilterProcessor
    MIN_SALE_VALUE = 30_000_000
    filter_processor = PolarsFilterProcessor("sale_price_usd", MIN_SALE_VALUE, ">=")
    result = filter_processor.process(sales_enriched)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    result.to_json(SALES_OUTPUT_PATH, orient="records", indent=2)

    context.add_output_metadata(
        {
            "record_count": dg.MetadataValue.int(len(result)),
            "filtered_from": dg.MetadataValue.int(total_count),
            "filter_threshold": dg.MetadataValue.text(f"${MIN_SALE_VALUE:,}"),
            "total_value": dg.MetadataValue.float(float(result["sale_price_usd"].sum())),
            "json_output": dg.MetadataValue.path(str(SALES_OUTPUT_PATH)),
            "preview": dg.MetadataValue.md(result.head(10).to_markdown(index=False)),
        }
    )
    context.log.info(f"Output {len(result)} high-value sales to {SALES_OUTPUT_PATH}")
    return result


@dg.asset(
    kinds={"duckdb", "json"},
    deps=[DLT_ARTWORKS_KEY, DLT_ARTISTS_KEY],  # Also depends on raw tables
    group_name="output",
)
def artworks_output(
    context: dg.AssetExecutionContext,
    sales_enriched: pd.DataFrame,  # Received via IO manager
) -> pd.DataFrame:
    """Aggregate sales data per artwork for catalog view.

    Receives sales_enriched via IO manager.
    Reads raw tables for catalog join (read-only connection).

    Demonstrates composing DuckDB processors:
    1. DuckDBAggregateProcessor - aggregate sales by artwork
    2. DuckDBJoinProcessor - join artworks with artists
    """
    # Use read-only connection to avoid locks with other processes
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)

    # Register the sales_enriched DataFrame we received
    conn.register("sales_enriched_df", sales_enriched)

    # 1. Aggregate sales per artwork
    agg_processor = DuckDBAggregateProcessor(
        source_table="sales_enriched_df",
        group_cols=["artwork_id"],
        agg_exprs={
            "total_sales_count": "COUNT(*)",
            "total_sales_value": "SUM(sale_price_usd)",
            "avg_sale_price": "ROUND(AVG(sale_price_usd), 0)",
            "last_sale_date": "MAX(sale_date)",
            "first_sale_date": "MIN(sale_date)",
        },
    )

    # 2. Join artworks with artists (from dlt raw schema)
    catalog_join = DuckDBJoinProcessor(
        base_table="raw.artworks_raw",
        joins=[
            ("raw.artists_raw", "artist_id", "artist_id"),
        ],
        select_cols=[
            "a.artwork_id",
            "a.title",
            "a.year",
            "a.medium",
            "a.price_usd AS list_price_usd",
            "b.name AS artist_name",
            "b.nationality",
            "b.birth_year",
        ],
    )

    # 3. Compose: CTE for aggregation, join catalog, add computed columns
    agg_sql = agg_processor.generate_sql()
    catalog_sql = catalog_join.generate_sql()

    sql = f"""
        WITH sales_agg AS ({agg_sql}),
             catalog AS ({catalog_sql})
        SELECT
            c.*,
            COALESCE(s.total_sales_count, 0) AS total_sales_count,
            COALESCE(s.total_sales_value, 0) AS total_sales_value,
            s.avg_sale_price,
            s.last_sale_date,
            s.first_sale_date,
            CASE WHEN s.total_sales_count > 0 THEN true ELSE false END AS has_sold,
            CASE
                WHEN c.list_price_usd < 20000000 THEN 'budget'
                WHEN c.list_price_usd < 100000000 THEN 'mid'
                ELSE 'premium'
            END AS price_tier
        FROM catalog c
        LEFT JOIN sales_agg s ON c.artwork_id = s.artwork_id
        ORDER BY COALESCE(s.total_sales_value, 0) DESC
    """

    result = conn.sql(sql).df()
    conn.close()

    # String transformation using Polars
    string_processor = PolarsStringProcessor("artist_name", "upper")
    result = string_processor.process(result)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    result.to_json(ARTWORKS_OUTPUT_PATH, orient="records", indent=2)

    tier_counts = result["price_tier"].value_counts().to_dict()

    context.add_output_metadata(
        {
            "record_count": dg.MetadataValue.int(len(result)),
            "artworks_sold": dg.MetadataValue.int(int(result["has_sold"].sum())),
            "artworks_unsold": dg.MetadataValue.int(int((~result["has_sold"]).sum())),
            "price_tier_distribution": dg.MetadataValue.json(tier_counts),
            "total_catalog_value": dg.MetadataValue.float(float(result["list_price_usd"].sum())),
            "json_output": dg.MetadataValue.path(str(ARTWORKS_OUTPUT_PATH)),
            "preview": dg.MetadataValue.md(result.head(10).to_markdown(index=False)),
        }
    )
    context.log.info(f"Output {len(result)} artworks to {ARTWORKS_OUTPUT_PATH}")
    return result
