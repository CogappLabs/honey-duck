"""Transform and output assets for the honey-duck pipeline.

IO Patterns Across the Pipeline
-------------------------------
This pipeline uses two different IO patterns:

1. HARVEST LAYER (dlt_assets.py) - dlt manages IO:
   - dlt writes directly to DuckDB raw.* tables
   - No data returned, yields MaterializeResult with metadata only
   - Dagster IO manager is NOT used

2. TRANSFORM LAYER (this file) - Mixed pattern:
   - INPUT: Reads from raw.* tables via direct SQL queries (not IO manager)
   - OUTPUT: Returns pd.DataFrame -> IO manager stores in main.{asset_name}

   Why direct SQL for input? Transform assets use `deps=[...]` to declare
   dependencies on harvest assets, but read data via SQL joins rather than
   receiving DataFrames as function parameters. This allows complex multi-table
   joins that would be awkward with the IO manager pattern.

3. OUTPUT LAYER (this file) - IO manager for input:
   - INPUT: Receives DataFrame via function parameter (IO manager loads it)
   - OUTPUT: Returns pd.DataFrame -> IO manager stores + writes JSON file

Data Flow:
    ┌─────────────────────────────────────────────────────────────────┐
    │ HARVEST (dlt writes to DuckDB, no IO manager)                   │
    │   dlt_harvest_assets -> raw.sales_raw, raw.artworks_raw, etc.  │
    │   media_harvest      -> raw.media                               │
    └──────────────────────────────┬──────────────────────────────────┘
                                   │ deps (no data passed)
                                   ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │ TRANSFORM (read raw.* via SQL, IO manager stores output)        │
    │   sales_transform    : SQL joins raw.* -> return DataFrame      │
    │   artworks_transform : SQL joins raw.* -> return DataFrame      │
    └──────────────────────────────┬──────────────────────────────────┘
                                   │ DataFrame via IO manager
                                   ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │ OUTPUT (IO manager loads input, stores output + writes JSON)    │
    │   sales_output    : param DataFrame -> return DataFrame + JSON  │
    │   artworks_output : param DataFrame -> return DataFrame + JSON  │
    └─────────────────────────────────────────────────────────────────┘

Asset Graph:
    dlt_harvest_sales_raw ────────┐
                                  ├──→ sales_transform ────→ sales_output
    dlt_harvest_artworks_raw ─────┤
                                  │
    dlt_harvest_artists_raw ──────┼──→ artworks_transform ─→ artworks_output
                                  │
    media_harvest ────────────────┘
"""

from datetime import timedelta

import dagster as dg
import pandas as pd

from cogapp_deps.dagster import read_table, write_json_output
from cogapp_deps.processors import Chain
from cogapp_deps.processors.duckdb import (
    DuckDBJoinProcessor,
    DuckDBSQLProcessor,
    configure as configure_duckdb,
)
from cogapp_deps.processors.polars import PolarsFilterProcessor, PolarsStringProcessor

from .resources import (
    ARTWORKS_OUTPUT_PATH,
    DUCKDB_PATH,
    SALES_OUTPUT_PATH,
)

# Configure DuckDB processors to use the main database for reading raw tables
# Use read_only=True to avoid lock conflicts with the IO manager
configure_duckdb(db_path=DUCKDB_PATH, read_only=True)


# All harvest assets - used as dependencies for transform layer
HARVEST_DEPS = [
    dg.AssetKey("dlt_harvest_sales_raw"),
    dg.AssetKey("dlt_harvest_artworks_raw"),
    dg.AssetKey("dlt_harvest_artists_raw"),
    dg.AssetKey("dlt_harvest_media"),
]


# -----------------------------------------------------------------------------
# Sales Pipeline: sales_transform → sales_output
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"duckdb"},
    deps=HARVEST_DEPS,
    group_name="transform",
)
def sales_transform(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """Join sales with artworks and artists, add sale-focused metrics."""
    # Join sales → artworks → artists
    result = DuckDBJoinProcessor(
        base_table="raw.sales_raw",
        joins=[
            ("raw.artworks_raw", "a.artwork_id", "artwork_id"),
            ("raw.artists_raw", "b.artist_id", "artist_id"),
        ],
        select_cols=[
            "a.sale_id", "a.artwork_id", "a.sale_date", "a.sale_price_usd", "a.buyer_country",
            "b.title", "b.artist_id", "b.year AS artwork_year", "b.medium", "b.price_usd AS list_price_usd",
            "c.name AS artist_name", "c.nationality",
        ],
    ).process()

    # Add price metrics
    result = DuckDBSQLProcessor(sql="""
        SELECT *,
            sale_price_usd - list_price_usd AS price_diff,
            ROUND((sale_price_usd - list_price_usd) * 100.0 / list_price_usd, 1) AS pct_change
        FROM _input
        ORDER BY sale_date DESC
    """).process(result)

    # Normalize artist names
    result = Chain([
        PolarsStringProcessor("artist_name", "strip"),
        PolarsStringProcessor("artist_name", "upper"),
    ]).process(result)

    context.add_output_metadata({
        "record_count": len(result),
        "columns": result.columns.tolist(),
        "preview": dg.MetadataValue.md(result.head(5).to_markdown(index=False)),
        "unique_artworks": result["artwork_id"].nunique(),
        "total_sales_value": float(result["sale_price_usd"].sum()),
        "date_range": f"{result['sale_date'].min()} to {result['sale_date'].max()}",
    })
    context.log.info(f"Transformed {len(result)} sales records")
    return result


@dg.asset(
    kinds={"duckdb", "json"},
    deps=["sales_transform", "artworks_output"],
    group_name="output",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def sales_output(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """Filter high-value sales and output to JSON.

    Depends on artworks_output to avoid DuckDB lock conflicts (single writer).
    """
    sales_df = read_table("sales_transform")
    total_count = len(sales_df)

    # Filter high-value sales
    MIN_SALE_VALUE = 30_000_000
    filter_processor = PolarsFilterProcessor("sale_price_usd", MIN_SALE_VALUE, ">=")
    result = filter_processor.process(sales_df)

    write_json_output(result, SALES_OUTPUT_PATH, context, extra_metadata={
        "filtered_from": total_count,
        "filter_threshold": f"${MIN_SALE_VALUE:,}",
        "total_value": float(result["sale_price_usd"].sum()),
    })
    context.log.info(f"Output {len(result)} high-value sales to {SALES_OUTPUT_PATH}")
    return result


# -----------------------------------------------------------------------------
# Artworks Pipeline: artworks_transform → artworks_output
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"duckdb"},
    deps=HARVEST_DEPS,
    group_name="transform",
)
def artworks_transform(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """Join artworks with artists, media, and aggregate sales history per artwork."""
    # Aggregate sales per artwork
    sales_agg = DuckDBJoinProcessor(
        base_table="raw.sales_raw",
        joins=[("raw.artworks_raw", "a.artwork_id", "artwork_id")],
        select_cols=["a.artwork_id", "a.sale_price_usd", "a.sale_date"],
    ).process()

    sales_agg = DuckDBSQLProcessor(sql="""
        SELECT artwork_id,
            COUNT(*) AS sale_count,
            SUM(sale_price_usd) AS total_sales_value,
            ROUND(AVG(sale_price_usd), 0) AS avg_sale_price,
            MIN(sale_date) AS first_sale_date,
            MAX(sale_date) AS last_sale_date
        FROM _input
        GROUP BY artwork_id
    """).process(sales_agg)

    # Build artwork catalog (artworks + artists)
    catalog = DuckDBJoinProcessor(
        base_table="raw.artworks_raw",
        joins=[("raw.artists_raw", "artist_id", "artist_id")],
        select_cols=[
            "a.artwork_id", "a.title", "a.year", "a.medium", "a.price_usd AS list_price_usd",
            "b.name AS artist_name", "b.nationality",
        ],
    ).process()

    # Process media
    media_df = read_table("media", schema="raw")

    primary_media = DuckDBSQLProcessor(sql="""
        SELECT artwork_id, filename AS primary_image, alt_text AS primary_image_alt
        FROM _input WHERE sort_order = 1
    """).process(media_df)

    all_media = DuckDBSQLProcessor(sql="""
        SELECT artwork_id,
            list({
                'sort_order': sort_order, 'filename': filename, 'media_type': media_type,
                'file_format': file_format, 'width_px': width_px, 'height_px': height_px,
                'file_size_kb': file_size_kb, 'alt_text': alt_text
            } ORDER BY sort_order) AS media
        FROM _input
        GROUP BY artwork_id
    """).process(media_df)

    # Final assembly - join all intermediate results
    result = DuckDBSQLProcessor(sql="""
        SELECT c.*,
            COALESCE(s.sale_count, 0) AS sale_count,
            COALESCE(s.total_sales_value, 0) AS total_sales_value,
            s.avg_sale_price, s.first_sale_date, s.last_sale_date,
            CASE WHEN s.sale_count > 0 THEN true ELSE false END AS has_sold,
            CASE
                WHEN c.list_price_usd < 500000 THEN 'budget'
                WHEN c.list_price_usd < 3000000 THEN 'mid'
                ELSE 'premium'
            END AS price_tier,
            RANK() OVER (ORDER BY COALESCE(s.total_sales_value, 0) DESC) AS sales_rank,
            pm.primary_image, pm.primary_image_alt,
            COALESCE(len(am.media), 0) AS media_count,
            am.media
        FROM _input c
        LEFT JOIN sales_agg s ON c.artwork_id = s.artwork_id
        LEFT JOIN primary_media pm ON c.artwork_id = pm.artwork_id
        LEFT JOIN all_media am ON c.artwork_id = am.artwork_id
        ORDER BY COALESCE(s.total_sales_value, 0) DESC
    """).process(catalog, tables={
        "sales_agg": sales_agg,
        "primary_media": primary_media,
        "all_media": all_media,
    })

    # Normalize artist names
    result = PolarsStringProcessor("artist_name", "upper").process(result)

    context.add_output_metadata({
        "record_count": len(result),
        "artworks_sold": int(result["has_sold"].sum()),
        "artworks_unsold": int((~result["has_sold"]).sum()),
        "artworks_with_media": int((result["media_count"] > 0).sum()),
        "total_catalog_value": float(result["list_price_usd"].sum()),
        "preview": dg.MetadataValue.md(result.head(5).to_markdown(index=False)),
    })
    context.log.info(f"Transformed {len(result)} artworks with sales history and media")
    return result


@dg.asset(
    kinds={"duckdb", "json"},
    deps=["artworks_transform"],
    group_name="output",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def artworks_output(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """Output artwork catalog to JSON."""
    result = read_table("artworks_transform")
    tier_counts = result["price_tier"].value_counts().to_dict()

    write_json_output(result, ARTWORKS_OUTPUT_PATH, context, extra_metadata={
        "price_tier_distribution": tier_counts,
    })
    context.log.info(f"Output {len(result)} artworks to {ARTWORKS_OUTPUT_PATH}")
    return result
