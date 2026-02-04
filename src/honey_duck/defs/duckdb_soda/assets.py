"""DuckDB + Soda pipeline with column lineage tracking.

Asset Graph:
    dlt_harvest_* --> sales_transform_soda --> sales_output_soda
                  └-> artworks_transform_soda --> artworks_output_soda

This module uses the declarative asset factories from cogapp_libs.dagster.duckdb.
All data flows through DuckDB/Parquet without materializing into Python memory.
"""

import dagster as dg

from cogapp_libs.dagster.duckdb import duckdb_output_asset, duckdb_transform_asset
from cogapp_libs.dagster.lineage import build_lineage, passthrough_lineage

from ..shared.constants import (
    MIN_SALE_VALUE_USD,
    PRICE_TIER_BUDGET_MAX_USD,
    PRICE_TIER_MID_MAX_USD,
)

# -----------------------------------------------------------------------------
# Asset Keys for Lineage
# -----------------------------------------------------------------------------

SALES_RAW = dg.AssetKey("dlt_harvest_sales_raw")
ARTWORKS_RAW = dg.AssetKey("dlt_harvest_artworks_raw")
ARTISTS_RAW = dg.AssetKey("dlt_harvest_artists_raw")
MEDIA_RAW = dg.AssetKey("dlt_harvest_media")
SALES_TRANSFORM = dg.AssetKey("sales_transform_soda")
ARTWORKS_TRANSFORM = dg.AssetKey("artworks_transform_soda")

# -----------------------------------------------------------------------------
# Column Lineage Definitions
# -----------------------------------------------------------------------------

SALES_TRANSFORM_LINEAGE = build_lineage(
    passthrough={
        "sale_id": SALES_RAW,
        "artwork_id": SALES_RAW,
        "sale_date": SALES_RAW,
        "sale_price_usd": SALES_RAW,
        "buyer_country": SALES_RAW,
        "title": ARTWORKS_RAW,
        "artist_id": ARTWORKS_RAW,
        "medium": ARTWORKS_RAW,
        "artist_name": ARTISTS_RAW,
        "nationality": ARTISTS_RAW,
    },
    rename={
        "artwork_year": (ARTWORKS_RAW, "year"),
        "list_price_usd": (ARTWORKS_RAW, "price_usd"),
    },
    computed={
        "price_diff": [(SALES_RAW, "sale_price_usd"), (ARTWORKS_RAW, "price_usd")],
        "pct_change": [(SALES_RAW, "sale_price_usd"), (ARTWORKS_RAW, "price_usd")],
    },
)

ARTWORKS_TRANSFORM_LINEAGE = build_lineage(
    passthrough={
        "artwork_id": ARTWORKS_RAW,
        "title": ARTWORKS_RAW,
        "year": ARTWORKS_RAW,
        "medium": ARTWORKS_RAW,
        "nationality": ARTISTS_RAW,
    },
    rename={
        "list_price_usd": (ARTWORKS_RAW, "price_usd"),
        "artist_name": (ARTISTS_RAW, "name"),
        "primary_image": (MEDIA_RAW, "filename"),
        "primary_image_alt": (MEDIA_RAW, "alt_text"),
    },
    computed={
        "sale_count": [(SALES_RAW, "sale_id")],
        "total_sales_value": [(SALES_RAW, "sale_price_usd")],
        "avg_sale_price": [(SALES_RAW, "sale_price_usd")],
        "first_sale_date": [(SALES_RAW, "sale_date")],
        "last_sale_date": [(SALES_RAW, "sale_date")],
        "has_sold": [(SALES_RAW, "sale_id")],
        "price_tier": [(ARTWORKS_RAW, "price_usd")],
        "sales_rank": [(SALES_RAW, "sale_price_usd")],
        "media_count": [(MEDIA_RAW, "artwork_id")],
        "media": [
            (MEDIA_RAW, "sort_order"),
            (MEDIA_RAW, "filename"),
            (MEDIA_RAW, "media_type"),
            (MEDIA_RAW, "file_format"),
        ],
    },
)

SALES_OUTPUT_LINEAGE = passthrough_lineage(
    source=SALES_TRANSFORM,
    columns=[
        "sale_id",
        "artwork_id",
        "sale_date",
        "sale_price_usd",
        "buyer_country",
        "title",
        "artist_id",
        "artwork_year",
        "medium",
        "list_price_usd",
        "artist_name",
        "nationality",
        "pct_change",
    ],
    renames={"price_difference": "price_diff"},
)

ARTWORKS_OUTPUT_LINEAGE = passthrough_lineage(
    source=ARTWORKS_TRANSFORM,
    columns=[
        "artwork_id",
        "title",
        "year",
        "medium",
        "list_price_usd",
        "artist_name",
        "nationality",
        "sale_count",
        "total_sales_value",
        "avg_sale_price",
        "first_sale_date",
        "last_sale_date",
        "has_sold",
        "price_tier",
        "sales_rank",
        "primary_image",
        "primary_image_alt",
        "media_count",
        "media",
    ],
)

# -----------------------------------------------------------------------------
# SQL Queries
# -----------------------------------------------------------------------------

SALES_TRANSFORM_SQL = """
SELECT
    s.sale_id, s.artwork_id, s.sale_date, s.sale_price_usd, s.buyer_country,
    aw.title, aw.artist_id, aw.year AS artwork_year, aw.medium,
    aw.price_usd AS list_price_usd,
    upper(trim(ar.name)) AS artist_name, ar.nationality,
    s.sale_price_usd - aw.price_usd AS price_diff,
    round((s.sale_price_usd - aw.price_usd) * 100.0 / nullif(aw.price_usd, 0), 1) AS pct_change
FROM sales s
LEFT JOIN artworks aw USING (artwork_id)
LEFT JOIN artists ar ON aw.artist_id = ar.artist_id
ORDER BY s.sale_date DESC, s.sale_id
"""

ARTWORKS_TRANSFORM_SQL = f"""
WITH sales_agg AS (
    SELECT artwork_id,
        count(*) AS sale_count,
        sum(sale_price_usd) AS total_sales_value,
        round(avg(sale_price_usd), 0) AS avg_sale_price,
        min(sale_date) AS first_sale_date,
        max(sale_date) AS last_sale_date
    FROM sales s JOIN artworks aw USING (artwork_id)
    GROUP BY artwork_id
),
catalog AS (
    SELECT aw.artwork_id, aw.title, aw.year, aw.medium,
        aw.price_usd AS list_price_usd,
        ar.name AS artist_name, ar.nationality
    FROM artworks aw LEFT JOIN artists ar USING (artist_id)
),
primary_media AS (
    SELECT artwork_id, filename AS primary_image, alt_text AS primary_image_alt
    FROM media WHERE sort_order = 1
),
all_media AS (
    SELECT artwork_id, list({{
        'sort_order': sort_order, 'filename': filename,
        'media_type': media_type, 'file_format': file_format,
        'width_px': width_px, 'height_px': height_px,
        'file_size_kb': file_size_kb, 'alt_text': alt_text
    }} ORDER BY sort_order) AS media
    FROM media GROUP BY artwork_id
)
SELECT
    c.artwork_id, c.title, c.year, c.medium, c.list_price_usd,
    upper(c.artist_name) AS artist_name, c.nationality,
    coalesce(s.sale_count, 0) AS sale_count,
    coalesce(s.total_sales_value, 0) AS total_sales_value,
    s.avg_sale_price, s.first_sale_date, s.last_sale_date,
    s.sale_count > 0 AS has_sold,
    CASE
        WHEN c.list_price_usd < {PRICE_TIER_BUDGET_MAX_USD} THEN 'budget'
        WHEN c.list_price_usd < {PRICE_TIER_MID_MAX_USD} THEN 'mid'
        ELSE 'premium'
    END AS price_tier,
    rank() OVER (ORDER BY coalesce(s.total_sales_value, 0) DESC) AS sales_rank,
    pm.primary_image, pm.primary_image_alt,
    coalesce(length(am.media), 0) AS media_count, am.media
FROM catalog c
LEFT JOIN sales_agg s USING (artwork_id)
LEFT JOIN primary_media pm USING (artwork_id)
LEFT JOIN all_media am USING (artwork_id)
ORDER BY coalesce(s.total_sales_value, 0) DESC, c.artwork_id
"""

# -----------------------------------------------------------------------------
# Assets (declarative via factories)
# -----------------------------------------------------------------------------

sales_transform_soda = duckdb_transform_asset(
    name="sales_transform_soda",
    sql=SALES_TRANSFORM_SQL,
    harvest_views=["sales", "artworks", "artists"],
    lineage=SALES_TRANSFORM_LINEAGE,
    example_id=("sale_id", 2),
    group_name="transform_soda",
    kinds={"duckdb", "sql", "soda"},
)

artworks_transform_soda = duckdb_transform_asset(
    name="artworks_transform_soda",
    sql=ARTWORKS_TRANSFORM_SQL,
    harvest_views=["sales", "artworks", "artists", "media"],
    lineage=ARTWORKS_TRANSFORM_LINEAGE,
    example_id=("artwork_id", 2),
    group_name="transform_soda",
    kinds={"duckdb", "sql", "soda"},
)

sales_output_soda = duckdb_output_asset(
    name="sales_output_soda",
    source="sales_transform_soda",
    output_path_attr="sales_soda",
    where=f"sale_price_usd >= {MIN_SALE_VALUE_USD}",
    select="* EXCLUDE (price_diff), price_diff AS price_difference",
    lineage=SALES_OUTPUT_LINEAGE,
    example_id=("sale_id", 2),
    example_renames={"price_difference": "price_diff"},
    group_name="output_soda",
    kinds={"duckdb", "sql", "json", "soda"},
    # Explicit ordering: sales_output depends on artworks_output to ensure
    # deterministic execution order for consistent JSON output hashes.
    deps=["artworks_output_soda"],
)

artworks_output_soda = duckdb_output_asset(
    name="artworks_output_soda",
    source="artworks_transform_soda",
    output_path_attr="artworks_soda",
    lineage=ARTWORKS_OUTPUT_LINEAGE,
    example_id=("artwork_id", 2),
    group_name="output_soda",
    kinds={"duckdb", "sql", "json", "soda"},
)
