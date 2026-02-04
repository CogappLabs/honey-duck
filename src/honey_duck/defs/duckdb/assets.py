"""Pure DuckDB SQL implementation of honey-duck pipeline transforms.

This module implements the transform and output layers using only inline
DuckDB SQL queries. Uses DuckDBResource for managed connection handling.

Asset Graph:
    dlt_harvest_* (shared) ──→ sales_transform_duckdb ──→ sales_output_duckdb
                           └──→ artworks_transform_duckdb ──→ artworks_output_duckdb
"""

import time
from datetime import timedelta

import dagster as dg
import polars as pl
from dagster_duckdb import DuckDBResource

from cogapp_libs.dagster import write_json_output

from ..shared.constants import (
    MIN_SALE_VALUE_USD,
    PRICE_TIER_BUDGET_MAX_USD,
    PRICE_TIER_MID_MAX_USD,
)
from ..shared.resources import OutputPathsResource, PathsResource

# Use centralized harvest dependencies for consistency
from ..shared.helpers import STANDARD_HARVEST_DEPS as HARVEST_DEPS


# -----------------------------------------------------------------------------
# SQL Query Builders
# -----------------------------------------------------------------------------


def _sales_transform_sql(harvest_dir: str) -> str:
    """Generate SQL for sales transform.

    Uses DuckDB friendly SQL features:
    - Dot notation for string functions: ar.name.trim().upper()
    - nullif() for safe division
    """
    return f"""
SELECT
    s.sale_id,
    s.artwork_id,
    s.sale_date,
    s.sale_price_usd,
    s.buyer_country,
    aw.title,
    aw.artist_id,
    aw.year AS artwork_year,
    aw.medium,
    aw.price_usd AS list_price_usd,
    ar.name.trim().upper() AS artist_name,
    ar.nationality,
    s.sale_price_usd - aw.price_usd AS price_diff,
    round((s.sale_price_usd - aw.price_usd) * 100.0 / nullif(aw.price_usd, 0), 1) AS pct_change
FROM read_parquet('{harvest_dir}/raw/sales_raw/**/*.parquet') s
LEFT JOIN read_parquet('{harvest_dir}/raw/artworks_raw/**/*.parquet') aw
    ON s.artwork_id = aw.artwork_id
LEFT JOIN read_parquet('{harvest_dir}/raw/artists_raw/**/*.parquet') ar
    ON aw.artist_id = ar.artist_id
ORDER BY s.sale_date DESC, s.sale_id
"""


def _artworks_transform_sql(harvest_dir: str) -> str:
    """Generate SQL for artworks transform.

    Uses DuckDB friendly SQL features:
    - GROUP BY ALL to infer grouped columns
    - Dot notation for string functions: c.artist_name.upper()
    - Boolean expression without CASE: s.sale_count > 0 AS has_sold
    - list() with ORDER BY for ordered aggregation
    """
    return f"""
WITH sales_per_artwork AS (
    SELECT
        s.artwork_id,
        count(*) AS sale_count,
        sum(s.sale_price_usd) AS total_sales_value,
        round(avg(s.sale_price_usd), 0) AS avg_sale_price,
        min(s.sale_date) AS first_sale_date,
        max(s.sale_date) AS last_sale_date
    FROM read_parquet('{harvest_dir}/raw/sales_raw/**/*.parquet') s
    JOIN read_parquet('{harvest_dir}/raw/artworks_raw/**/*.parquet') aw
        ON s.artwork_id = aw.artwork_id
    GROUP BY ALL
),
catalog AS (
    SELECT
        aw.artwork_id,
        aw.title,
        aw.year,
        aw.medium,
        aw.price_usd AS list_price_usd,
        ar.name AS artist_name,
        ar.nationality
    FROM read_parquet('{harvest_dir}/raw/artworks_raw/**/*.parquet') aw
    LEFT JOIN read_parquet('{harvest_dir}/raw/artists_raw/**/*.parquet') ar
        ON aw.artist_id = ar.artist_id
),
primary_media AS (
    SELECT
        artwork_id,
        filename AS primary_image,
        alt_text AS primary_image_alt
    FROM read_parquet('{harvest_dir}/raw/media/**/*.parquet')
    WHERE sort_order = 1
),
all_media AS (
    SELECT
        artwork_id,
        list({{
            'sort_order': sort_order,
            'filename': filename,
            'media_type': media_type,
            'file_format': file_format,
            'width_px': width_px,
            'height_px': height_px,
            'file_size_kb': file_size_kb,
            'alt_text': alt_text
        }} ORDER BY sort_order) AS media
    FROM read_parquet('{harvest_dir}/raw/media/**/*.parquet')
    GROUP BY ALL
)
SELECT
    c.artwork_id,
    c.title,
    c.year,
    c.medium,
    c.list_price_usd,
    c.artist_name.upper() AS artist_name,
    c.nationality,
    coalesce(s.sale_count, 0) AS sale_count,
    coalesce(s.total_sales_value, 0) AS total_sales_value,
    s.avg_sale_price,
    s.first_sale_date,
    s.last_sale_date,
    s.sale_count > 0 AS has_sold,
    CASE
        WHEN c.list_price_usd < {PRICE_TIER_BUDGET_MAX_USD} THEN 'budget'
        WHEN c.list_price_usd < {PRICE_TIER_MID_MAX_USD} THEN 'mid'
        ELSE 'premium'
    END AS price_tier,
    rank() OVER (ORDER BY coalesce(s.total_sales_value, 0) DESC) AS sales_rank,
    pm.primary_image,
    pm.primary_image_alt,
    coalesce(len(am.media), 0) AS media_count,
    am.media
FROM catalog c
LEFT JOIN sales_per_artwork s ON c.artwork_id = s.artwork_id
LEFT JOIN primary_media pm ON c.artwork_id = pm.artwork_id
LEFT JOIN all_media am ON c.artwork_id = am.artwork_id
ORDER BY coalesce(s.total_sales_value, 0) DESC, c.artwork_id
"""


# -----------------------------------------------------------------------------
# Sales Pipeline: sales_transform_duckdb → sales_output_duckdb
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"duckdb", "sql"},
    deps=HARVEST_DEPS,
    group_name="transform_duckdb",
)
def sales_transform_duckdb(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    paths: PathsResource,
) -> pl.DataFrame:
    """Join sales with artworks and artists using pure DuckDB SQL."""
    start_time = time.perf_counter()
    harvest_dir = paths.harvest_dir

    with duckdb.get_connection() as conn:
        result: pl.DataFrame = conn.sql(_sales_transform_sql(harvest_dir)).pl()

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    context.add_output_metadata(
        {
            "record_count": len(result),
            "columns": result.columns,
            "preview": dg.MetadataValue.md(result.head(5).to_pandas().to_markdown(index=False)),
            "unique_artworks": result["artwork_id"].n_unique(),
            "total_sales_value": float(result["sale_price_usd"].sum()),
            "date_range": f"{str(result['sale_date'].min())} to {str(result['sale_date'].max())}",
            "processing_time_ms": round(elapsed_ms, 2),
        }
    )
    context.log.info(f"Transformed {len(result)} sales records (DuckDB SQL) in {elapsed_ms:.1f}ms")
    return result


@dg.asset(
    kinds={"duckdb", "sql", "json"},
    # Depends on artworks output to ensure deterministic ordering in JSON files.
    # See assets_polars.py:sales_output_polars for detailed explanation.
    deps=["artworks_output_duckdb"],
    group_name="output_duckdb",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def sales_output_duckdb(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    sales_transform_duckdb: pl.DataFrame,
    output_paths: OutputPathsResource,
) -> pl.DataFrame:
    """Filter high-value sales using DuckDB SQL and output to JSON."""
    start_time = time.perf_counter()
    total_count = len(sales_transform_duckdb)
    sales_path = output_paths.sales_duckdb

    with duckdb.get_connection() as conn:
        # Register DataFrame and filter using SQL
        conn.register("sales", sales_transform_duckdb)
        result: pl.DataFrame = conn.sql(f"""
            SELECT * FROM sales
            WHERE sale_price_usd >= {MIN_SALE_VALUE_USD}
        """).pl()
        total_value = conn.sql(f"""
            SELECT SUM(sale_price_usd) FROM sales
            WHERE sale_price_usd >= {MIN_SALE_VALUE_USD}
        """).fetchone()[0]

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(
        result,
        sales_path,
        context,
        extra_metadata={
            "filtered_from": total_count,
            "filter_threshold": f"${MIN_SALE_VALUE_USD:,}",
            "total_value": float(total_value) if total_value else 0,
            "processing_time_ms": round(elapsed_ms, 2),
        },
    )
    context.log.info(f"Output {len(result)} high-value sales to {sales_path} in {elapsed_ms:.1f}ms")
    return result


# -----------------------------------------------------------------------------
# Artworks Pipeline: artworks_transform_duckdb → artworks_output_duckdb
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"duckdb", "sql"},
    deps=HARVEST_DEPS,
    group_name="transform_duckdb",
)
def artworks_transform_duckdb(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    paths: PathsResource,
) -> pl.DataFrame:
    """Join artworks with artists, media, and aggregate sales using pure DuckDB SQL."""
    start_time = time.perf_counter()
    harvest_dir = paths.harvest_dir

    with duckdb.get_connection() as conn:
        result: pl.DataFrame = conn.sql(_artworks_transform_sql(harvest_dir)).pl()

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    context.add_output_metadata(
        {
            "record_count": len(result),
            "artworks_sold": int(result["has_sold"].sum()),
            "artworks_unsold": int((~result["has_sold"]).sum()),
            "artworks_with_media": int((result["media_count"] > 0).sum()),
            "total_catalog_value": float(result["list_price_usd"].sum()),
            "preview": dg.MetadataValue.md(result.head(5).to_pandas().to_markdown(index=False)),
            "processing_time_ms": round(elapsed_ms, 2),
        }
    )
    context.log.info(f"Transformed {len(result)} artworks (DuckDB SQL) in {elapsed_ms:.1f}ms")
    return result


@dg.asset(
    kinds={"duckdb", "sql", "json"},
    group_name="output_duckdb",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def artworks_output_duckdb(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    artworks_transform_duckdb: pl.DataFrame,
    output_paths: OutputPathsResource,
) -> pl.DataFrame:
    """Output artwork catalog to JSON using DuckDB's native export."""
    start_time = time.perf_counter()
    artworks_path = output_paths.artworks_duckdb

    with duckdb.get_connection() as conn:
        conn.register("artworks", artworks_transform_duckdb)
        tier_counts = conn.sql("""
            SELECT price_tier, COUNT(*) as count
            FROM artworks
            GROUP BY price_tier
        """).df()
        tier_dict = dict(zip(tier_counts["price_tier"], tier_counts["count"]))

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(
        artworks_transform_duckdb,
        artworks_path,
        context,
        extra_metadata={
            "price_tier_distribution": tier_dict,
            "processing_time_ms": round(elapsed_ms, 2),
        },
    )
    context.log.info(
        f"Output {len(artworks_transform_duckdb)} artworks to {artworks_path} in {elapsed_ms:.1f}ms"
    )
    return artworks_transform_duckdb
