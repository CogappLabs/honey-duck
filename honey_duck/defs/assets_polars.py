"""Pure Polars implementation of honey-duck pipeline transforms.

This module implements the transform and output layers using only inline
Polars expressions with lazy evaluation for optimal performance.

Asset Graph:
    dlt_harvest_* (shared) ──→ sales_transform_polars ──→ sales_output_polars
                           └──→ artworks_transform_polars ──→ artworks_output_polars
"""

import time
from datetime import timedelta

import dagster as dg
import duckdb
import polars as pl

from cogapp_deps.dagster import write_json_output

from .constants import (
    MIN_SALE_VALUE_USD,
    PRICE_TIER_BUDGET_MAX_USD,
    PRICE_TIER_MID_MAX_USD,
)
from .resources import (
    ARTWORKS_OUTPUT_PATH_POLARS,
    DUCKDB_PATH,
    SALES_OUTPUT_PATH_POLARS,
)


# -----------------------------------------------------------------------------
# Dependencies
# -----------------------------------------------------------------------------

HARVEST_DEPS = [
    dg.AssetKey("dlt_harvest_sales_raw"),
    dg.AssetKey("dlt_harvest_artworks_raw"),
    dg.AssetKey("dlt_harvest_artists_raw"),
    dg.AssetKey("dlt_harvest_media"),
]


def _read_raw_table_lazy(table: str) -> pl.LazyFrame:
    """Read table from raw schema as Polars LazyFrame."""
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        return conn.sql(f"SELECT * FROM raw.{table}").pl().lazy()
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Sales Pipeline: sales_transform_polars → sales_output_polars
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"polars"},
    deps=HARVEST_DEPS,
    group_name="transform_polars",
)
def sales_transform_polars(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Join sales with artworks and artists using pure Polars lazy expressions."""
    start_time = time.perf_counter()

    sales = _read_raw_table_lazy("sales_raw")
    artworks = _read_raw_table_lazy("artworks_raw")
    artists = _read_raw_table_lazy("artists_raw")

    # Build lazy query: join -> select -> compute -> normalize -> sort
    result = (
        sales
        .join(artworks, on="artwork_id", how="left", suffix="_aw")
        .join(artists, on="artist_id", how="left", suffix="_ar")
        .select(
            "sale_id",
            "artwork_id",
            "sale_date",
            "sale_price_usd",
            "buyer_country",
            "title",
            "artist_id",
            pl.col("year").alias("artwork_year"),
            "medium",
            pl.col("price_usd").alias("list_price_usd"),
            pl.col("name").alias("artist_name"),
            "nationality",
        )
        .with_columns(
            (pl.col("sale_price_usd") - pl.col("list_price_usd")).alias("price_diff"),
            pl.when(pl.col("list_price_usd").is_null() | (pl.col("list_price_usd") == 0))
            .then(None)
            .otherwise(
                ((pl.col("sale_price_usd") - pl.col("list_price_usd")) * 100.0
                 / pl.col("list_price_usd")).round(1)
            ).alias("pct_change"),
        )
        .with_columns(
            pl.col("artist_name").str.strip_chars().str.to_uppercase()
        )
        .sort(["sale_date", "sale_id"], descending=[True, False])
        .collect()  # Single materialization point
    )

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    context.add_output_metadata({
        "record_count": len(result),
        "columns": result.columns,
        "preview": dg.MetadataValue.md(result.head(5).to_pandas().to_markdown(index=False)),
        "unique_artworks": result["artwork_id"].n_unique(),
        "total_sales_value": float(result["sale_price_usd"].sum()),
        "date_range": f"{str(result['sale_date'].min())} to {str(result['sale_date'].max())}",
        "processing_time_ms": round(elapsed_ms, 2),
    })
    context.log.info(f"Transformed {len(result)} sales records (Polars lazy) in {elapsed_ms:.1f}ms")
    return result


@dg.asset(
    kinds={"polars", "json"},
    deps=["artworks_output_polars"],
    group_name="output_polars",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def sales_output_polars(
    context: dg.AssetExecutionContext,
    sales_transform_polars: pl.DataFrame,
) -> pl.DataFrame:
    """Filter high-value sales using Polars lazy and output to JSON."""
    start_time = time.perf_counter()
    total_count = len(sales_transform_polars)

    # Use lazy for filtering
    result = (
        sales_transform_polars
        .lazy()
        .filter(pl.col("sale_price_usd") >= MIN_SALE_VALUE_USD)
        .collect()
    )

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(result, SALES_OUTPUT_PATH_POLARS, context, extra_metadata={
        "filtered_from": total_count,
        "filter_threshold": f"${MIN_SALE_VALUE_USD:,}",
        "total_value": float(result["sale_price_usd"].sum()),
        "processing_time_ms": round(elapsed_ms, 2),
    })
    context.log.info(f"Output {len(result)} high-value sales to {SALES_OUTPUT_PATH_POLARS} in {elapsed_ms:.1f}ms")
    return result


# -----------------------------------------------------------------------------
# Artworks Pipeline: artworks_transform_polars → artworks_output_polars
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"polars"},
    deps=HARVEST_DEPS,
    group_name="transform_polars",
)
def artworks_transform_polars(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Join artworks with artists, media, and aggregate sales using Polars lazy."""
    start_time = time.perf_counter()

    sales = _read_raw_table_lazy("sales_raw")
    artworks = _read_raw_table_lazy("artworks_raw")
    artists = _read_raw_table_lazy("artists_raw")
    media = _read_raw_table_lazy("media")

    # Collect artwork_ids first for filtering (needed for lazy compatibility)
    artwork_ids = artworks.select("artwork_id").collect()["artwork_id"]

    # Aggregate sales per artwork (lazy)
    sales_per_artwork = (
        sales
        .filter(pl.col("artwork_id").is_in(artwork_ids))
        .group_by("artwork_id")
        .agg(
            pl.len().alias("sale_count"),
            pl.col("sale_price_usd").sum().alias("total_sales_value"),
            pl.col("sale_price_usd").mean().round(0).alias("avg_sale_price"),
            pl.col("sale_date").min().alias("first_sale_date"),
            pl.col("sale_date").max().alias("last_sale_date"),
        )
    )

    # Build catalog: join artworks with artists (lazy)
    catalog = (
        artworks
        .join(artists, on="artist_id", how="left")
        .select(
            "artwork_id",
            "title",
            "year",
            "medium",
            pl.col("price_usd").alias("list_price_usd"),
            pl.col("name").alias("artist_name"),
            "nationality",
        )
    )

    # Primary media: sort_order = 1 (lazy)
    primary_media = (
        media
        .filter(pl.col("sort_order") == 1)
        .select(
            "artwork_id",
            pl.col("filename").alias("primary_image"),
            pl.col("alt_text").alias("primary_image_alt"),
        )
    )

    # All media aggregated as struct list (lazy)
    all_media = (
        media
        .sort("sort_order")
        .group_by("artwork_id")
        .agg(
            pl.struct(
                "sort_order", "filename", "media_type", "file_format",
                "width_px", "height_px", "file_size_kb", "alt_text"
            ).alias("media")
        )
    )

    # Final assembly: join all intermediate results (lazy until collect)
    result = (
        catalog
        .join(sales_per_artwork, on="artwork_id", how="left")
        .join(primary_media, on="artwork_id", how="left")
        .join(all_media, on="artwork_id", how="left")
        .with_columns(
            pl.col("sale_count").fill_null(0),
            pl.col("total_sales_value").fill_null(0),
        )
        .with_columns(
            (pl.col("sale_count") > 0).alias("has_sold"),
            pl.when(pl.col("list_price_usd") < PRICE_TIER_BUDGET_MAX_USD)
            .then(pl.lit("budget"))
            .when(pl.col("list_price_usd") < PRICE_TIER_MID_MAX_USD)
            .then(pl.lit("mid"))
            .otherwise(pl.lit("premium"))
            .alias("price_tier"),
            pl.col("media").list.len().fill_null(0).alias("media_count"),
        )
        .with_columns(
            pl.col("total_sales_value")
            .rank(method="ordinal", descending=True)
            .alias("sales_rank"),
            pl.col("artist_name").str.to_uppercase(),
        )
        .sort(["total_sales_value", "artwork_id"], descending=[True, False])
        .collect()  # Single materialization point
    )

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    context.add_output_metadata({
        "record_count": len(result),
        "artworks_sold": int(result["has_sold"].sum()),
        "artworks_unsold": int((~result["has_sold"]).sum()),
        "artworks_with_media": int((result["media_count"] > 0).sum()),
        "total_catalog_value": float(result["list_price_usd"].sum()),
        "preview": dg.MetadataValue.md(result.head(5).to_pandas().to_markdown(index=False)),
        "processing_time_ms": round(elapsed_ms, 2),
    })
    context.log.info(f"Transformed {len(result)} artworks (Polars lazy) in {elapsed_ms:.1f}ms")
    return result


@dg.asset(
    kinds={"polars", "json"},
    group_name="output_polars",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def artworks_output_polars(
    context: dg.AssetExecutionContext,
    artworks_transform_polars: pl.DataFrame,
) -> pl.DataFrame:
    """Output artwork catalog to JSON using pure Polars."""
    start_time = time.perf_counter()

    vc = artworks_transform_polars["price_tier"].value_counts()
    tier_counts = dict(zip(vc["price_tier"].to_list(), vc["count"].to_list()))

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(
        artworks_transform_polars, ARTWORKS_OUTPUT_PATH_POLARS, context,
        extra_metadata={
            "price_tier_distribution": tier_counts,
            "processing_time_ms": round(elapsed_ms, 2),
        }
    )
    context.log.info(f"Output {len(artworks_transform_polars)} artworks to {ARTWORKS_OUTPUT_PATH_POLARS} in {elapsed_ms:.1f}ms")
    return artworks_transform_polars
