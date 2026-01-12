"""Pure Polars implementation of honey-duck pipeline transforms.

This module implements the transform and output layers using only inline
Polars expressions with lazy evaluation for optimal performance.

Performance optimizations:
- Native Polars scan_parquet: Direct Parquet reading without DuckDB overhead
- Streaming writes: LazyFrame returns enable sink_parquet() via IO manager
- Semi-joins: Avoid early materialization for filtering

Asset Graph:
    dlt_harvest_* (shared) ──→ sales_joined_polars ──→ sales_transform_polars ──→ sales_output_polars
                           └──→ artworks_catalog_polars ──┐
                           └──→ artworks_sales_agg_polars ─┼──→ artworks_transform_polars ──→ artworks_output_polars
                           └──→ artworks_media_polars ─────┘
"""

import time
from datetime import timedelta

import dagster as dg
import polars as pl

from cogapp_deps.dagster import read_harvest_table_lazy, write_json_output

from .constants import (
    MIN_SALE_VALUE_USD,
    PRICE_TIER_BUDGET_MAX_USD,
    PRICE_TIER_MID_MAX_USD,
)
from .helpers import STANDARD_HARVEST_DEPS as HARVEST_DEPS
from .resources import OutputPathsResource, PathsResource


# -----------------------------------------------------------------------------
# Sales Pipeline: sales_joined_polars → sales_transform_polars → sales_output_polars
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"polars"},
    deps=HARVEST_DEPS,
    group_name="transform_polars",
)
def sales_joined_polars(
    context: dg.AssetExecutionContext,
    paths: PathsResource,
) -> pl.LazyFrame:
    """Join sales with artworks and artists (streaming via sink_parquet).

    Returns LazyFrame to enable streaming writes - the IO manager uses
    sink_parquet() instead of write_parquet(), never materializing
    the full DataFrame in memory.
    """
    harvest_dir = paths.harvest_dir

    # Native Polars scan_parquet - no DuckDB overhead
    sales = read_harvest_table_lazy(harvest_dir, "sales_raw")
    artworks = read_harvest_table_lazy(harvest_dir, "artworks_raw")
    artists = read_harvest_table_lazy(harvest_dir, "artists_raw")

    # Join and select columns - stays lazy, no .collect()
    result = (
        sales.join(artworks, on="artwork_id", how="left", suffix="_aw")
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
    )

    # Add schema metadata (available without collecting)
    context.add_output_metadata(
        {
            "columns": result.collect_schema().names(),
            "streaming": True,
        }
    )
    context.log.info("Returning LazyFrame for streaming write via sink_parquet")
    return result


@dg.asset(
    kinds={"polars"},
    group_name="transform_polars",
)
def sales_transform_polars(
    context: dg.AssetExecutionContext,
    sales_joined_polars: pl.LazyFrame,
) -> pl.LazyFrame:
    """Add computed columns, normalize artist names, and filter high-value sales.

    Returns LazyFrame for streaming - filter moved here from output asset.
    """
    start_time = time.perf_counter()

    # Add price metrics, normalize artist names, filter, and sort
    # Input is already LazyFrame from IO manager's scan_parquet
    result = (
        sales_joined_polars.with_columns(
            (pl.col("sale_price_usd") - pl.col("list_price_usd")).alias("price_diff"),
            pl.when(pl.col("list_price_usd").is_null() | (pl.col("list_price_usd") == 0))
            .then(None)
            .otherwise(
                (
                    (pl.col("sale_price_usd") - pl.col("list_price_usd"))
                    * 100.0
                    / pl.col("list_price_usd")
                ).round(1)
            )
            .alias("pct_change"),
        )
        .with_columns(pl.col("artist_name").str.strip_chars().str.to_uppercase())
        .sort(["sale_date", "sale_id"], descending=[True, False])
    )

    # Add schema metadata (available without collecting)
    elapsed_ms = (time.perf_counter() - start_time) * 1000
    context.add_output_metadata(
        {
            "columns": result.collect_schema().names(),
            "streaming": True,
            "processing_time_ms": round(elapsed_ms, 2),
        }
    )
    context.log.info(f"Returning LazyFrame for streaming write in {elapsed_ms:.1f}ms")
    return result


@dg.asset(
    kinds={"polars", "json"},
    # Depends on artworks_output_polars to ensure deterministic ordering:
    # Without this, Dagster may materialize outputs in parallel with non-deterministic
    # row ordering in JSON files (due to parallel execution). This artificial dependency
    # ensures artworks is written first, making sales output consistent across runs.
    deps=["artworks_output_polars"],
    group_name="output_polars",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def sales_output_polars(
    context: dg.AssetExecutionContext,
    sales_transform_polars: pl.LazyFrame,
    output_paths: OutputPathsResource,
) -> pl.DataFrame:
    """Filter high-value sales and output to JSON.

    Accepts LazyFrame, applies final filter, collects for JSON output.
    """
    start_time = time.perf_counter()
    sales_path = output_paths.sales_polars

    # Filter and collect - single materialization point
    result = sales_transform_polars.filter(pl.col("sale_price_usd") >= MIN_SALE_VALUE_USD).collect()

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(
        result,
        sales_path,
        context,
        extra_metadata={
            "filter_threshold": f"${MIN_SALE_VALUE_USD:,}",
            "total_value": float(result["sale_price_usd"].sum()),
            "processing_time_ms": round(elapsed_ms, 2),
        },
    )
    context.log.info(f"Output {len(result)} high-value sales to {sales_path} in {elapsed_ms:.1f}ms")
    return result


# -----------------------------------------------------------------------------
# Artworks Pipeline: artworks_*_polars → artworks_transform_polars → artworks_output_polars
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"polars"},
    deps=HARVEST_DEPS,
    group_name="transform_polars",
)
def artworks_catalog_polars(
    context: dg.AssetExecutionContext,
    paths: PathsResource,
) -> pl.LazyFrame:
    """Build artwork catalog by joining artworks with artists.

    Returns LazyFrame for streaming writes.
    """
    start_time = time.perf_counter()
    harvest_dir = paths.harvest_dir

    # Native Polars scan_parquet - no DuckDB overhead
    artworks = read_harvest_table_lazy(harvest_dir, "artworks_raw")
    artists = read_harvest_table_lazy(harvest_dir, "artists_raw")

    result = artworks.join(artists, on="artist_id", how="left").select(
        "artwork_id",
        "title",
        "year",
        "medium",
        pl.col("price_usd").alias("list_price_usd"),
        pl.col("name").alias("artist_name"),
        "nationality",
    )

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    context.add_output_metadata(
        {
            "columns": result.collect_schema().names(),
            "streaming": True,
            "processing_time_ms": round(elapsed_ms, 2),
        }
    )
    context.log.info(f"Built catalog LazyFrame in {elapsed_ms:.1f}ms")
    return result


@dg.asset(
    kinds={"polars"},
    deps=HARVEST_DEPS,
    group_name="transform_polars",
)
def artworks_sales_agg_polars(
    context: dg.AssetExecutionContext,
    paths: PathsResource,
    artworks_catalog_polars: pl.LazyFrame,
) -> pl.LazyFrame:
    """Aggregate sales metrics per artwork.

    Uses semi-join instead of .is_in() to avoid early materialization.
    Returns LazyFrame for streaming writes.
    """
    start_time = time.perf_counter()
    harvest_dir = paths.harvest_dir

    # Native Polars scan_parquet - no DuckDB overhead
    sales = read_harvest_table_lazy(harvest_dir, "sales_raw")

    # Use semi-join to filter sales to only artworks in catalog
    # This avoids early materialization of artwork_ids
    result = (
        sales.join(
            artworks_catalog_polars.select("artwork_id"),
            on="artwork_id",
            how="semi",  # Keep only sales for artworks in catalog
        )
        .group_by("artwork_id")
        .agg(
            pl.len().alias("sale_count"),
            pl.col("sale_price_usd").sum().alias("total_sales_value"),
            pl.col("sale_price_usd").mean().round(0).alias("avg_sale_price"),
            pl.col("sale_date").min().alias("first_sale_date"),
            pl.col("sale_date").max().alias("last_sale_date"),
        )
    )

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    context.add_output_metadata(
        {
            "columns": result.collect_schema().names(),
            "streaming": True,
            "processing_time_ms": round(elapsed_ms, 2),
        }
    )
    context.log.info(f"Built sales aggregation LazyFrame in {elapsed_ms:.1f}ms")
    return result


@dg.asset(
    kinds={"polars"},
    deps=HARVEST_DEPS,
    group_name="transform_polars",
)
def artworks_media_polars(
    context: dg.AssetExecutionContext,
    paths: PathsResource,
) -> pl.LazyFrame:
    """Prepare media data: primary image and all media aggregated.

    Returns LazyFrame for streaming writes.
    """
    start_time = time.perf_counter()
    harvest_dir = paths.harvest_dir

    # Native Polars scan_parquet - no DuckDB overhead
    media = read_harvest_table_lazy(harvest_dir, "media")

    # Primary media (sort_order = 1) and all media aggregated (chained for readability)
    primary_media = media.filter(pl.col("sort_order") == 1).select(
        "artwork_id",
        pl.col("filename").alias("primary_image"),
        pl.col("alt_text").alias("primary_image_alt"),
    )

    all_media = (
        media.sort("sort_order")
        .group_by("artwork_id")
        .agg(
            pl.struct(
                "sort_order",
                "filename",
                "media_type",
                "file_format",
                "width_px",
                "height_px",
                "file_size_kb",
                "alt_text",
            ).alias("media")
        )
    )

    # Join primary with aggregated - stays lazy
    result = primary_media.join(all_media, on="artwork_id", how="outer")

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    context.add_output_metadata(
        {
            "columns": result.collect_schema().names(),
            "streaming": True,
            "processing_time_ms": round(elapsed_ms, 2),
        }
    )
    context.log.info(f"Built media LazyFrame in {elapsed_ms:.1f}ms")
    return result


@dg.asset(
    kinds={"polars"},
    group_name="transform_polars",
)
def artworks_transform_polars(
    context: dg.AssetExecutionContext,
    artworks_catalog_polars: pl.LazyFrame,
    artworks_sales_agg_polars: pl.LazyFrame,
    artworks_media_polars: pl.LazyFrame,
) -> pl.LazyFrame:
    """Join catalog, sales aggregates, and media with computed fields.

    Returns LazyFrame for streaming writes.
    """
    start_time = time.perf_counter()

    # Join all intermediate results and add computed fields (chained for readability)
    # All inputs are LazyFrames - stays lazy throughout
    result = (
        artworks_catalog_polars.join(artworks_sales_agg_polars, on="artwork_id", how="left")
        .join(artworks_media_polars, on="artwork_id", how="left")
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
            pl.col("total_sales_value").rank(method="ordinal", descending=True).alias("sales_rank"),
            pl.col("artist_name").str.to_uppercase(),
        )
        .sort(["total_sales_value", "artwork_id"], descending=[True, False])
    )

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    context.add_output_metadata(
        {
            "columns": result.collect_schema().names(),
            "streaming": True,
            "processing_time_ms": round(elapsed_ms, 2),
        }
    )
    context.log.info(f"Built transform LazyFrame in {elapsed_ms:.1f}ms")
    return result


@dg.asset(
    kinds={"polars", "json"},
    group_name="output_polars",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def artworks_output_polars(
    context: dg.AssetExecutionContext,
    artworks_transform_polars: pl.LazyFrame,
    output_paths: OutputPathsResource,
) -> pl.DataFrame:
    """Output artwork catalog to JSON.

    Accepts LazyFrame, collects for JSON output - single materialization point.
    """
    start_time = time.perf_counter()
    artworks_path = output_paths.artworks_polars

    # Single collect point for entire pipeline
    result = artworks_transform_polars.collect()

    vc = result["price_tier"].value_counts()
    tier_counts = dict(zip(vc["price_tier"].to_list(), vc["count"].to_list()))

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(
        result,
        artworks_path,
        context,
        extra_metadata={
            "price_tier_distribution": tier_counts,
            "processing_time_ms": round(elapsed_ms, 2),
        },
    )
    context.log.info(f"Output {len(result)} artworks to {artworks_path} in {elapsed_ms:.1f}ms")
    return result
