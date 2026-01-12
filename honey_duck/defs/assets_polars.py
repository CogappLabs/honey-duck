"""Pure Polars implementation of honey-duck pipeline transforms.

This module implements the transform and output layers using only inline
Polars expressions with lazy evaluation for optimal performance.

Each transformation is split into separate assets for intermediate persistence:
- Step assets perform logical transformation units
- Transform assets combine steps for backward compatibility
- Each step writes to Parquet via PolarsParquetIOManager

Asset Graph:
    dlt_harvest_* (shared) ──→ sales_joined_polars ──→ sales_transform_polars ──→ sales_output_polars
                           └──→ artworks_catalog_polars ──┐
                           └──→ artworks_sales_agg_polars ─┼──→ artworks_transform_polars ──→ artworks_output_polars
                           └──→ artworks_media_polars ─────┘
"""

import time
from datetime import timedelta

import dagster as dg
import duckdb
import polars as pl

from cogapp_deps.dagster import (
    add_dataframe_metadata,
    track_timing,
    write_json_output,
)

from .constants import (
    MIN_SALE_VALUE_USD,
    PRICE_TIER_BUDGET_MAX_USD,
    PRICE_TIER_MID_MAX_USD,
)
from .helpers import STANDARD_HARVEST_DEPS as HARVEST_DEPS
from .resources import OutputPathsResource, PathsResource


def _read_raw_parquet_lazy(harvest_dir: str, table: str) -> pl.LazyFrame:
    """Read raw Parquet files as Polars LazyFrame using DuckDB."""
    conn = duckdb.connect(":memory:")
    try:
        return conn.sql(
            f"SELECT * FROM read_parquet('{harvest_dir}/raw/{table}/**/*.parquet')"
        ).pl().lazy()
    finally:
        conn.close()


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
) -> pl.DataFrame:
    """Join sales with artworks and artists.

    Uses ConfigurableResource injection for runtime path configuration.
    """
    with track_timing(context, "sales join"):
        harvest_dir = paths.harvest_dir

        sales = _read_raw_parquet_lazy(harvest_dir, "sales_raw")
        artworks = _read_raw_parquet_lazy(harvest_dir, "artworks_raw")
        artists = _read_raw_parquet_lazy(harvest_dir, "artists_raw")

        # Join and select columns
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
            .collect()
        )

    # Add standard metadata automatically
    add_dataframe_metadata(context, result)
    return result


@dg.asset(
    kinds={"polars"},
    group_name="transform_polars",
)
def sales_transform_polars(
    context: dg.AssetExecutionContext,
    sales_joined_polars: pl.DataFrame,
) -> pl.DataFrame:
    """Add computed columns and normalize artist names."""
    start_time = time.perf_counter()

    # Add price metrics, normalize artist names, and sort (chained for readability)
    result = (
        sales_joined_polars
        .lazy()
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
        .collect()
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
    context.log.info(f"Transformed {len(result)} sales records in {elapsed_ms:.1f}ms")
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
    sales_transform_polars: pl.DataFrame,
    output_paths: OutputPathsResource,
) -> pl.DataFrame:
    """Filter high-value sales using Polars lazy and output to JSON."""
    start_time = time.perf_counter()
    total_count = len(sales_transform_polars)
    sales_path = output_paths.sales_polars

    # Use lazy for filtering
    result = (
        sales_transform_polars
        .lazy()
        .filter(pl.col("sale_price_usd") >= MIN_SALE_VALUE_USD)
        .collect()
    )

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(result, sales_path, context, extra_metadata={
        "filtered_from": total_count,
        "filter_threshold": f"${MIN_SALE_VALUE_USD:,}",
        "total_value": float(result["sale_price_usd"].sum()),
        "processing_time_ms": round(elapsed_ms, 2),
    })
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
) -> pl.DataFrame:
    """Build artwork catalog by joining artworks with artists."""
    start_time = time.perf_counter()
    harvest_dir = paths.harvest_dir

    artworks = _read_raw_parquet_lazy(harvest_dir, "artworks_raw")
    artists = _read_raw_parquet_lazy(harvest_dir, "artists_raw")

    result = (
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
        .collect()
    )

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    context.add_output_metadata({
        "record_count": len(result),
        "columns": result.columns,
        "preview": dg.MetadataValue.md(result.head(5).to_pandas().to_markdown(index=False)),
        "processing_time_ms": round(elapsed_ms, 2),
    })
    context.log.info(f"Built catalog for {len(result)} artworks in {elapsed_ms:.1f}ms")
    return result


@dg.asset(
    kinds={"polars"},
    deps=HARVEST_DEPS,
    group_name="transform_polars",
)
def artworks_sales_agg_polars(
    context: dg.AssetExecutionContext,
    paths: PathsResource,
    artworks_catalog_polars: pl.DataFrame,
) -> pl.DataFrame:
    """Aggregate sales metrics per artwork."""
    start_time = time.perf_counter()
    harvest_dir = paths.harvest_dir

    sales = _read_raw_parquet_lazy(harvest_dir, "sales_raw")

    # Get artwork_ids from catalog for filtering
    artwork_ids = artworks_catalog_polars["artwork_id"]

    result = (
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
        .collect()
    )

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    context.add_output_metadata({
        "record_count": len(result),
        "columns": result.columns,
        "preview": dg.MetadataValue.md(result.head(5).to_pandas().to_markdown(index=False)),
        "processing_time_ms": round(elapsed_ms, 2),
    })
    context.log.info(f"Aggregated sales for {len(result)} artworks in {elapsed_ms:.1f}ms")
    return result


@dg.asset(
    kinds={"polars"},
    deps=HARVEST_DEPS,
    group_name="transform_polars",
)
def artworks_media_polars(
    context: dg.AssetExecutionContext,
    paths: PathsResource,
) -> pl.DataFrame:
    """Prepare media data: primary image and all media aggregated."""
    start_time = time.perf_counter()
    harvest_dir = paths.harvest_dir

    media = _read_raw_parquet_lazy(harvest_dir, "media")

    # Primary media (sort_order = 1) and all media aggregated (chained for readability)
    primary_media = (
        media
        .filter(pl.col("sort_order") == 1)
        .select(
            "artwork_id",
            pl.col("filename").alias("primary_image"),
            pl.col("alt_text").alias("primary_image_alt"),
        )
    )

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

    # Join primary with aggregated
    result = (
        primary_media
        .join(all_media, on="artwork_id", how="outer")
        .collect()
    )

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    context.add_output_metadata({
        "record_count": len(result),
        "columns": result.columns,
        "preview": dg.MetadataValue.md(result.head(5).to_pandas().to_markdown(index=False)),
        "processing_time_ms": round(elapsed_ms, 2),
    })
    context.log.info(f"Prepared media for {len(result)} artworks in {elapsed_ms:.1f}ms")
    return result


@dg.asset(
    kinds={"polars"},
    group_name="transform_polars",
)
def artworks_transform_polars(
    context: dg.AssetExecutionContext,
    artworks_catalog_polars: pl.DataFrame,
    artworks_sales_agg_polars: pl.DataFrame,
    artworks_media_polars: pl.DataFrame,
) -> pl.DataFrame:
    """Join catalog, sales aggregates, and media with computed fields."""
    start_time = time.perf_counter()

    # Join all intermediate results and add computed fields (chained for readability)
    result = (
        artworks_catalog_polars
        .lazy()
        .join(artworks_sales_agg_polars.lazy(), on="artwork_id", how="left")
        .join(artworks_media_polars.lazy(), on="artwork_id", how="left")
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
    output_paths: OutputPathsResource,
) -> pl.DataFrame:
    """Output artwork catalog to JSON using pure Polars."""
    start_time = time.perf_counter()
    artworks_path = output_paths.artworks_polars

    vc = artworks_transform_polars["price_tier"].value_counts()
    tier_counts = dict(zip(vc["price_tier"].to_list(), vc["count"].to_list()))

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(
        artworks_transform_polars, artworks_path, context,
        extra_metadata={
            "price_tier_distribution": tier_counts,
            "processing_time_ms": round(elapsed_ms, 2),
        }
    )
    context.log.info(f"Output {len(artworks_transform_polars)} artworks to {artworks_path} in {elapsed_ms:.1f}ms")
    return artworks_transform_polars
