"""Multi-asset implementation for tightly coupled intermediate steps.

This module demonstrates the multi-asset pattern where one function produces
multiple related assets. Each intermediate step is yielded separately but
computed together in a single function.

Key features:
- Each output has `kinds` for better UI filtering
- Each output has `description` for documentation
- IO manager persists each intermediate step
- External deps declared via `deps` parameter

Note: `internal_asset_deps` cannot be combined with external `deps` in Dagster.
The internal dependency chain (joined → transform) is implicit via yield order
but won't show as edges in the asset graph.

Use this pattern when:
- Intermediate steps are tightly coupled and should run together
- You want each step persisted independently (via IO manager)
- You need individual asset tracking in the lineage graph
- Steps must complete together (can't selectively materialize)

For independent steps, use split assets (assets_polars.py).
For single-asset observability, use graph-backed ops (assets_polars_ops.py).

Asset Graph:
    dlt_harvest_* ──→ sales_joined_polars_multi ──→ sales_output_polars_multi
                  │   sales_transform_polars_multi ─┘
                  └──→ artworks_catalog_polars_multi
                      artworks_sales_agg_polars_multi
                      artworks_media_polars_multi
                      artworks_transform_polars_multi ──→ artworks_output_polars_multi
"""

import time
from datetime import timedelta
from typing import Iterator

import dagster as dg
import polars as pl

from cogapp_libs.dagster import read_harvest_table_lazy, write_json_output

from ..shared.constants import (
    MIN_SALE_VALUE_USD,
    PRICE_TIER_BUDGET_MAX_USD,
    PRICE_TIER_MID_MAX_USD,
)
from ..shared.helpers import STANDARD_HARVEST_DEPS as HARVEST_DEPS
from ..shared.resources import OutputPathsResource, PathsResource


# -----------------------------------------------------------------------------
# Sales Pipeline Multi-Asset
# -----------------------------------------------------------------------------


@dg.multi_asset(
    outs={
        "sales_joined_polars_multi": dg.AssetOut(
            kinds={"polars"},
            io_manager_key="io_manager",
            description="Joined sales with artworks and artists",
        ),
        "sales_transform_polars_multi": dg.AssetOut(
            kinds={"polars"},
            io_manager_key="io_manager",
            description="Filtered and enriched sales data",
        ),
    },
    deps=HARVEST_DEPS,
    group_name="transform_polars_multi",
)
def sales_pipeline_multi(
    context: dg.AssetExecutionContext,
    paths: PathsResource,
) -> Iterator[dg.Output]:
    """Multi-asset: Sales pipeline with intermediate persistence.

    Produces two assets in one function:
    1. sales_joined_polars_multi - Joined sales data
    2. sales_transform_polars_multi - Filtered and enriched sales

    Each output is persisted via IO manager and tracked separately in lineage.
    Internal dependencies show joined → transform in the asset graph.
    """
    harvest_dir = paths.harvest_dir

    # Step 1: Join sales with artworks and artists
    context.log.info("Loading and joining sales data...")
    # Native Polars scan_parquet - no DuckDB overhead
    sales = read_harvest_table_lazy(harvest_dir, "sales_raw")
    artworks = read_harvest_table_lazy(harvest_dir, "artworks_raw")
    artists = read_harvest_table_lazy(harvest_dir, "artists_raw")

    joined = (
        sales.join(artworks, on="artwork_id", suffix="_artworks")
        .join(artists, on="artist_id", suffix="_artists")
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
            pl.col("name").str.strip_chars().str.to_uppercase().alias("artist_name"),
            "nationality",
        )
        .collect()
    )

    context.log.info(f"Joined {len(joined):,} sales records")

    # Yield first asset
    yield dg.Output(
        joined,
        output_name="sales_joined_polars_multi",
        metadata={
            "record_count": len(joined),
            "preview": dg.MetadataValue.md(joined.head(5).to_pandas().to_markdown(index=False)),
        },
    )

    # Step 2: Transform - filter and add computed columns
    context.log.info("Transforming sales data...")
    transformed = joined.filter(pl.col("sale_price_usd") > MIN_SALE_VALUE_USD).with_columns(
        [
            (pl.col("sale_price_usd") - pl.col("list_price_usd")).alias("price_diff"),
            (
                (pl.col("sale_price_usd") - pl.col("list_price_usd"))
                / pl.col("list_price_usd")
                * 100
            ).alias("pct_change"),
        ]
    )

    context.log.info(f"Filtered to {len(transformed):,} high-value sales")

    # Yield second asset
    yield dg.Output(
        transformed,
        output_name="sales_transform_polars_multi",
        metadata={
            "record_count": len(transformed),
            "high_value_threshold": dg.MetadataValue.text(f"${MIN_SALE_VALUE_USD:,}"),
            "preview": dg.MetadataValue.md(
                transformed.head(5).to_pandas().to_markdown(index=False)
            ),
        },
    )


# -----------------------------------------------------------------------------
# Artworks Pipeline Multi-Asset
# -----------------------------------------------------------------------------


@dg.multi_asset(
    outs={
        "artworks_catalog_polars_multi": dg.AssetOut(
            kinds={"polars"},
            io_manager_key="io_manager",
            description="Base artwork catalog joined with artists",
        ),
        "artworks_sales_agg_polars_multi": dg.AssetOut(
            kinds={"polars"},
            io_manager_key="io_manager",
            description="Aggregated sales metrics per artwork",
        ),
        "artworks_media_polars_multi": dg.AssetOut(
            kinds={"polars"},
            io_manager_key="io_manager",
            description="Media information per artwork",
        ),
        "artworks_transform_polars_multi": dg.AssetOut(
            kinds={"polars"},
            io_manager_key="io_manager",
            description="Complete enriched artwork catalog",
        ),
    },
    deps=HARVEST_DEPS,
    group_name="transform_polars_multi",
)
def artworks_pipeline_multi(
    context: dg.AssetExecutionContext,
    paths: PathsResource,
) -> Iterator[dg.Output]:
    """Multi-asset: Artworks pipeline with multiple intermediate steps.

    Produces four assets in one function:
    1. artworks_catalog_polars_multi - Base artwork catalog
    2. artworks_sales_agg_polars_multi - Sales aggregations
    3. artworks_media_polars_multi - Media information
    4. artworks_transform_polars_multi - Complete enriched catalog

    Internal dependencies show catalog/sales_agg/media → transform in the asset graph.
    """
    harvest_dir = paths.harvest_dir

    # Step 1: Create base catalog
    context.log.info("Creating artwork catalog...")
    # Native Polars scan_parquet - no DuckDB overhead
    artworks = read_harvest_table_lazy(harvest_dir, "artworks_raw")
    artists = read_harvest_table_lazy(harvest_dir, "artists_raw")

    catalog = (
        artworks.join(artists, on="artist_id", suffix="_artists")
        .select(
            "artwork_id",
            "title",
            "year",
            "medium",
            pl.col("price_usd").alias("list_price_usd"),
            pl.col("name").str.strip_chars().str.to_uppercase().alias("artist_name"),
            "nationality",
        )
        .collect()
    )

    context.log.info(f"Created catalog with {len(catalog):,} artworks")
    yield dg.Output(
        catalog,
        output_name="artworks_catalog_polars_multi",
        metadata={"record_count": len(catalog)},
    )

    # Step 2: Aggregate sales data
    context.log.info("Aggregating sales data...")
    sales = read_harvest_table_lazy(harvest_dir, "sales_raw")

    sales_agg = (
        sales.group_by("artwork_id")
        .agg(
            [
                pl.len().alias("sale_count"),
                pl.sum("sale_price_usd").alias("total_sales_value"),
                pl.mean("sale_price_usd").alias("avg_sale_price"),
                pl.min("sale_date").alias("first_sale_date"),
                pl.max("sale_date").alias("last_sale_date"),
            ]
        )
        .collect()
    )

    context.log.info(f"Aggregated sales for {len(sales_agg):,} artworks")
    yield dg.Output(
        sales_agg,
        output_name="artworks_sales_agg_polars_multi",
        metadata={"record_count": len(sales_agg)},
    )

    # Step 3: Join media information
    context.log.info("Loading media information...")
    media = read_harvest_table_lazy(harvest_dir, "media")

    # Get primary image (sort_order = 1) per artwork
    media_agg = (
        media.with_columns(
            pl.when(pl.col("sort_order") == 1)
            .then(pl.col("filename"))
            .otherwise(None)
            .alias("primary_image"),
            pl.when(pl.col("sort_order") == 1)
            .then(pl.col("alt_text"))
            .otherwise(None)
            .alias("primary_image_alt"),
        )
        .group_by("artwork_id")
        .agg(
            [
                pl.col("primary_image").drop_nulls().first().alias("primary_image"),
                pl.col("primary_image_alt").drop_nulls().first().alias("primary_image_alt"),
                pl.len().alias("media_count"),
            ]
        )
        .collect()
    )

    context.log.info(f"Loaded media for {len(media_agg):,} artworks")
    yield dg.Output(
        media_agg,
        output_name="artworks_media_polars_multi",
        metadata={"record_count": len(media_agg)},
    )

    # Step 4: Combine all data
    context.log.info("Combining all artwork data...")
    combined = (
        catalog.join(sales_agg, on="artwork_id", how="left")
        .join(media_agg, on="artwork_id", how="left")
        .with_columns(
            [
                pl.col("sale_count").fill_null(0),
                pl.col("total_sales_value").fill_null(0.0),
                (pl.col("sale_count") > 0).alias("has_sold"),
                pl.when(pl.col("list_price_usd") <= PRICE_TIER_BUDGET_MAX_USD)
                .then(pl.lit("budget"))
                .when(pl.col("list_price_usd") <= PRICE_TIER_MID_MAX_USD)
                .then(pl.lit("mid"))
                .otherwise(pl.lit("premium"))
                .alias("price_tier"),
            ]
        )
        .with_columns(
            pl.col("total_sales_value")
            .rank(method="ordinal", descending=True)
            .over("price_tier")
            .alias("sales_rank")
        )
    )

    context.log.info(f"Final catalog has {len(combined):,} artworks")
    yield dg.Output(
        combined,
        output_name="artworks_transform_polars_multi",
        metadata={
            "record_count": len(combined),
            "artworks_with_sales": int(combined["has_sold"].sum()),
            "preview": dg.MetadataValue.md(combined.head(5).to_pandas().to_markdown(index=False)),
        },
    )


# -----------------------------------------------------------------------------
# Output Assets (standard single assets that depend on multi-asset outputs)
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"polars", "json"},
    group_name="output_polars_multi",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def sales_output_polars_multi(
    context: dg.AssetExecutionContext,
    sales_transform_polars_multi: pl.DataFrame,
    output_paths: OutputPathsResource,
) -> pl.DataFrame:
    """Output: High-value sales above minimum threshold.

    Depends on: sales_transform_polars_multi (from multi-asset)
    """
    start_time = time.perf_counter()
    sales_path = output_paths.sales_polars_multi

    result = sales_transform_polars_multi.filter(pl.col("sale_price_usd") > MIN_SALE_VALUE_USD)

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(
        result,
        sales_path,
        context,
        extra_metadata={
            "min_sale_value": f"${MIN_SALE_VALUE_USD:,}",
            "processing_time_ms": round(elapsed_ms, 2),
        },
    )
    context.log.info(f"Output {len(result)} high-value sales to {sales_path} in {elapsed_ms:.1f}ms")
    return result


@dg.asset(
    kinds={"polars", "json"},
    group_name="output_polars_multi",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def artworks_output_polars_multi(
    context: dg.AssetExecutionContext,
    artworks_transform_polars_multi: pl.DataFrame,
    output_paths: OutputPathsResource,
) -> pl.DataFrame:
    """Output: Complete artworks catalog to JSON.

    Depends on: artworks_transform_polars_multi (from multi-asset)
    """
    start_time = time.perf_counter()
    artworks_path = output_paths.artworks_polars_multi

    # Output all artworks sorted by total sales value
    result = artworks_transform_polars_multi.sort("total_sales_value", descending=True)

    tier_counts = result.group_by("price_tier").agg(pl.len().alias("count"))
    tier_dist = dict(
        zip(tier_counts["price_tier"].to_list(), tier_counts["count"].to_list(), strict=False)
    )

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(
        result,
        artworks_path,
        context,
        extra_metadata={
            "tier_distribution": tier_dist,
            "processing_time_ms": round(elapsed_ms, 2),
        },
    )
    context.log.info(f"Output {len(result)} artworks to {artworks_path} in {elapsed_ms:.1f}ms")
    return result
