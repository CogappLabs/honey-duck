"""Graph-backed asset implementation using ops for transform layer.

This module demonstrates a hybrid pattern:
- Harvest layer: Regular assets (dlt-based, shared with other pipelines)
- Transform layer: Ops composed in @graph_asset
- Output layer: Regular assets

The @graph_asset pattern gives op-level observability (retries, step logs)
while maintaining proper asset dependencies in the lineage graph.

Pattern for declaring asset dependencies with @graph_asset:
1. Use `ins` with `AssetIn(key=..., dagster_type=Nothing)` to declare dependencies
2. Accept Nothing-typed parameters in the graph function
3. First op uses `ins={"_dep": In(Nothing)}` to accept the Nothing values
4. Pass the Nothing values from graph function to first op

This establishes asset lineage (shows in UI) while ops read data directly
from files (no data flows through the Nothing dependency chain).

Asset Graph:
    dlt_harvest_* ──→ sales_transform_polars_ops ──→ sales_output_polars_ops
                  └──→ artworks_transform_polars_ops ──→ artworks_output_polars_ops
"""

import time
from datetime import timedelta
from typing import cast

import dagster as dg
from dagster import Nothing
import polars as pl

from cogapp_libs.dagster import (
    read_harvest_table_lazy,
    track_timing,
    write_json_output,
)

from ..shared.constants import (
    MIN_SALE_VALUE_USD,
    PRICE_TIER_BUDGET_MAX_USD,
    PRICE_TIER_MID_MAX_USD,
)
from ..shared.resources import OutputPathsResource, HARVEST_DIR


# Default harvest directory for ops (ops don't get resource injection)
_DEFAULT_HARVEST_DIR = str(HARVEST_DIR)


# -----------------------------------------------------------------------------
# Sales Pipeline Ops
# -----------------------------------------------------------------------------


@dg.op(ins={"_dep1": dg.In(Nothing), "_dep2": dg.In(Nothing), "_dep3": dg.In(Nothing)})
def join_sales_data(context: dg.OpExecutionContext) -> pl.DataFrame:
    """Op: Join sales with artworks and artists."""
    with track_timing(context, "join"):
        # Native Polars scan_parquet - no DuckDB overhead
        sales = read_harvest_table_lazy(_DEFAULT_HARVEST_DIR, "sales_raw")
        artworks = read_harvest_table_lazy(_DEFAULT_HARVEST_DIR, "artworks_raw")
        artists = read_harvest_table_lazy(_DEFAULT_HARVEST_DIR, "artists_raw")

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
            .collect()
        )

    context.log.info(
        f"[join_sales_data] Joined {len(result):,} sales records "
        f"across {result['artwork_id'].n_unique():,} artworks"
    )
    return result


@dg.op
def add_sales_metrics(context: dg.OpExecutionContext, joined_data: pl.DataFrame) -> pl.DataFrame:
    """Op: Add computed columns and normalize artist names."""
    start_time = time.perf_counter()

    result = (
        joined_data.lazy()
        .with_columns(
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
        .collect()
    )

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    total_value = float(result["sale_price_usd"].sum())
    avg_price = total_value / len(result)
    date_range = f"{str(result['sale_date'].min())} to {str(result['sale_date'].max())}"

    context.log.info(
        f"[add_sales_metrics] Transformed {len(result):,} sales | "
        f"Total value: ${total_value:,.0f} | Avg: ${avg_price:,.0f} | "
        f"Date range: {date_range} | {elapsed_ms:.1f}ms"
    )
    return result


# -----------------------------------------------------------------------------
# Sales Graph-Backed Asset (ops composed via @graph_asset)
# -----------------------------------------------------------------------------


@dg.graph_asset(
    kinds={"polars"},
    group_name="transform_polars_ops",
    ins={
        "_sales": dg.AssetIn(key="dlt_harvest_sales_raw", dagster_type=Nothing),
        "_artworks": dg.AssetIn(key="dlt_harvest_artworks_raw", dagster_type=Nothing),
        "_artists": dg.AssetIn(key="dlt_harvest_artists_raw", dagster_type=Nothing),
    },
)
def sales_transform_polars_ops(_sales: None, _artworks: None, _artists: None) -> pl.DataFrame:
    """Transform sales data using ops for detailed observability.

    Graph-backed asset with Nothing-typed inputs:
    - Declares dependencies on harvest assets (shows in lineage graph)
    - Ops read data directly from Parquet files
    - Op-level logs and potential retries
    """
    # Wire Nothing inputs to first op to establish dependency chain
    joined = join_sales_data(_dep1=_sales, _dep2=_artworks, _dep3=_artists)
    transformed = add_sales_metrics(joined)
    return cast(pl.DataFrame, transformed)


# -----------------------------------------------------------------------------
# Artworks Pipeline Ops
# -----------------------------------------------------------------------------


@dg.op(
    ins={
        "_dep1": dg.In(Nothing),
        "_dep2": dg.In(Nothing),
        "_dep3": dg.In(Nothing),
        "_dep4": dg.In(Nothing),
    }
)
def build_artwork_catalog(context: dg.OpExecutionContext) -> pl.DataFrame:
    """Op: Build artwork catalog by joining artworks with artists."""
    start_time = time.perf_counter()

    # Native Polars scan_parquet - no DuckDB overhead
    artworks = read_harvest_table_lazy(_DEFAULT_HARVEST_DIR, "artworks_raw")
    artists = read_harvest_table_lazy(_DEFAULT_HARVEST_DIR, "artists_raw")

    result = (
        artworks.join(artists, on="artist_id", how="left")
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
    context.log.info(
        f"[build_artwork_catalog] Built catalog for {len(result):,} artworks "
        f"across {result['artist_name'].n_unique():,} artists in {elapsed_ms:.1f}ms"
    )
    return result


@dg.op
def aggregate_sales_metrics(context: dg.OpExecutionContext, catalog: pl.DataFrame) -> pl.DataFrame:
    """Op: Aggregate sales metrics per artwork."""
    start_time = time.perf_counter()

    # Native Polars scan_parquet - no DuckDB overhead
    sales = read_harvest_table_lazy(_DEFAULT_HARVEST_DIR, "sales_raw")

    # Use semi-join to filter sales to only artworks in catalog (avoids early materialization)
    result = (
        sales.join(catalog.lazy().select("artwork_id"), on="artwork_id", how="semi")
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
    total_sales_value = float(result["total_sales_value"].sum())
    context.log.info(
        f"[aggregate_sales_metrics] Aggregated sales for {len(result):,} artworks | "
        f"Total value: ${total_sales_value:,.0f} | {elapsed_ms:.1f}ms"
    )
    return result


@dg.op(ins={"_dep": dg.In(Nothing)})
def prepare_media_data(context: dg.OpExecutionContext) -> pl.DataFrame:
    """Op: Prepare media data - primary image and all media aggregated."""
    start_time = time.perf_counter()

    # Native Polars scan_parquet - no DuckDB overhead
    media = read_harvest_table_lazy(_DEFAULT_HARVEST_DIR, "media")

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

    result = primary_media.join(all_media, on="artwork_id", how="full").collect()

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    context.log.info(
        f"[prepare_media_data] Prepared media for {len(result):,} artworks in {elapsed_ms:.1f}ms"
    )
    return result


@dg.op
def join_and_enrich_artworks(
    context: dg.OpExecutionContext,
    catalog: pl.DataFrame,
    sales_agg: pl.DataFrame,
    media: pl.DataFrame,
) -> pl.DataFrame:
    """Op: Join catalog, sales aggregates, and media with computed fields."""
    start_time = time.perf_counter()

    result = (
        catalog.lazy()
        .join(sales_agg.lazy(), on="artwork_id", how="left")
        .join(media.lazy(), on="artwork_id", how="left")
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
        .collect()
    )

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    artworks_sold = int(result["has_sold"].sum())
    artworks_unsold = int((~result["has_sold"]).sum())
    artworks_with_media = int((result["media_count"] > 0).sum())
    total_catalog_value = float(result["list_price_usd"].sum())

    context.log.info(
        f"[join_and_enrich_artworks] Enriched {len(result):,} artworks | "
        f"Sold: {artworks_sold:,} | Unsold: {artworks_unsold:,} | "
        f"With media: {artworks_with_media:,} | "
        f"Total catalog value: ${total_catalog_value:,.0f} | {elapsed_ms:.1f}ms"
    )
    return result


# -----------------------------------------------------------------------------
# Artworks Graph-Backed Asset
# -----------------------------------------------------------------------------


@dg.graph_asset(
    kinds={"polars"},
    group_name="transform_polars_ops",
    ins={
        "_sales": dg.AssetIn(key="dlt_harvest_sales_raw", dagster_type=Nothing),
        "_artworks": dg.AssetIn(key="dlt_harvest_artworks_raw", dagster_type=Nothing),
        "_artists": dg.AssetIn(key="dlt_harvest_artists_raw", dagster_type=Nothing),
        "_media": dg.AssetIn(key="dlt_harvest_media", dagster_type=Nothing),
    },
)
def artworks_transform_polars_ops(
    _sales: None, _artworks: None, _artists: None, _media: None
) -> pl.DataFrame:
    """Transform artworks data using ops for detailed observability.

    Graph-backed asset with Nothing-typed inputs:
    - Declares dependencies on harvest assets (shows in lineage graph)
    - Ops read data directly from Parquet files
    - Op-level logs and potential retries
    """
    # Wire Nothing inputs to first op to establish dependency chain
    catalog = build_artwork_catalog(_dep1=_sales, _dep2=_artworks, _dep3=_artists, _dep4=_media)
    sales_agg = aggregate_sales_metrics(catalog)
    media = prepare_media_data(_dep=_media)
    transformed = join_and_enrich_artworks(catalog, sales_agg, media)
    return cast(pl.DataFrame, transformed)


# -----------------------------------------------------------------------------
# Output Assets
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"polars", "json"},
    # Depends on artworks output to ensure deterministic ordering in JSON files.
    deps=["artworks_output_polars_ops"],
    group_name="output_polars_ops",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def sales_output_polars_ops(
    context: dg.AssetExecutionContext,
    sales_transform_polars_ops: pl.DataFrame,
    output_paths: OutputPathsResource,
) -> pl.DataFrame:
    """Filter high-value sales and output to JSON."""
    start_time = time.perf_counter()
    total_count = len(sales_transform_polars_ops)
    sales_path = output_paths.sales_polars_ops

    # Direct filter - no need for .lazy().collect() on DataFrame
    result = sales_transform_polars_ops.filter(pl.col("sale_price_usd") >= MIN_SALE_VALUE_USD)

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(
        result,
        sales_path,
        context,
        extra_metadata={
            "filtered_from": total_count,
            "filter_threshold": f"${MIN_SALE_VALUE_USD:,}",
            "total_value": float(result["sale_price_usd"].sum()),
            "processing_time_ms": round(elapsed_ms, 2),
        },
    )
    context.log.info(
        f"Output {len(result):,} high-value sales to {sales_path} in {elapsed_ms:.1f}ms"
    )
    return result


@dg.asset(
    kinds={"polars", "json"},
    group_name="output_polars_ops",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def artworks_output_polars_ops(
    context: dg.AssetExecutionContext,
    artworks_transform_polars_ops: pl.DataFrame,
    output_paths: OutputPathsResource,
) -> pl.DataFrame:
    """Output artwork catalog to JSON."""
    start_time = time.perf_counter()
    artworks_path = output_paths.artworks_polars_ops

    vc = artworks_transform_polars_ops["price_tier"].value_counts()
    tier_counts = dict(zip(vc["price_tier"].to_list(), vc["count"].to_list()))

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(
        artworks_transform_polars_ops,
        artworks_path,
        context,
        extra_metadata={
            "price_tier_distribution": tier_counts,
            "processing_time_ms": round(elapsed_ms, 2),
        },
    )
    context.log.info(
        f"Output {len(artworks_transform_polars_ops):,} artworks to {artworks_path} in {elapsed_ms:.1f}ms"
    )
    return artworks_transform_polars_ops
