"""Graph-backed asset implementation using ops for detailed observability.

This module demonstrates the graph-backed asset pattern for proof-of-concept:
- Each transformation step is an op with detailed logging and metadata
- Ops are composed into @graph_asset decorated functions
- Results in single asset in lineage graph with op-level observability
- No intermediate persistence (data flows through memory)

Use this pattern when:
- You want detailed op-level logs and metadata for debugging
- Intermediate persistence is not required
- You're building complex multi-step transformations

For intermediate persistence, use the split asset pattern in assets_polars.py instead.

Asset Graph:
    dlt_harvest_* ──→ sales_transform_polars_ops ──→ sales_output_polars_ops
                  └──→ artworks_transform_polars_ops ──→ artworks_output_polars_ops
"""

import time
from datetime import timedelta

import dagster as dg
import polars as pl

from cogapp_deps.dagster import (
    read_harvest_tables_lazy,
    read_parquet_table_lazy,
    track_timing,
    write_json_output,
)

from .config import CONFIG
from .constants import (
    MIN_SALE_VALUE_USD,
    PRICE_TIER_BUDGET_MAX_USD,
    PRICE_TIER_MID_MAX_USD,
)
from .helpers import STANDARD_HARVEST_DEPS as HARVEST_DEPS
from .resources import (
    ARTWORKS_OUTPUT_PATH_POLARS_OPS,
    HARVEST_PARQUET_DIR,
    SALES_OUTPUT_PATH_POLARS_OPS,
)


# -----------------------------------------------------------------------------
# Sales Pipeline Ops
# -----------------------------------------------------------------------------


@dg.op
def join_sales_data(context: dg.OpExecutionContext) -> pl.DataFrame:
    """Op: Join sales with artworks and artists.

    Demonstrates batch reading in ops context for large datasets.
    """
    with track_timing(context, "join"):
        # Batch read with validation
        tables = read_harvest_tables_lazy(
            HARVEST_PARQUET_DIR,
            ("sales_raw", ["sale_id", "artwork_id", "sale_date", "sale_price_usd", "buyer_country"]),
            ("artworks_raw", ["artwork_id", "artist_id", "title", "year", "medium", "price_usd"]),
            ("artists_raw", ["artist_id", "name", "nationality"]),
            asset_name="join_sales_data",
        )

        # Join and select columns
        result = (
            tables["sales_raw"]
            .join(tables["artworks_raw"], on="artwork_id", how="left", suffix="_aw")
            .join(tables["artists_raw"], on="artist_id", how="left", suffix="_ar")
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

    # Add price metrics, normalize artist names, and sort (chained for readability)
    result = (
        joined_data
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

    # Calculate metadata for logging
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
# Sales Graph-Backed Asset
# -----------------------------------------------------------------------------


@dg.graph_asset(
    kinds={"polars"},
    group_name="transform_polars_ops",
)
def sales_transform_polars_ops() -> pl.DataFrame:
    """Transform sales data using ops for detailed observability.

    Graph-backed asset provides:
    - Op-level logging and metadata for each transformation step
    - Single asset in lineage graph
    - No intermediate persistence (data flows through memory)
    """
    # Execute ops in sequence
    joined = join_sales_data()
    transformed = add_sales_metrics(joined)
    return transformed


# -----------------------------------------------------------------------------
# Artworks Pipeline Ops
# -----------------------------------------------------------------------------


@dg.op
def build_artwork_catalog(context: dg.OpExecutionContext) -> pl.DataFrame:
    """Op: Build artwork catalog by joining artworks with artists."""
    start_time = time.perf_counter()

    artworks = read_parquet_table_lazy(
        HARVEST_PARQUET_DIR / "raw",
        "artworks_raw",
        required_columns=["artwork_id", "artist_id", "title", "year", "medium", "price_usd"],
        asset_name="build_artwork_catalog",
    )
    artists = read_parquet_table_lazy(
        HARVEST_PARQUET_DIR / "raw",
        "artists_raw",
        required_columns=["artist_id", "name", "nationality"],
        asset_name="build_artwork_catalog",
    )

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
    context.log.info(
        f"[build_artwork_catalog] Built catalog for {len(result):,} artworks "
        f"across {result['artist_name'].n_unique():,} artists in {elapsed_ms:.1f}ms"
    )
    return result


@dg.op
def aggregate_sales_metrics(context: dg.OpExecutionContext, catalog: pl.DataFrame) -> pl.DataFrame:
    """Op: Aggregate sales metrics per artwork."""
    start_time = time.perf_counter()

    sales = read_parquet_table_lazy(
        HARVEST_PARQUET_DIR / "raw",
        "sales_raw",
        required_columns=["artwork_id", "sale_price_usd", "sale_date"],
        asset_name="aggregate_sales_metrics",
    )
    artwork_ids = catalog["artwork_id"]

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
    total_sales_value = float(result["total_sales_value"].sum())
    context.log.info(
        f"[aggregate_sales_metrics] Aggregated sales for {len(result):,} artworks | "
        f"Total value: ${total_sales_value:,.0f} | {elapsed_ms:.1f}ms"
    )
    return result


@dg.op
def prepare_media_data(context: dg.OpExecutionContext) -> pl.DataFrame:
    """Op: Prepare media data - primary image and all media aggregated."""
    start_time = time.perf_counter()

    media = read_parquet_table_lazy(
        HARVEST_PARQUET_DIR / "raw",
        "media",
        required_columns=["artwork_id", "sort_order", "filename", "alt_text", "media_type",
                         "file_format", "width_px", "height_px", "file_size_kb"],
        asset_name="prepare_media_data",
    )

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

    # Join all intermediate results and add computed fields (chained for readability)
    result = (
        catalog
        .lazy()
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
            pl.col("total_sales_value")
            .rank(method="ordinal", descending=True)
            .alias("sales_rank"),
            pl.col("artist_name").str.to_uppercase(),
        )
        .sort(["total_sales_value", "artwork_id"], descending=[True, False])
        .collect()  # Single materialization point
    )

    elapsed_ms = (time.perf_counter() - start_time) * 1000

    # Calculate metadata
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
)
def artworks_transform_polars_ops() -> pl.DataFrame:
    """Transform artworks data using ops for detailed observability.

    Graph-backed asset provides:
    - Op-level logging and metadata for each transformation step
    - Single asset in lineage graph
    - No intermediate persistence (data flows through memory)
    """
    # Execute ops in sequence
    catalog = build_artwork_catalog()
    sales_agg = aggregate_sales_metrics(catalog)
    media = prepare_media_data()
    transformed = join_and_enrich_artworks(catalog, sales_agg, media)
    return transformed


# -----------------------------------------------------------------------------
# Output Assets
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"polars", "json"},
    deps=["artworks_output_polars_ops"],
    group_name="output_polars_ops",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def sales_output_polars_ops(
    context: dg.AssetExecutionContext,
    sales_transform_polars_ops: pl.DataFrame,
) -> pl.DataFrame:
    """Filter high-value sales and output to JSON."""
    start_time = time.perf_counter()
    total_count = len(sales_transform_polars_ops)

    # Use lazy for filtering
    result = (
        sales_transform_polars_ops
        .lazy()
        .filter(pl.col("sale_price_usd") >= MIN_SALE_VALUE_USD)
        .collect()
    )

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(result, SALES_OUTPUT_PATH_POLARS_OPS, context, extra_metadata={
        "filtered_from": total_count,
        "filter_threshold": f"${MIN_SALE_VALUE_USD:,}",
        "total_value": float(result["sale_price_usd"].sum()),
        "processing_time_ms": round(elapsed_ms, 2),
    })
    context.log.info(
        f"Output {len(result):,} high-value sales to {SALES_OUTPUT_PATH_POLARS_OPS} in {elapsed_ms:.1f}ms"
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
) -> pl.DataFrame:
    """Output artwork catalog to JSON."""
    start_time = time.perf_counter()

    vc = artworks_transform_polars_ops["price_tier"].value_counts()
    tier_counts = dict(zip(vc["price_tier"].to_list(), vc["count"].to_list()))

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(
        artworks_transform_polars_ops, ARTWORKS_OUTPUT_PATH_POLARS_OPS, context,
        extra_metadata={
            "price_tier_distribution": tier_counts,
            "processing_time_ms": round(elapsed_ms, 2),
        }
    )
    context.log.info(
        f"Output {len(artworks_transform_polars_ops):,} artworks to {ARTWORKS_OUTPUT_PATH_POLARS_OPS} in {elapsed_ms:.1f}ms"
    )
    return artworks_transform_polars_ops
