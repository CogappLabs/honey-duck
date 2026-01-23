"""Polars implementation variant - demonstrates running same logic in separate group.

This module is a reference implementation showing how to duplicate pipeline logic
in a different asset group. Originally designed to demonstrate FilesystemIOManager
(pickle-based) vs DuckDB IO manager, both implementations now use PolarsParquetIOManager.

Use cases for this pattern:
- Running identical logic with different configurations (e.g., staging vs production)
- Demonstrating that asset groups are isolated (can run in parallel without conflicts)
- Testing alternative IO manager configurations without modifying main implementation

Note: In production, you'd typically use Dagster's resources/configuration system
to parameterize a single implementation rather than duplicating code.

Asset Graph:
    dlt_harvest_* (shared) --> sales_transform_polars_fs --> sales_output_polars_fs
                           └--> artworks_transform_polars_fs --> artworks_output_polars_fs
"""

import time
from datetime import timedelta

import dagster as dg
import polars as pl

from cogapp_libs.dagster import read_harvest_table_lazy, write_json_output

from ..shared.constants import (
    MIN_SALE_VALUE_USD,
    PRICE_TIER_BUDGET_MAX_USD,
    PRICE_TIER_MID_MAX_USD,
)
from ..shared.resources import OutputPathsResource, PathsResource

# Use centralized harvest dependencies for consistency
from ..shared.helpers import STANDARD_HARVEST_DEPS as HARVEST_DEPS


# -----------------------------------------------------------------------------
# Sales Pipeline: sales_transform_polars_fs -> sales_output_polars_fs
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"polars"},
    deps=HARVEST_DEPS,
    group_name="transform_polars_fs",
)
def sales_transform_polars_fs(
    context: dg.AssetExecutionContext, paths: PathsResource
) -> pl.DataFrame:
    """Join sales with artworks and artists using pure Polars lazy expressions."""
    start_time = time.perf_counter()
    harvest_dir = paths.harvest_dir

    # Native Polars scan_parquet - no DuckDB overhead
    sales = read_harvest_table_lazy(harvest_dir, "sales_raw")
    artworks = read_harvest_table_lazy(harvest_dir, "artworks_raw")
    artists = read_harvest_table_lazy(harvest_dir, "artists_raw")

    # Build lazy query: join -> select -> compute -> normalize -> sort
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
    context.add_output_metadata(
        {
            "record_count": len(result),
            "columns": result.columns,
            "preview": dg.MetadataValue.md(result.head(5).to_pandas().to_markdown(index=False)),
            "unique_artworks": result["artwork_id"].n_unique(),
            "total_sales_value": float(result["sale_price_usd"].sum()),
            "date_range": f"{str(result['sale_date'].min())} to {str(result['sale_date'].max())}",
            "processing_time_ms": round(elapsed_ms, 2),
            "io_manager": "PolarsParquetIOManager",
        }
    )
    context.log.info(f"Transformed {len(result)} sales records (Polars FS) in {elapsed_ms:.1f}ms")
    return result


@dg.asset(
    kinds={"polars", "json"},
    # Depends on artworks output to ensure deterministic ordering in JSON files.
    # See assets_polars.py:sales_output_polars for detailed explanation.
    deps=["artworks_output_polars_fs"],
    group_name="output_polars_fs",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def sales_output_polars_fs(
    context: dg.AssetExecutionContext,
    sales_transform_polars_fs: pl.DataFrame,
    output_paths: OutputPathsResource,
) -> pl.DataFrame:
    """Filter high-value sales and output to JSON."""
    start_time = time.perf_counter()
    total_count = len(sales_transform_polars_fs)
    sales_path = output_paths.sales_polars_fs

    # Direct filter - no need for .lazy().collect() on DataFrame
    result = sales_transform_polars_fs.filter(pl.col("sale_price_usd") >= MIN_SALE_VALUE_USD)

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
            "io_manager": "PolarsParquetIOManager",
        },
    )
    context.log.info(f"Output {len(result)} high-value sales to {sales_path} in {elapsed_ms:.1f}ms")
    return result


# -----------------------------------------------------------------------------
# Artworks Pipeline: artworks_transform_polars_fs -> artworks_output_polars_fs
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"polars"},
    deps=HARVEST_DEPS,
    group_name="transform_polars_fs",
)
def artworks_transform_polars_fs(
    context: dg.AssetExecutionContext, paths: PathsResource
) -> pl.DataFrame:
    """Join artworks with artists, media, and aggregate sales using Polars lazy."""
    start_time = time.perf_counter()
    harvest_dir = paths.harvest_dir

    # Native Polars scan_parquet - no DuckDB overhead
    sales = read_harvest_table_lazy(harvest_dir, "sales_raw")
    artworks = read_harvest_table_lazy(harvest_dir, "artworks_raw")
    artists = read_harvest_table_lazy(harvest_dir, "artists_raw")
    media = read_harvest_table_lazy(harvest_dir, "media")

    # Build catalog: join artworks with artists
    catalog = artworks.join(artists, on="artist_id", how="left").select(
        "artwork_id",
        "title",
        "year",
        "medium",
        pl.col("price_usd").alias("list_price_usd"),
        pl.col("name").alias("artist_name"),
        "nationality",
    )

    # Use semi-join to filter sales to only artworks in catalog (avoids early materialization)
    sales_per_artwork = (
        sales.join(catalog.select("artwork_id"), on="artwork_id", how="semi")
        .group_by("artwork_id")
        .agg(
            pl.len().alias("sale_count"),
            pl.col("sale_price_usd").sum().alias("total_sales_value"),
            pl.col("sale_price_usd").mean().round(0).alias("avg_sale_price"),
            pl.col("sale_date").min().alias("first_sale_date"),
            pl.col("sale_date").max().alias("last_sale_date"),
        )
    )

    # Primary media: sort_order = 1
    primary_media = media.filter(pl.col("sort_order") == 1).select(
        "artwork_id",
        pl.col("filename").alias("primary_image"),
        pl.col("alt_text").alias("primary_image_alt"),
    )

    # All media aggregated as struct list
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

    # Final assembly
    result = (
        catalog.join(sales_per_artwork, on="artwork_id", how="left")
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
            pl.col("total_sales_value").rank(method="ordinal", descending=True).alias("sales_rank"),
            pl.col("artist_name").str.to_uppercase(),
        )
        .sort(["total_sales_value", "artwork_id"], descending=[True, False])
        .collect()
    )

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
            "io_manager": "PolarsParquetIOManager",
        }
    )
    context.log.info(f"Transformed {len(result)} artworks (Polars FS) in {elapsed_ms:.1f}ms")
    return result


@dg.asset(
    kinds={"polars", "json"},
    group_name="output_polars_fs",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def artworks_output_polars_fs(
    context: dg.AssetExecutionContext,
    artworks_transform_polars_fs: pl.DataFrame,
    output_paths: OutputPathsResource,
) -> pl.DataFrame:
    """Output artwork catalog to JSON using pure Polars."""
    start_time = time.perf_counter()
    artworks_path = output_paths.artworks_polars_fs

    vc = artworks_transform_polars_fs["price_tier"].value_counts()
    tier_counts = dict(zip(vc["price_tier"].to_list(), vc["count"].to_list()))

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(
        artworks_transform_polars_fs,
        artworks_path,
        context,
        extra_metadata={
            "price_tier_distribution": tier_counts,
            "processing_time_ms": round(elapsed_ms, 2),
            "io_manager": "PolarsParquetIOManager",
        },
    )
    context.log.info(
        f"Output {len(artworks_transform_polars_fs)} artworks to {artworks_path} in {elapsed_ms:.1f}ms"
    )
    return artworks_transform_polars_fs
