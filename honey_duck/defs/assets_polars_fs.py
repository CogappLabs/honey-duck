"""Pure Polars implementation with filesystem IO manager.

This module is identical to assets_polars.py but uses Dagster's default
FilesystemIOManager instead of DuckDBPandasPolarsIOManager. This demonstrates
how the same processing logic can use different storage backends.

Storage comparison:
- DuckDB IO manager: Stores DataFrames as DuckDB tables (columnar, queryable)
- Filesystem IO manager: Pickles DataFrames to files (simple, portable)

Asset Graph:
    dlt_harvest_* (shared) --> sales_transform_polars_fs --> sales_output_polars_fs
                           └--> artworks_transform_polars_fs --> artworks_output_polars_fs
"""

from datetime import timedelta

import dagster as dg
import polars as pl

from cogapp_deps.dagster import (
    add_dataframe_metadata,
    read_harvest_table_lazy,
    read_harvest_tables_lazy,
    track_timing,
    write_json_output,
)

from .constants import (
    MIN_SALE_VALUE_USD,
    PRICE_TIER_BUDGET_MAX_USD,
    PRICE_TIER_MID_MAX_USD,
)
from .helpers import STANDARD_HARVEST_DEPS as HARVEST_DEPS
from .resources import (
    ARTWORKS_OUTPUT_PATH_POLARS_FS,
    HARVEST_PARQUET_DIR,
    SALES_OUTPUT_PATH_POLARS_FS,
)


def _read_raw_table_lazy(table: str) -> pl.LazyFrame:
    """Read table from Parquet files as Polars LazyFrame."""
    return read_harvest_table_lazy(
        HARVEST_PARQUET_DIR,
        table,
        asset_name="polars_fs_pipeline",
    )


# -----------------------------------------------------------------------------
# Sales Pipeline: sales_transform_polars_fs -> sales_output_polars_fs
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"polars"},
    deps=HARVEST_DEPS,
    group_name="transform_polars_fs",
    io_manager_key="fs_io_manager",
)
def sales_transform_polars_fs(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Join sales with artworks and artists using pure Polars lazy expressions.

    Demonstrates helpers with FilesystemIOManager backend.
    """
    with track_timing(context, "sales transform (FS)"):
        # Batch read tables
        tables = read_harvest_tables_lazy(
            HARVEST_PARQUET_DIR,
            ("sales_raw", None),
            ("artworks_raw", None),
            ("artists_raw", None),
            asset_name="sales_transform_polars_fs",
        )

        # Build lazy query: join -> select -> compute -> normalize -> sort
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

    # Add metadata with extra fields
    add_dataframe_metadata(
        context,
        result,
        unique_artworks=result["artwork_id"].n_unique(),
        total_sales_value=float(result["sale_price_usd"].sum()),
        date_range=f"{str(result['sale_date'].min())} to {str(result['sale_date'].max())}",
        io_manager="FilesystemIOManager",
    )
    return result


@dg.asset(
    kinds={"polars", "json"},
    deps=["artworks_output_polars_fs"],
    group_name="output_polars_fs",
    io_manager_key="fs_io_manager",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def sales_output_polars_fs(
    context: dg.AssetExecutionContext,
    sales_transform_polars_fs: pl.DataFrame,
) -> pl.DataFrame:
    """Filter high-value sales using Polars lazy and output to JSON."""
    start_time = time.perf_counter()
    total_count = len(sales_transform_polars_fs)

    result = (
        sales_transform_polars_fs
        .lazy()
        .filter(pl.col("sale_price_usd") >= MIN_SALE_VALUE_USD)
        .collect()
    )

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(result, SALES_OUTPUT_PATH_POLARS_FS, context, extra_metadata={
        "filtered_from": total_count,
        "filter_threshold": f"${MIN_SALE_VALUE_USD:,}",
        "total_value": float(result["sale_price_usd"].sum()),
        "processing_time_ms": round(elapsed_ms, 2),
        "io_manager": "FilesystemIOManager",
    })
    context.log.info(f"Output {len(result)} high-value sales to {SALES_OUTPUT_PATH_POLARS_FS} in {elapsed_ms:.1f}ms")
    return result


# -----------------------------------------------------------------------------
# Artworks Pipeline: artworks_transform_polars_fs -> artworks_output_polars_fs
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"polars"},
    deps=HARVEST_DEPS,
    group_name="transform_polars_fs",
    io_manager_key="fs_io_manager",
)
def artworks_transform_polars_fs(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Join artworks with artists, media, and aggregate sales using Polars lazy."""
    start_time = time.perf_counter()

    sales = _read_raw_table_lazy("sales_raw")
    artworks = _read_raw_table_lazy("artworks_raw")
    artists = _read_raw_table_lazy("artists_raw")
    media = _read_raw_table_lazy("media")

    # Collect artwork_ids first for filtering
    artwork_ids = artworks.select("artwork_id").collect()["artwork_id"]

    # Aggregate sales per artwork
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

    # Build catalog: join artworks with artists
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

    # Primary media: sort_order = 1
    primary_media = (
        media
        .filter(pl.col("sort_order") == 1)
        .select(
            "artwork_id",
            pl.col("filename").alias("primary_image"),
            pl.col("alt_text").alias("primary_image_alt"),
        )
    )

    # All media aggregated as struct list
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

    # Final assembly
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
        .collect()
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
        "io_manager": "FilesystemIOManager",
    })
    context.log.info(f"Transformed {len(result)} artworks (Polars FS) in {elapsed_ms:.1f}ms")
    return result


@dg.asset(
    kinds={"polars", "json"},
    group_name="output_polars_fs",
    io_manager_key="fs_io_manager",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def artworks_output_polars_fs(
    context: dg.AssetExecutionContext,
    artworks_transform_polars_fs: pl.DataFrame,
) -> pl.DataFrame:
    """Output artwork catalog to JSON using pure Polars."""
    start_time = time.perf_counter()

    vc = artworks_transform_polars_fs["price_tier"].value_counts()
    tier_counts = dict(zip(vc["price_tier"].to_list(), vc["count"].to_list()))

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(
        artworks_transform_polars_fs, ARTWORKS_OUTPUT_PATH_POLARS_FS, context,
        extra_metadata={
            "price_tier_distribution": tier_counts,
            "processing_time_ms": round(elapsed_ms, 2),
            "io_manager": "FilesystemIOManager",
        }
    )
    context.log.info(f"Output {len(artworks_transform_polars_fs)} artworks to {ARTWORKS_OUTPUT_PATH_POLARS_FS} in {elapsed_ms:.1f}ms")
    return artworks_transform_polars_fs
