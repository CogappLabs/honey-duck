"""Pure Pandas implementation of honey-duck pipeline transforms.

This module implements the transform and output layers using only inline
Pandas expressions - no processor classes from cogapp_deps.

Note: Assets convert to Polars DataFrames before returning for compatibility
with the PolarsParquetIOManager. All transformations use Pandas.

Asset Graph:
    dlt_harvest_* (shared) ──→ sales_transform_pandas ──→ sales_output_pandas
                           └──→ artworks_transform_pandas ──→ artworks_output_pandas
"""

import time
from datetime import timedelta

import dagster as dg
import duckdb
import numpy as np
import pandas as pd
import polars as pl

from cogapp_deps.dagster import write_json_output

from .constants import (
    MIN_SALE_VALUE_USD,
    PRICE_TIER_BUDGET_MAX_USD,
    PRICE_TIER_MID_MAX_USD,
)
from .resources import (
    ARTWORKS_OUTPUT_PATH_PANDAS,
    DUCKDB_PATH,
    SALES_OUTPUT_PATH_PANDAS,
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


def _read_raw_table(table: str) -> pd.DataFrame:
    """Read table from raw schema as Pandas DataFrame."""
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        return conn.sql(f"SELECT * FROM raw.{table}").df()
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Sales Pipeline: sales_transform_pandas → sales_output_pandas
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"pandas"},
    deps=HARVEST_DEPS,
    group_name="transform_pandas",
)
def sales_transform_pandas(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Join sales with artworks and artists using pure Pandas expressions."""
    start_time = time.perf_counter()

    sales = _read_raw_table("sales_raw")
    artworks = _read_raw_table("artworks_raw")
    artists = _read_raw_table("artists_raw")

    # Join: sales -> artworks -> artists
    result = (
        sales
        .merge(artworks, on="artwork_id", how="left", suffixes=("", "_aw"))
        .merge(artists, on="artist_id", how="left", suffixes=("", "_ar"))
    )

    # Select and rename columns
    result = result[[
        "sale_id", "artwork_id", "sale_date", "sale_price_usd", "buyer_country",
        "title", "artist_id", "year", "medium", "price_usd", "name", "nationality"
    ]].rename(columns={
        "year": "artwork_year",
        "price_usd": "list_price_usd",
        "name": "artist_name",
    })

    # Add computed columns using vectorized operations
    result = result.assign(
        price_diff=result["sale_price_usd"] - result["list_price_usd"],
        pct_change=np.where(
            result["list_price_usd"].isna() | (result["list_price_usd"] == 0),
            np.nan,
            ((result["sale_price_usd"] - result["list_price_usd"]) * 100.0
             / result["list_price_usd"]).round(1)
        ),
        artist_name=result["artist_name"].str.strip().str.upper(),
    )

    # Sort by sale_date descending, sale_id for deterministic ordering
    result = result.sort_values(["sale_date", "sale_id"], ascending=[False, True]).reset_index(drop=True)

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    context.add_output_metadata({
        "record_count": len(result),
        "columns": list(result.columns),
        "preview": dg.MetadataValue.md(result.head(5).to_markdown(index=False)),
        "unique_artworks": result["artwork_id"].nunique(),
        "total_sales_value": float(result["sale_price_usd"].sum()),
        "date_range": f"{result['sale_date'].min()} to {result['sale_date'].max()}",
        "processing_time_ms": round(elapsed_ms, 2),
    })
    context.log.info(f"Transformed {len(result)} sales records (Pandas) in {elapsed_ms:.1f}ms")

    # Convert to Polars for PolarsParquetIOManager
    return pl.from_pandas(result)


@dg.asset(
    kinds={"pandas", "json"},
    deps=["artworks_output_pandas"],
    group_name="output_pandas",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def sales_output_pandas(
    context: dg.AssetExecutionContext,
    sales_transform_pandas: pl.DataFrame,
) -> pl.DataFrame:
    """Filter high-value sales using pure Pandas and output to JSON."""
    start_time = time.perf_counter()

    # Convert from Polars to Pandas for processing
    sales_df = sales_transform_pandas.to_pandas()
    total_count = len(sales_df)

    # Filter using query() for readability
    result = sales_df.query(f"sale_price_usd >= {MIN_SALE_VALUE_USD}").copy()

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(result, SALES_OUTPUT_PATH_PANDAS, context, extra_metadata={
        "filtered_from": total_count,
        "filter_threshold": f"${MIN_SALE_VALUE_USD:,}",
        "total_value": float(result["sale_price_usd"].sum()),
        "processing_time_ms": round(elapsed_ms, 2),
    })
    context.log.info(f"Output {len(result)} high-value sales to {SALES_OUTPUT_PATH_PANDAS} in {elapsed_ms:.1f}ms")

    # Convert to Polars for PolarsParquetIOManager
    return pl.from_pandas(result)


# -----------------------------------------------------------------------------
# Artworks Pipeline: artworks_transform_pandas → artworks_output_pandas
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"pandas"},
    deps=HARVEST_DEPS,
    group_name="transform_pandas",
)
def artworks_transform_pandas(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Join artworks with artists, media, and aggregate sales using pure Pandas."""
    start_time = time.perf_counter()

    sales = _read_raw_table("sales_raw")
    artworks = _read_raw_table("artworks_raw")
    artists = _read_raw_table("artists_raw")
    media = _read_raw_table("media")

    # Aggregate sales per artwork using vectorized groupby
    valid_artwork_ids = set(artworks["artwork_id"])
    valid_sales = sales[sales["artwork_id"].isin(valid_artwork_ids)]
    sales_per_artwork = (
        valid_sales
        .groupby("artwork_id", as_index=False)
        .agg(
            sale_count=("sale_id", "count"),
            total_sales_value=("sale_price_usd", "sum"),
            avg_sale_price=("sale_price_usd", "mean"),
            first_sale_date=("sale_date", "min"),
            last_sale_date=("sale_date", "max"),
        )
        .assign(avg_sale_price=lambda df: df["avg_sale_price"].round(0))
    )

    # Build catalog: join artworks with artists
    catalog = (
        artworks
        .merge(artists, on="artist_id", how="left")
        [["artwork_id", "title", "year", "medium", "price_usd", "name", "nationality"]]
        .rename(columns={"price_usd": "list_price_usd", "name": "artist_name"})
    )

    # Primary media (sort_order = 1)
    primary_media = (
        media
        .query("sort_order == 1")
        [["artwork_id", "filename", "alt_text"]]
        .rename(columns={"filename": "primary_image", "alt_text": "primary_image_alt"})
    )

    # All media aggregated as list of dicts
    # Group and convert to records - this is unavoidably row-wise but optimized
    media_cols = [
        "sort_order", "filename", "media_type", "file_format",
        "width_px", "height_px", "file_size_kb", "alt_text"
    ]
    media_sorted = media.sort_values("sort_order")
    all_media = (
        media_sorted
        .groupby("artwork_id")[media_cols]
        .apply(lambda g: g.to_dict("records"), include_groups=False)  # type: ignore[call-overload]
        .reset_index(name="media")
    )

    # Final assembly: join all intermediate results
    result = (
        catalog
        .merge(sales_per_artwork, on="artwork_id", how="left")
        .merge(primary_media, on="artwork_id", how="left")
        .merge(all_media, on="artwork_id", how="left")
    )

    # Fill nulls and add derived columns using assign for cleaner chaining
    result = result.assign(
        sale_count=result["sale_count"].fillna(0).astype(int),
        total_sales_value=result["total_sales_value"].fillna(0),
        has_sold=result["sale_count"].fillna(0) > 0,
        price_tier=pd.cut(
            result["list_price_usd"],
            bins=[0, PRICE_TIER_BUDGET_MAX_USD, PRICE_TIER_MID_MAX_USD, float("inf")],
            labels=["budget", "mid", "premium"],
            right=False,
        ).astype(str),
        media_count=result["media"].apply(lambda x: len(x) if isinstance(x, list) else 0),
        artist_name=result["artist_name"].str.upper(),
    )

    # Add sales rank
    result["sales_rank"] = (
        result["total_sales_value"]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    # Sort by total_sales_value descending, artwork_id for deterministic ordering
    result = result.sort_values(["total_sales_value", "artwork_id"], ascending=[False, True]).reset_index(drop=True)

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    context.add_output_metadata({
        "record_count": len(result),
        "artworks_sold": int(result["has_sold"].sum()),
        "artworks_unsold": int((~result["has_sold"]).sum()),
        "artworks_with_media": int((result["media_count"] > 0).sum()),
        "total_catalog_value": float(result["list_price_usd"].sum()),
        "preview": dg.MetadataValue.md(result.head(5).to_markdown(index=False)),
        "processing_time_ms": round(elapsed_ms, 2),
    })
    context.log.info(f"Transformed {len(result)} artworks (Pandas) in {elapsed_ms:.1f}ms")

    # Convert to Polars for PolarsParquetIOManager
    return pl.from_pandas(result)


@dg.asset(
    kinds={"pandas", "json"},
    group_name="output_pandas",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def artworks_output_pandas(
    context: dg.AssetExecutionContext,
    artworks_transform_pandas: pl.DataFrame,
) -> pl.DataFrame:
    """Output artwork catalog to JSON using pure Pandas."""
    start_time = time.perf_counter()

    # Convert from Polars to Pandas for processing
    artworks_df = artworks_transform_pandas.to_pandas()

    tier_counts = artworks_df["price_tier"].value_counts().to_dict()

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(
        artworks_df, ARTWORKS_OUTPUT_PATH_PANDAS, context,
        extra_metadata={
            "price_tier_distribution": tier_counts,
            "processing_time_ms": round(elapsed_ms, 2),
        }
    )
    context.log.info(f"Output {len(artworks_df)} artworks to {ARTWORKS_OUTPUT_PATH_PANDAS} in {elapsed_ms:.1f}ms")

    # Convert to Polars for PolarsParquetIOManager
    return artworks_transform_pandas
