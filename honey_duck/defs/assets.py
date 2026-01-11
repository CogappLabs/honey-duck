"""Transform and output assets for the honey-duck pipeline.

IO Approaches
-------------
This pipeline uses two IO approaches across three layers:

1. dlt-managed IO (harvest layer):
   - dlt writes directly to Parquet files
   - DuckDB views (raw.*) point to Parquet files for SQL access
   - Dagster IO manager is NOT used
   - Assets yield MaterializeResult with metadata only

2. Dagster IO manager (transform + output layers):
   - TRANSFORM: Reads raw.* via SQL views (deps declare dependency), returns DataFrame
   - OUTPUT: Receives DataFrame as parameter, returns DataFrame + writes JSON

Data Flow:
    ┌─────────────────────────────────────────────────────────────────┐
    │ HARVEST (dlt writes to Parquet, DuckDB views for SQL access)    │
    │   dlt_harvest_assets -> Parquet files -> DuckDB raw.* views    │
    └──────────────────────────────┬──────────────────────────────────┘
                                   │ deps (no data passed)
                                   ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │ TRANSFORM (read raw.* via SQL views, IO manager stores output)  │
    │   sales_transform    : SQL joins raw.* -> return DataFrame      │
    │   artworks_transform : SQL joins raw.* -> return DataFrame      │
    └──────────────────────────────┬──────────────────────────────────┘
                                   │ DataFrame via IO manager
                                   ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │ OUTPUT (IO manager loads input as param, writes JSON + stores)  │
    │   sales_output    : receives DataFrame -> return + JSON         │
    │   artworks_output : receives DataFrame -> return + JSON         │
    └─────────────────────────────────────────────────────────────────┘

Asset Graph:
    dlt_harvest_sales_raw ────────┐
                                  ├──→ sales_transform ────→ sales_output
    dlt_harvest_artworks_raw ─────┤
                                  │
    dlt_harvest_artists_raw ──────┼──→ artworks_transform ─→ artworks_output
                                  │
    dlt_harvest_media ────────────┘
"""

import time
from datetime import timedelta

import dagster as dg
import polars as pl

from cogapp_deps.dagster import read_table, write_json_output
from cogapp_deps.processors import Chain
from cogapp_deps.processors.duckdb import (
    DuckDBQueryProcessor,
    DuckDBSQLProcessor,
    configure as configure_duckdb,
)
from cogapp_deps.processors.polars import PolarsFilterProcessor, PolarsStringProcessor

from .constants import (
    MIN_SALE_VALUE_USD,
    PRICE_TIER_BUDGET_MAX_USD,
    PRICE_TIER_MID_MAX_USD,
)
from .resources import (
    ARTWORKS_OUTPUT_PATH,
    DUCKDB_PATH,
    HARVEST_PARQUET_DIR,
    SALES_OUTPUT_PATH,
)

# Configure DuckDB processors to use the main database for reading raw tables
# Use read_only=False to allow view creation when needed
configure_duckdb(db_path=DUCKDB_PATH, read_only=False)

# Track whether views have been set up in this process
# Note: In multiprocess execution, each process initializes views independently
_views_initialized = False

# View setup retry configuration
_VIEW_SETUP_MAX_RETRIES = 3
_VIEW_SETUP_RETRY_BASE_DELAY = 0.1  # seconds


def _ensure_parquet_views():
    """Ensure views in DuckDB pointing to Parquet files exist (called per-asset as needed).

    Creates DuckDB views that point to Parquet files, enabling SQL queries over the
    harvest data. Uses retry logic to handle concurrent access in multiprocess execution.

    Raises:
        RuntimeError: If views cannot be created after max retries
        ValueError: If parquet directory path is invalid
    """
    global _views_initialized
    if _views_initialized:
        return

    import duckdb
    from pathlib import Path

    # Validate and resolve parquet directory path to prevent injection
    parquet_dir = Path(HARVEST_PARQUET_DIR / "raw").resolve()
    if not str(parquet_dir).startswith(str(Path.cwd().resolve())):
        raise ValueError(f"Invalid parquet directory outside project: {parquet_dir}")

    # Retry logic for handling concurrent access
    for attempt in range(_VIEW_SETUP_MAX_RETRIES):
        try:
            conn = duckdb.connect(str(DUCKDB_PATH))
            try:
                conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

                # Create or replace views pointing to Parquet files
                # Using CREATE OR REPLACE to handle both new and existing views
                conn.execute(f"""
                    CREATE OR REPLACE VIEW raw.sales_raw AS
                    SELECT * FROM read_parquet('{parquet_dir}/sales_raw/*.parquet')
                """)
                conn.execute(f"""
                    CREATE OR REPLACE VIEW raw.artworks_raw AS
                    SELECT * FROM read_parquet('{parquet_dir}/artworks_raw/*.parquet')
                """)
                conn.execute(f"""
                    CREATE OR REPLACE VIEW raw.artists_raw AS
                    SELECT * FROM read_parquet('{parquet_dir}/artists_raw/*.parquet')
                """)
                conn.execute(f"""
                    CREATE OR REPLACE VIEW raw.media AS
                    SELECT * FROM read_parquet('{parquet_dir}/media/*.parquet')
                """)
                _views_initialized = True
                return
            finally:
                conn.close()
        except Exception as e:
            error_str = str(e).lower()
            # If another process already created the views, that's fine
            if "already exists" in error_str or (attempt < _VIEW_SETUP_MAX_RETRIES - 1 and "lock" in error_str):
                if "lock" in error_str:
                    # Wait a bit and retry with exponential backoff
                    time.sleep(_VIEW_SETUP_RETRY_BASE_DELAY * (attempt + 1))
                    continue
                else:
                    # Views exist, we're done
                    _views_initialized = True
                    return
            # On last attempt or unknown error, raise with context
            if attempt == _VIEW_SETUP_MAX_RETRIES - 1:
                raise RuntimeError(
                    f"Failed to initialize Parquet views after {_VIEW_SETUP_MAX_RETRIES} attempts. "
                    f"Last error: {str(e)}"
                ) from e


# -----------------------------------------------------------------------------
# Dependencies
# -----------------------------------------------------------------------------

# All harvest assets - used as dependencies for transform layer
HARVEST_DEPS = [
    dg.AssetKey("dlt_harvest_sales_raw"),
    dg.AssetKey("dlt_harvest_artworks_raw"),
    dg.AssetKey("dlt_harvest_artists_raw"),
    dg.AssetKey("dlt_harvest_media"),
]


# -----------------------------------------------------------------------------
# Sales Pipeline: sales_transform → sales_output
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"duckdb"},
    deps=HARVEST_DEPS,
    group_name="transform",
)
def sales_transform(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Join sales with artworks and artists, add sale-focused metrics."""
    start_time = time.perf_counter()

    # Ensure Parquet views are set up
    _ensure_parquet_views()

    # Extract: join sales → artworks → artists (from database)
    result = DuckDBQueryProcessor(sql="""
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
            ar.name AS artist_name,
            ar.nationality
        FROM raw.sales_raw s
        LEFT JOIN raw.artworks_raw aw ON s.artwork_id = aw.artwork_id
        LEFT JOIN raw.artists_raw ar ON aw.artist_id = ar.artist_id
    """).process()

    # Transform: add price metrics (with division safety)
    result = DuckDBSQLProcessor(sql="""
        SELECT *,
            sale_price_usd - list_price_usd AS price_diff,
            CASE
                WHEN list_price_usd IS NULL OR list_price_usd = 0 THEN NULL
                ELSE ROUND((sale_price_usd - list_price_usd) * 100.0 / list_price_usd, 1)
            END AS pct_change
        FROM _input
        ORDER BY sale_date DESC, sale_id
    """).process(result)

    # Transform: normalize artist names (Chain converts to Polars)
    result: pl.DataFrame = Chain([  # type: ignore[no-redef]
        PolarsStringProcessor("artist_name", "strip"),
        PolarsStringProcessor("artist_name", "upper"),
    ]).process(result)

    # Report
    elapsed_ms = (time.perf_counter() - start_time) * 1000
    context.add_output_metadata({
        "record_count": len(result),
        "columns": result.columns,
        "preview": dg.MetadataValue.md(
            result.head(5).to_pandas().to_markdown(index=False)  # type: ignore[operator]
        ),
        "unique_artworks": result["artwork_id"].n_unique(),
        "total_sales_value": float(result["sale_price_usd"].sum()),
        "date_range": f"{str(result['sale_date'].min())} to {str(result['sale_date'].max())}",
        "processing_time_ms": round(elapsed_ms, 2),
    })
    context.log.info(f"Transformed {len(result)} sales records in {elapsed_ms:.1f}ms")
    return result  # type: ignore[return-value]


@dg.asset(
    kinds={"duckdb", "json"},
    # Depends on artworks_output for operational ordering (DuckDB single-writer lock),
    # not for data. Data comes from sales_transform via IO manager parameter.
    deps=["artworks_output"],
    group_name="output",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def sales_output(
    context: dg.AssetExecutionContext,
    sales_transform: pl.DataFrame,
) -> pl.DataFrame:
    """Filter high-value sales and output to JSON."""
    start_time = time.perf_counter()
    total_count = len(sales_transform)

    # Transform: filter high-value sales
    result = PolarsFilterProcessor(
        "sale_price_usd", MIN_SALE_VALUE_USD, ">="
    ).process(sales_transform)

    # Output
    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(result, SALES_OUTPUT_PATH, context, extra_metadata={
        "filtered_from": total_count,
        "filter_threshold": f"${MIN_SALE_VALUE_USD:,}",
        "total_value": float(result["sale_price_usd"].sum()),
        "processing_time_ms": round(elapsed_ms, 2),
    })
    context.log.info(f"Output {len(result)} high-value sales to {SALES_OUTPUT_PATH} in {elapsed_ms:.1f}ms")
    return result


# -----------------------------------------------------------------------------
# Artworks Pipeline: artworks_transform → artworks_output
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"duckdb"},
    deps=HARVEST_DEPS,
    group_name="transform",
)
def artworks_transform(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Join artworks with artists, media, and aggregate sales history per artwork."""
    start_time = time.perf_counter()

    # Ensure Parquet views are set up
    _ensure_parquet_views()

    # Extract: aggregate sales per artwork (from database)
    sales_per_artwork = DuckDBQueryProcessor(sql="""
        SELECT
            s.artwork_id,
            COUNT(*) AS sale_count,
            SUM(s.sale_price_usd) AS total_sales_value,
            ROUND(AVG(s.sale_price_usd), 0) AS avg_sale_price,
            MIN(s.sale_date) AS first_sale_date,
            MAX(s.sale_date) AS last_sale_date
        FROM raw.sales_raw s
        JOIN raw.artworks_raw aw ON s.artwork_id = aw.artwork_id
        GROUP BY s.artwork_id
    """).process()

    # Extract: build artwork catalog (from database)
    catalog = DuckDBQueryProcessor(sql="""
        SELECT
            aw.artwork_id,
            aw.title,
            aw.year,
            aw.medium,
            aw.price_usd AS list_price_usd,
            ar.name AS artist_name,
            ar.nationality
        FROM raw.artworks_raw aw
        LEFT JOIN raw.artists_raw ar ON aw.artist_id = ar.artist_id
    """).process()

    # Extract: process media
    media_df = read_table("media", schema="raw")

    primary_media = DuckDBSQLProcessor(sql="""
        SELECT artwork_id, filename AS primary_image, alt_text AS primary_image_alt
        FROM _input
        WHERE sort_order = 1
    """).process(media_df)

    all_media = DuckDBSQLProcessor(sql="""
        SELECT artwork_id,
            list({
                'sort_order': sort_order, 'filename': filename, 'media_type': media_type,
                'file_format': file_format, 'width_px': width_px, 'height_px': height_px,
                'file_size_kb': file_size_kb, 'alt_text': alt_text
            } ORDER BY sort_order) AS media
        FROM _input
        GROUP BY artwork_id
    """).process(media_df)

    # Transform: final assembly - join all intermediate results
    # Price tier thresholds: budget < $500k, mid < $3M, premium >= $3M
    result = DuckDBSQLProcessor(sql=f"""
        SELECT
            c.*,
            COALESCE(s.sale_count, 0) AS sale_count,
            COALESCE(s.total_sales_value, 0) AS total_sales_value,
            s.avg_sale_price,
            s.first_sale_date,
            s.last_sale_date,
            CASE WHEN s.sale_count > 0 THEN true ELSE false END AS has_sold,
            CASE
                WHEN c.list_price_usd < {PRICE_TIER_BUDGET_MAX_USD} THEN 'budget'
                WHEN c.list_price_usd < {PRICE_TIER_MID_MAX_USD} THEN 'mid'
                ELSE 'premium'
            END AS price_tier,
            RANK() OVER (ORDER BY COALESCE(s.total_sales_value, 0) DESC) AS sales_rank,
            pm.primary_image,
            pm.primary_image_alt,
            COALESCE(len(am.media), 0) AS media_count,
            am.media
        FROM _input c
        LEFT JOIN sales_per_artwork s ON c.artwork_id = s.artwork_id
        LEFT JOIN primary_media pm ON c.artwork_id = pm.artwork_id
        LEFT JOIN all_media am ON c.artwork_id = am.artwork_id
        ORDER BY COALESCE(s.total_sales_value, 0) DESC, c.artwork_id
    """).process(catalog, tables={
        "sales_per_artwork": sales_per_artwork,
        "primary_media": primary_media,
        "all_media": all_media,
    })

    # Transform: normalize artist names (converts to Polars)
    result: pl.DataFrame = PolarsStringProcessor("artist_name", "upper").process(result)  # type: ignore[no-redef]

    # Report
    elapsed_ms = (time.perf_counter() - start_time) * 1000
    context.add_output_metadata({
        "record_count": len(result),
        "artworks_sold": int(result["has_sold"].sum()),
        "artworks_unsold": int((~result["has_sold"]).sum()),
        "artworks_with_media": int((result["media_count"] > 0).sum()),
        "total_catalog_value": float(result["list_price_usd"].sum()),
        "preview": dg.MetadataValue.md(
            result.head(5).to_pandas().to_markdown(index=False)  # type: ignore[operator]
        ),
        "processing_time_ms": round(elapsed_ms, 2),
    })
    context.log.info(f"Transformed {len(result)} artworks in {elapsed_ms:.1f}ms")
    return result  # type: ignore[return-value]


@dg.asset(
    kinds={"duckdb", "json"},
    group_name="output",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def artworks_output(
    context: dg.AssetExecutionContext,
    artworks_transform: pl.DataFrame,
) -> pl.DataFrame:
    """Output artwork catalog to JSON."""
    start_time = time.perf_counter()

    # Convert to dict with native Python types for JSON serialization
    vc = artworks_transform["price_tier"].value_counts()
    tier_counts = dict(zip(vc["price_tier"].to_list(), vc["count"].to_list()))

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    write_json_output(artworks_transform, ARTWORKS_OUTPUT_PATH, context, extra_metadata={
        "price_tier_distribution": tier_counts,
        "processing_time_ms": round(elapsed_ms, 2),
    })
    context.log.info(f"Output {len(artworks_transform)} artworks to {ARTWORKS_OUTPUT_PATH} in {elapsed_ms:.1f}ms")
    return artworks_transform
