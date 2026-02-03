"""DuckDB + Soda pipeline with column lineage tracking.

Asset Graph:
    dlt_harvest_* --> sales_transform_soda --> sales_output_soda
                  └-> artworks_transform_soda --> artworks_output_soda

Patterns:
- ParquetPathIOManager passes file paths (not DataFrames)
- DuckDB queries parquet directly: FROM 'file.parquet'
- Column lineage with example data for traceability
"""

import time
from datetime import timedelta
from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource

from cogapp_libs.dagster.lineage import get_example_row

from ..shared.constants import (
    MIN_SALE_VALUE_USD,
    PRICE_TIER_BUDGET_MAX_USD,
    PRICE_TIER_MID_MAX_USD,
)
from ..shared.helpers import STANDARD_HARVEST_DEPS as HARVEST_DEPS
from ..shared.resources import OutputPathsResource, PathsResource

# -----------------------------------------------------------------------------
# Asset Keys for Lineage
# -----------------------------------------------------------------------------

SALES_RAW = dg.AssetKey("dlt_harvest_sales_raw")
ARTWORKS_RAW = dg.AssetKey("dlt_harvest_artworks_raw")
ARTISTS_RAW = dg.AssetKey("dlt_harvest_artists_raw")
MEDIA_RAW = dg.AssetKey("dlt_harvest_media")
SALES_TRANSFORM = dg.AssetKey("sales_transform_soda")
ARTWORKS_TRANSFORM = dg.AssetKey("artworks_transform_soda")


def _dep(asset_key, col):
    """Shorthand for TableColumnDep."""
    return dg.TableColumnDep(asset_key=asset_key, column_name=col)


# -----------------------------------------------------------------------------
# Column Lineage Definitions
# -----------------------------------------------------------------------------


def _sales_transform_lineage():
    return dg.TableColumnLineage(
        deps_by_column={
            "sale_id": [_dep(SALES_RAW, "sale_id")],
            "artwork_id": [_dep(SALES_RAW, "artwork_id")],
            "sale_date": [_dep(SALES_RAW, "sale_date")],
            "sale_price_usd": [_dep(SALES_RAW, "sale_price_usd")],
            "buyer_country": [_dep(SALES_RAW, "buyer_country")],
            "title": [_dep(ARTWORKS_RAW, "title")],
            "artist_id": [_dep(ARTWORKS_RAW, "artist_id")],
            "artwork_year": [_dep(ARTWORKS_RAW, "year")],
            "medium": [_dep(ARTWORKS_RAW, "medium")],
            "list_price_usd": [_dep(ARTWORKS_RAW, "price_usd")],
            "artist_name": [_dep(ARTISTS_RAW, "name")],
            "nationality": [_dep(ARTISTS_RAW, "nationality")],
            "price_diff": [_dep(SALES_RAW, "sale_price_usd"), _dep(ARTWORKS_RAW, "price_usd")],
            "pct_change": [_dep(SALES_RAW, "sale_price_usd"), _dep(ARTWORKS_RAW, "price_usd")],
        }
    )


def _artworks_transform_lineage():
    return dg.TableColumnLineage(
        deps_by_column={
            "artwork_id": [_dep(ARTWORKS_RAW, "artwork_id")],
            "title": [_dep(ARTWORKS_RAW, "title")],
            "year": [_dep(ARTWORKS_RAW, "year")],
            "medium": [_dep(ARTWORKS_RAW, "medium")],
            "list_price_usd": [_dep(ARTWORKS_RAW, "price_usd")],
            "artist_name": [_dep(ARTISTS_RAW, "name")],
            "nationality": [_dep(ARTISTS_RAW, "nationality")],
            "sale_count": [_dep(SALES_RAW, "sale_id")],
            "total_sales_value": [_dep(SALES_RAW, "sale_price_usd")],
            "avg_sale_price": [_dep(SALES_RAW, "sale_price_usd")],
            "first_sale_date": [_dep(SALES_RAW, "sale_date")],
            "last_sale_date": [_dep(SALES_RAW, "sale_date")],
            "has_sold": [_dep(SALES_RAW, "sale_id")],
            "price_tier": [_dep(ARTWORKS_RAW, "price_usd")],
            "sales_rank": [_dep(SALES_RAW, "sale_price_usd")],
            "primary_image": [_dep(MEDIA_RAW, "filename")],
            "primary_image_alt": [_dep(MEDIA_RAW, "alt_text")],
            "media_count": [_dep(MEDIA_RAW, "artwork_id")],
            "media": [
                _dep(MEDIA_RAW, "sort_order"),
                _dep(MEDIA_RAW, "filename"),
                _dep(MEDIA_RAW, "media_type"),
                _dep(MEDIA_RAW, "file_format"),
            ],
        }
    )


def _sales_output_lineage():
    passthrough = [
        "sale_id",
        "artwork_id",
        "sale_date",
        "sale_price_usd",
        "buyer_country",
        "title",
        "artist_id",
        "artwork_year",
        "medium",
        "list_price_usd",
        "artist_name",
        "nationality",
        "pct_change",
    ]
    deps = {col: [_dep(SALES_TRANSFORM, col)] for col in passthrough}
    deps["price_difference"] = [_dep(SALES_TRANSFORM, "price_diff")]
    return dg.TableColumnLineage(deps_by_column=deps)


def _artworks_output_lineage():
    cols = [
        "artwork_id",
        "title",
        "year",
        "medium",
        "list_price_usd",
        "artist_name",
        "nationality",
        "sale_count",
        "total_sales_value",
        "avg_sale_price",
        "first_sale_date",
        "last_sale_date",
        "has_sold",
        "price_tier",
        "sales_rank",
        "primary_image",
        "primary_image_alt",
        "media_count",
        "media",
    ]
    return dg.TableColumnLineage(
        deps_by_column={col: [_dep(ARTWORKS_TRANSFORM, col)] for col in cols}
    )


# -----------------------------------------------------------------------------
# SQL Queries
# -----------------------------------------------------------------------------

SALES_TRANSFORM_SQL = """
SELECT
    s.sale_id, s.artwork_id, s.sale_date, s.sale_price_usd, s.buyer_country,
    aw.title, aw.artist_id, aw.year AS artwork_year, aw.medium,
    aw.price_usd AS list_price_usd,
    upper(trim(ar.name)) AS artist_name, ar.nationality,
    s.sale_price_usd - aw.price_usd AS price_diff,
    round((s.sale_price_usd - aw.price_usd) * 100.0 / nullif(aw.price_usd, 0), 1) AS pct_change
FROM sales s
LEFT JOIN artworks aw USING (artwork_id)
LEFT JOIN artists ar ON aw.artist_id = ar.artist_id
ORDER BY s.sale_date DESC, s.sale_id
"""

ARTWORKS_TRANSFORM_SQL = f"""
WITH sales_agg AS (
    SELECT artwork_id,
        count(*) AS sale_count,
        sum(sale_price_usd) AS total_sales_value,
        round(avg(sale_price_usd), 0) AS avg_sale_price,
        min(sale_date) AS first_sale_date,
        max(sale_date) AS last_sale_date
    FROM sales s JOIN artworks aw USING (artwork_id)
    GROUP BY artwork_id
),
catalog AS (
    SELECT aw.artwork_id, aw.title, aw.year, aw.medium,
        aw.price_usd AS list_price_usd,
        ar.name AS artist_name, ar.nationality
    FROM artworks aw LEFT JOIN artists ar USING (artist_id)
),
primary_media AS (
    SELECT artwork_id, filename AS primary_image, alt_text AS primary_image_alt
    FROM media WHERE sort_order = 1
),
all_media AS (
    SELECT artwork_id, list({{
        'sort_order': sort_order, 'filename': filename,
        'media_type': media_type, 'file_format': file_format,
        'width_px': width_px, 'height_px': height_px,
        'file_size_kb': file_size_kb, 'alt_text': alt_text
    }} ORDER BY sort_order) AS media
    FROM media GROUP BY artwork_id
)
SELECT
    c.artwork_id, c.title, c.year, c.medium, c.list_price_usd,
    upper(c.artist_name) AS artist_name, c.nationality,
    coalesce(s.sale_count, 0) AS sale_count,
    coalesce(s.total_sales_value, 0) AS total_sales_value,
    s.avg_sale_price, s.first_sale_date, s.last_sale_date,
    s.sale_count > 0 AS has_sold,
    CASE
        WHEN c.list_price_usd < {PRICE_TIER_BUDGET_MAX_USD} THEN 'budget'
        WHEN c.list_price_usd < {PRICE_TIER_MID_MAX_USD} THEN 'mid'
        ELSE 'premium'
    END AS price_tier,
    rank() OVER (ORDER BY coalesce(s.total_sales_value, 0) DESC) AS sales_rank,
    pm.primary_image, pm.primary_image_alt,
    coalesce(length(am.media), 0) AS media_count, am.media
FROM catalog c
LEFT JOIN sales_agg s USING (artwork_id)
LEFT JOIN primary_media pm USING (artwork_id)
LEFT JOIN all_media am USING (artwork_id)
ORDER BY coalesce(s.total_sales_value, 0) DESC, c.artwork_id
"""


# -----------------------------------------------------------------------------
# Assets
# -----------------------------------------------------------------------------


@dg.asset(
    kinds={"duckdb", "sql", "soda"},
    deps=HARVEST_DEPS,
    group_name="transform_soda",
    io_manager_key="parquet_path_io_manager",
)
def sales_transform_soda(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    paths: PathsResource,
    output_paths: OutputPathsResource,
) -> str:
    """Join sales with artworks and artists."""
    start = time.perf_counter()
    output_path = Path(output_paths.transforms_soda_dir) / "sales_transform.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.get_connection() as conn:
        # Register source views
        conn.execute(
            f"CREATE OR REPLACE VIEW sales AS SELECT * FROM '{paths.harvest_dir}/raw/sales_raw/**/*.parquet'"
        )
        conn.execute(
            f"CREATE OR REPLACE VIEW artworks AS SELECT * FROM '{paths.harvest_dir}/raw/artworks_raw/**/*.parquet'"
        )
        conn.execute(
            f"CREATE OR REPLACE VIEW artists AS SELECT * FROM '{paths.harvest_dir}/raw/artists_raw/**/*.parquet'"
        )

        # Transform and write
        conn.sql(SALES_TRANSFORM_SQL).write_parquet(str(output_path), compression="zstd")

        # Get stats
        row_count, unique_artworks, total_value, min_date, max_date = conn.sql(f"""
            SELECT count(*), count(DISTINCT artwork_id), sum(sale_price_usd), min(sale_date), max(sale_date)
            FROM '{output_path}'
        """).fetchone()

        # Example row for lineage (Day Dream, sale_id=2)
        examples = get_example_row(conn, str(output_path), "sale_id", 2)

    elapsed = (time.perf_counter() - start) * 1000
    context.add_output_metadata(
        {
            "dagster/column_lineage": _sales_transform_lineage(),
            "lineage_examples": examples,
            "record_count": row_count,
            "unique_artworks": unique_artworks,
            "total_sales_value": float(total_value or 0),
            "date_range": f"{min_date} to {max_date}",
            "processing_time_ms": round(elapsed, 2),
        }
    )
    context.log.info(f"Transformed {row_count} sales in {elapsed:.1f}ms")
    return str(output_path)


@dg.asset(
    kinds={"duckdb", "sql", "json", "soda"},
    deps=["artworks_output_soda"],
    group_name="output_soda",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
    io_manager_key="parquet_path_io_manager",
)
def sales_output_soda(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    sales_transform_soda: str,
    output_paths: OutputPathsResource,
) -> str:
    """Filter high-value sales and output to JSON."""
    start = time.perf_counter()
    output_path = output_paths.sales_soda
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with duckdb.get_connection() as conn:
        total = conn.sql(f"SELECT count(*) FROM '{sales_transform_soda}'").fetchone()[0]

        # Filter and rename price_diff -> price_difference
        conn.execute(f"""
            COPY (
                SELECT * EXCLUDE (price_diff), price_diff AS price_difference
                FROM '{sales_transform_soda}'
                WHERE sale_price_usd >= {MIN_SALE_VALUE_USD}
            ) TO '{output_path}' (FORMAT JSON, ARRAY true)
        """)

        filtered_count, filtered_value = conn.sql(f"""
            SELECT count(*), sum(sale_price_usd)
            FROM '{sales_transform_soda}'
            WHERE sale_price_usd >= {MIN_SALE_VALUE_USD}
        """).fetchone()

        # Example row with renamed column
        examples = get_example_row(conn, sales_transform_soda, "sale_id", 2)
        examples["price_difference"] = examples.pop("price_diff", None)

    elapsed = (time.perf_counter() - start) * 1000
    context.add_output_metadata(
        {
            "dagster/column_lineage": _sales_output_lineage(),
            "lineage_examples": examples,
            "record_count": filtered_count,
            "filtered_from": total,
            "filter_threshold": f"${MIN_SALE_VALUE_USD:,}",
            "total_value": float(filtered_value or 0),
            "json_output": dg.MetadataValue.path(output_path),
            "processing_time_ms": round(elapsed, 2),
        }
    )
    context.log.info(f"Output {filtered_count} high-value sales in {elapsed:.1f}ms")
    return sales_transform_soda


@dg.asset(
    kinds={"duckdb", "sql", "soda"},
    deps=HARVEST_DEPS,
    group_name="transform_soda",
    io_manager_key="parquet_path_io_manager",
)
def artworks_transform_soda(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    paths: PathsResource,
    output_paths: OutputPathsResource,
) -> str:
    """Join artworks with artists, media, and aggregate sales."""
    start = time.perf_counter()
    output_path = Path(output_paths.transforms_soda_dir) / "artworks_transform.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.get_connection() as conn:
        # Register source views
        conn.execute(
            f"CREATE OR REPLACE VIEW sales AS SELECT * FROM '{paths.harvest_dir}/raw/sales_raw/**/*.parquet'"
        )
        conn.execute(
            f"CREATE OR REPLACE VIEW artworks AS SELECT * FROM '{paths.harvest_dir}/raw/artworks_raw/**/*.parquet'"
        )
        conn.execute(
            f"CREATE OR REPLACE VIEW artists AS SELECT * FROM '{paths.harvest_dir}/raw/artists_raw/**/*.parquet'"
        )
        conn.execute(
            f"CREATE OR REPLACE VIEW media AS SELECT * FROM '{paths.harvest_dir}/raw/media/**/*.parquet'"
        )

        # Transform and write
        conn.sql(ARTWORKS_TRANSFORM_SQL).write_parquet(str(output_path), compression="zstd")

        # Get stats
        row_count, sold, unsold, with_media, catalog_value = conn.sql(f"""
            SELECT count(*),
                sum(CASE WHEN has_sold THEN 1 ELSE 0 END),
                sum(CASE WHEN NOT has_sold THEN 1 ELSE 0 END),
                sum(CASE WHEN media_count > 0 THEN 1 ELSE 0 END),
                sum(list_price_usd)
            FROM '{output_path}'
        """).fetchone()

        # Example row (Day Dream, artwork_id=2)
        examples = get_example_row(conn, str(output_path), "artwork_id", 2)

    elapsed = (time.perf_counter() - start) * 1000
    context.add_output_metadata(
        {
            "dagster/column_lineage": _artworks_transform_lineage(),
            "lineage_examples": examples,
            "record_count": row_count,
            "artworks_sold": sold,
            "artworks_unsold": unsold,
            "artworks_with_media": with_media,
            "total_catalog_value": float(catalog_value or 0),
            "processing_time_ms": round(elapsed, 2),
        }
    )
    context.log.info(f"Transformed {row_count} artworks in {elapsed:.1f}ms")
    return str(output_path)


@dg.asset(
    kinds={"duckdb", "sql", "json", "soda"},
    group_name="output_soda",
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
    io_manager_key="parquet_path_io_manager",
)
def artworks_output_soda(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    artworks_transform_soda: str,
    output_paths: OutputPathsResource,
) -> str:
    """Output artwork catalog to JSON."""
    start = time.perf_counter()
    output_path = output_paths.artworks_soda
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with duckdb.get_connection() as conn:
        conn.execute(
            f"COPY (SELECT * FROM '{artworks_transform_soda}') TO '{output_path}' (FORMAT JSON, ARRAY true)"
        )

        tier_rows = conn.sql(
            f"SELECT price_tier, count(*) FROM '{artworks_transform_soda}' GROUP BY price_tier"
        ).fetchall()
        tier_dist = {row[0]: row[1] for row in tier_rows}
        record_count = sum(tier_dist.values())

        # Example row (Day Dream, artwork_id=2)
        examples = get_example_row(conn, artworks_transform_soda, "artwork_id", 2)

    elapsed = (time.perf_counter() - start) * 1000
    context.add_output_metadata(
        {
            "dagster/column_lineage": _artworks_output_lineage(),
            "lineage_examples": examples,
            "record_count": record_count,
            "price_tier_distribution": tier_dist,
            "json_output": dg.MetadataValue.path(output_path),
            "processing_time_ms": round(elapsed, 2),
        }
    )
    context.log.info(f"Output {record_count} artworks in {elapsed:.1f}ms")
    return artworks_transform_soda
