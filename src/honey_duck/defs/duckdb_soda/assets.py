"""DuckDB + Soda implementation of honey-duck pipeline transforms.

Asset Graph:
    dlt_harvest_* (shared) --> sales_transform_soda --> sales_output_soda
                           └--> artworks_transform_soda --> artworks_output_soda

Key patterns:
- ParquetPathIOManager passes file paths between assets (no DataFrames)
- DuckDB queries parquet directly: FROM 'file.parquet'
- Views for clean SQL (no glob paths in queries)
- Relation.write_parquet() for output
- Column lineage with example data for traceability
"""

import time
from datetime import timedelta
from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource

from ..shared.constants import (
    MIN_SALE_VALUE_USD,
    PRICE_TIER_BUDGET_MAX_USD,
    PRICE_TIER_MID_MAX_USD,
)
from ..shared.resources import OutputPathsResource, PathsResource
from ..shared.helpers import STANDARD_HARVEST_DEPS as HARVEST_DEPS

# -----------------------------------------------------------------------------
# Asset Keys for Lineage
# -----------------------------------------------------------------------------

SALES_RAW = dg.AssetKey("dlt_harvest_sales_raw")
ARTWORKS_RAW = dg.AssetKey("dlt_harvest_artworks_raw")
ARTISTS_RAW = dg.AssetKey("dlt_harvest_artists_raw")
MEDIA_RAW = dg.AssetKey("dlt_harvest_media")
SALES_TRANSFORM = dg.AssetKey("sales_transform_soda")
ARTWORKS_TRANSFORM = dg.AssetKey("artworks_transform_soda")


# -----------------------------------------------------------------------------
# Column Lineage Definitions
# -----------------------------------------------------------------------------


def _dep(asset_key, col):
    """Shorthand for TableColumnDep."""
    return dg.TableColumnDep(asset_key=asset_key, column_name=col)


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
# Helpers
# -----------------------------------------------------------------------------


def _register_views(conn, harvest_dir: str):
    """Register parquet sources as views."""
    conn.execute(
        f"CREATE OR REPLACE VIEW sales AS SELECT * FROM '{harvest_dir}/raw/sales_raw/**/*.parquet'"
    )
    conn.execute(
        f"CREATE OR REPLACE VIEW artworks AS SELECT * FROM '{harvest_dir}/raw/artworks_raw/**/*.parquet'"
    )
    conn.execute(
        f"CREATE OR REPLACE VIEW artists AS SELECT * FROM '{harvest_dir}/raw/artists_raw/**/*.parquet'"
    )
    conn.execute(
        f"CREATE OR REPLACE VIEW media AS SELECT * FROM '{harvest_dir}/raw/media/**/*.parquet'"
    )


def _preview_md(rel, limit=5):
    """Markdown table from relation."""
    cols, rows = rel.columns, rel.limit(limit).fetchall()
    if not rows:
        return "*No data*"

    def fmt(v):
        if v is None:
            return ""
        s = str(v)
        return s[:50] + "..." if len(s) > 50 else s

    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join(["---"] * len(cols)) + " |",
        *["| " + " | ".join(fmt(c) for c in row) + " |" for row in rows],
    ]
    return "\n".join(lines)


def _example_row(
    conn, path: str, id_field: str | None = None, id_value: int | str | None = None
) -> dict:
    """Get one example row as dict for lineage examples.

    Args:
        conn: DuckDB connection
        path: Path to parquet file
        id_field: Optional field name to filter by (e.g., "sale_id")
        id_value: Optional value to match (e.g., 2)

    If id_field and id_value are provided, returns that specific record.
    Otherwise returns the first row.
    """
    if id_field and id_value is not None:
        # Quote string values
        val = f"'{id_value}'" if isinstance(id_value, str) else id_value
        result = conn.sql(f"SELECT * FROM '{path}' WHERE {id_field} = {val} LIMIT 1")
    else:
        result = conn.sql(f"SELECT * FROM '{path}' LIMIT 1")

    row = result.fetchone()
    if not row:
        return {}
    return {col: _format_value(val) for col, val in zip(result.columns, row)}


def _format_value(val):
    """Format value for display."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        abs_val = abs(val)
        sign = "-" if val < 0 else ""
        if abs_val >= 1_000_000:
            return f"{sign}${abs_val / 1_000_000:.1f}M"
        if abs_val >= 1_000:
            return f"{sign}${abs_val / 1_000:.1f}K"
        if isinstance(val, float):
            return f"{sign}${abs_val:.2f}"
        return f"{val:,}"
    return str(val)


def _lineage_metadata(
    conn,
    path: str,
    lineage_fn,
    renames: dict | None = None,
    id_field: str | None = None,
    id_value: int | str | None = None,
) -> dict:
    """Generate column lineage metadata with example values.

    Args:
        conn: DuckDB connection
        path: Path to parquet file to sample
        lineage_fn: Function returning TableColumnLineage
        renames: Optional dict mapping new_name -> old_name for column renames
        id_field: Optional field to filter by for specific record example
        id_value: Optional value to match for specific record example
    """
    examples = _example_row(conn, path, id_field, id_value)
    if renames:
        for new_name, old_name in renames.items():
            examples[new_name] = examples.pop(old_name, None)
    return {
        "dagster/column_lineage": lineage_fn(),
        "lineage_examples": examples,
    }


# -----------------------------------------------------------------------------
# SQL Queries
# -----------------------------------------------------------------------------

SALES_TRANSFORM_SQL = """
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
    upper(trim(ar.name)) AS artist_name,
    ar.nationality,
    s.sale_price_usd - aw.price_usd AS price_diff,
    round((s.sale_price_usd - aw.price_usd) * 100.0 / nullif(aw.price_usd, 0), 1) AS pct_change
FROM sales s
LEFT JOIN artworks aw USING (artwork_id)
LEFT JOIN artists ar ON aw.artist_id = ar.artist_id
ORDER BY s.sale_date DESC, s.sale_id
"""

ARTWORKS_TRANSFORM_SQL = f"""
WITH sales_agg AS (
    SELECT
        s.artwork_id,
        count(*) AS sale_count,
        sum(s.sale_price_usd) AS total_sales_value,
        round(avg(s.sale_price_usd), 0) AS avg_sale_price,
        min(s.sale_date) AS first_sale_date,
        max(s.sale_date) AS last_sale_date
    FROM sales s
    JOIN artworks aw USING (artwork_id)
    GROUP BY s.artwork_id
),
catalog AS (
    SELECT
        aw.artwork_id, aw.title, aw.year, aw.medium,
        aw.price_usd AS list_price_usd,
        ar.name AS artist_name,
        ar.nationality
    FROM artworks aw
    LEFT JOIN artists ar USING (artist_id)
),
primary_media AS (
    SELECT artwork_id, filename AS primary_image, alt_text AS primary_image_alt
    FROM media
    WHERE sort_order = 1
),
all_media AS (
    SELECT artwork_id, list({{
        'sort_order': sort_order, 'filename': filename,
        'media_type': media_type, 'file_format': file_format,
        'width_px': width_px, 'height_px': height_px,
        'file_size_kb': file_size_kb, 'alt_text': alt_text
    }} ORDER BY sort_order) AS media
    FROM media
    GROUP BY artwork_id
)
SELECT
    c.artwork_id, c.title, c.year, c.medium, c.list_price_usd,
    upper(c.artist_name) AS artist_name,
    c.nationality,
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
    coalesce(length(am.media), 0) AS media_count,
    am.media
FROM catalog c
LEFT JOIN sales_agg s USING (artwork_id)
LEFT JOIN primary_media pm USING (artwork_id)
LEFT JOIN all_media am USING (artwork_id)
ORDER BY coalesce(s.total_sales_value, 0) DESC, c.artwork_id
"""


# -----------------------------------------------------------------------------
# Sales Pipeline
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
        _register_views(conn, paths.harvest_dir)
        conn.sql(SALES_TRANSFORM_SQL).write_parquet(str(output_path), compression="zstd")

        stats = conn.sql(f"""
            SELECT count(*), count(DISTINCT artwork_id), sum(sale_price_usd), min(sale_date), max(sale_date)
            FROM '{output_path}'
        """).fetchone()

        preview = _preview_md(conn.sql(f"SELECT * FROM '{output_path}'"))
        # Use sale_id=2 (Day Dream, $86M sale) as the example
        lineage = _lineage_metadata(
            conn, str(output_path), _sales_transform_lineage, id_field="sale_id", id_value=2
        )

    elapsed = (time.perf_counter() - start) * 1000
    context.add_output_metadata(
        {
            **lineage,
            "record_count": stats[0],
            "unique_artworks": stats[1],
            "total_sales_value": float(stats[2] or 0),
            "date_range": f"{stats[3]} to {stats[4]}",
            "preview": dg.MetadataValue.md(preview),
            "processing_time_ms": round(elapsed, 2),
        }
    )
    context.log.info(f"Transformed {stats[0]} sales in {elapsed:.1f}ms")
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

        # Filter, rename price_diff -> price_difference, write JSON
        conn.execute(f"""
            COPY (
                SELECT * EXCLUDE (price_diff), price_diff AS price_difference
                FROM '{sales_transform_soda}'
                WHERE sale_price_usd >= {MIN_SALE_VALUE_USD}
            ) TO '{output_path}' (FORMAT JSON, ARRAY true)
        """)

        filtered = conn.sql(f"""
            SELECT count(*), sum(sale_price_usd)
            FROM '{sales_transform_soda}'
            WHERE sale_price_usd >= {MIN_SALE_VALUE_USD}
        """).fetchone()

        # Use sale_id=2 (Day Dream, $86M sale) as the example
        lineage = _lineage_metadata(
            conn,
            sales_transform_soda,
            _sales_output_lineage,
            renames={"price_difference": "price_diff"},
            id_field="sale_id",
            id_value=2,
        )

    elapsed = (time.perf_counter() - start) * 1000
    context.add_output_metadata(
        {
            **lineage,
            "record_count": filtered[0],
            "filtered_from": total,
            "filter_threshold": f"${MIN_SALE_VALUE_USD:,}",
            "total_value": float(filtered[1] or 0),
            "json_output": dg.MetadataValue.path(output_path),
            "processing_time_ms": round(elapsed, 2),
        }
    )
    context.log.info(f"Output {filtered[0]} high-value sales in {elapsed:.1f}ms")
    return sales_transform_soda


# -----------------------------------------------------------------------------
# Artworks Pipeline
# -----------------------------------------------------------------------------


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
        _register_views(conn, paths.harvest_dir)
        conn.sql(ARTWORKS_TRANSFORM_SQL).write_parquet(str(output_path), compression="zstd")

        stats = conn.sql(f"""
            SELECT count(*),
                   sum(CASE WHEN has_sold THEN 1 ELSE 0 END),
                   sum(CASE WHEN NOT has_sold THEN 1 ELSE 0 END),
                   sum(CASE WHEN media_count > 0 THEN 1 ELSE 0 END),
                   sum(list_price_usd)
            FROM '{output_path}'
        """).fetchone()

        preview = _preview_md(conn.sql(f"SELECT * FROM '{output_path}'"))
        # Use artwork_id=2 (Day Dream) as the example
        lineage = _lineage_metadata(
            conn, str(output_path), _artworks_transform_lineage, id_field="artwork_id", id_value=2
        )

    elapsed = (time.perf_counter() - start) * 1000
    context.add_output_metadata(
        {
            **lineage,
            "record_count": stats[0],
            "artworks_sold": stats[1],
            "artworks_unsold": stats[2],
            "artworks_with_media": stats[3],
            "total_catalog_value": float(stats[4] or 0),
            "preview": dg.MetadataValue.md(preview),
            "processing_time_ms": round(elapsed, 2),
        }
    )
    context.log.info(f"Transformed {stats[0]} artworks in {elapsed:.1f}ms")
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

        stats = conn.sql(f"""
            SELECT price_tier, count(*) FROM '{artworks_transform_soda}' GROUP BY price_tier
        """).fetchall()
        tier_dist = {row[0]: row[1] for row in stats}
        record_count = sum(tier_dist.values())

        # Use artwork_id=2 (Day Dream) as the example
        lineage = _lineage_metadata(
            conn,
            artworks_transform_soda,
            _artworks_output_lineage,
            id_field="artwork_id",
            id_value=2,
        )

    elapsed = (time.perf_counter() - start) * 1000
    context.add_output_metadata(
        {
            **lineage,
            "record_count": record_count,
            "price_tier_distribution": tier_dist,
            "json_output": dg.MetadataValue.path(output_path),
            "processing_time_ms": round(elapsed, 2),
        }
    )
    context.log.info(f"Output {record_count} artworks in {elapsed:.1f}ms")
    return artworks_transform_soda
