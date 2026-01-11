"""Example assets using Elasticsearch IO Manager for search and analytics.

This file demonstrates how to use ElasticsearchIOManager to index pipeline
outputs for full-text search, analytics, and Kibana dashboards.

To use:
1. Start Elasticsearch:
   docker-compose -f docker-compose.elasticsearch.yml up -d

2. Add to definitions.py:
   from honey_duck.defs.elasticsearch_example import (
       sales_searchable,
       artworks_searchable,
   )

3. Configure IO manager in definitions.py:
   resources={
       "elasticsearch_io_manager": ElasticsearchIOManager(
           hosts=["http://localhost:9200"],
           index_prefix="honey_duck_",
       ),
   }

4. Materialize assets:
   uv run dagster asset materialize -a sales_searchable
"""

import dagster as dg
import polars as pl

from cogapp_deps.dagster import add_dataframe_metadata, track_timing
from honey_duck.defs.helpers import AssetGroups

# ============================================================================
# Custom Elasticsearch Mappings
# ============================================================================

SALES_MAPPING = {
    "properties": {
        # Exact-match fields (for filtering/aggregations)
        "sale_id": {"type": "keyword"},
        "artwork_id": {"type": "keyword"},
        "artist_name": {"type": "keyword"},
        "buyer_country": {"type": "keyword"},
        "price_tier": {"type": "keyword"},
        # Numeric fields
        "sale_price_usd": {"type": "float"},
        "sale_year": {"type": "integer"},
        # Date fields
        "sale_date": {
            "type": "date",
            "format": "yyyy-MM-dd||epoch_millis",
        },
        # Full-text search fields (with keyword subfield for exact match)
        "artwork_title": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword"}},
        },
        "artist_bio": {"type": "text"},
        # Geographic data (if available)
        "sale_location": {"type": "geo_point"},  # {"lat": 40.7, "lon": -74.0}
    }
}

ARTWORKS_MAPPING = {
    "properties": {
        "artwork_id": {"type": "keyword"},
        "title": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword"}},
        },
        "artist_name": {"type": "keyword"},
        "medium": {"type": "keyword"},
        "year": {"type": "integer"},
        "total_sales": {"type": "integer"},
        "total_revenue_usd": {"type": "float"},
        "avg_sale_price_usd": {"type": "float"},
        "max_sale_price_usd": {"type": "float"},
        "last_sale_date": {"type": "date"},
    }
}


# ============================================================================
# Elasticsearch Output Assets
# ============================================================================


@dg.asset(
    io_manager_key="elasticsearch_io_manager",
    kinds={"polars", "elasticsearch"},
    group_name=AssetGroups.OUTPUT_ELASTICSEARCH,
    deps=["sales_transform"],
)
def sales_searchable(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """High-value sales indexed to Elasticsearch for full-text search and analytics.

    Enables:
    - Full-text search on artwork titles and artist names
    - Filtering by price, date, location, tier
    - Aggregations and analytics in Kibana
    - Real-time dashboards showing sales trends

    Index: honey_duck_sales_searchable

    Example Kibana queries:
    - Search: "Picasso" in artwork_title or artist_name
    - Filter: sale_price_usd >= 50000000
    - Aggregate: Average sale price by artist_name
    """
    with track_timing(context, "loading_and_filtering"):
        # In production, you'd load from sales_transform asset
        # For this example, we'll create sample data
        result = pl.DataFrame(
            {
                "sale_id": ["S001", "S002", "S003"],
                "artwork_id": ["A001", "A002", "A003"],
                "artwork_title": [
                    "Les Femmes d'Alger",
                    "Salvator Mundi",
                    "The Card Players",
                ],
                "artist_name": ["Pablo Picasso", "Leonardo da Vinci", "Paul Cézanne"],
                "sale_price_usd": [179_400_000.0, 450_300_000.0, 250_000_000.0],
                "sale_date": ["2015-05-11", "2017-11-15", "2011-04-01"],
                "sale_year": [2015, 2017, 2011],
                "price_tier": ["premium", "premium", "premium"],
                "buyer_country": ["QA", "SA", "QA"],
            }
        )

    add_dataframe_metadata(
        context,
        result,
        total_value=float(result["sale_price_usd"].sum()),
        avg_price=float(result["sale_price_usd"].mean()),
        unique_artworks=result["artwork_id"].n_unique(),
    )

    context.log.info(
        f"Indexing {len(result):,} sales to Elasticsearch for search and analytics"
    )

    return result


@dg.asset(
    io_manager_key="elasticsearch_io_manager",
    kinds={"polars", "elasticsearch"},
    group_name=AssetGroups.OUTPUT_ELASTICSEARCH,
    deps=["artworks_transform"],
)
def artworks_searchable(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Artwork catalog with sales history indexed to Elasticsearch.

    Enables:
    - Full-text search on artwork titles
    - Filtering by medium, artist, price range
    - Sales history aggregations
    - Top artworks dashboards in Kibana

    Index: honey_duck_artworks_searchable

    Example Kibana visualizations:
    - Top 10 artists by total revenue
    - Most expensive artworks (max_sale_price_usd)
    - Sales volume over time (total_sales)
    """
    with track_timing(context, "loading_and_aggregating"):
        # In production, load from artworks_transform
        result = pl.DataFrame(
            {
                "artwork_id": ["A001", "A002", "A003"],
                "title": ["Les Femmes d'Alger", "Salvator Mundi", "The Card Players"],
                "artist_name": ["Pablo Picasso", "Leonardo da Vinci", "Paul Cézanne"],
                "medium": ["Oil on canvas", "Oil on panel", "Oil on canvas"],
                "year": [1955, 1500, 1895],
                "total_sales": [3, 1, 2],
                "total_revenue_usd": [300_000_000.0, 450_300_000.0, 500_000_000.0],
                "avg_sale_price_usd": [100_000_000.0, 450_300_000.0, 250_000_000.0],
                "max_sale_price_usd": [179_400_000.0, 450_300_000.0, 250_000_000.0],
                "last_sale_date": ["2015-05-11", "2017-11-15", "2012-01-01"],
            }
        )

    add_dataframe_metadata(
        context,
        result,
        total_artworks=len(result),
        total_sales=int(result["total_sales"].sum()),
        total_revenue=float(result["total_revenue_usd"].sum()),
    )

    context.log.info(f"Indexing {len(result):,} artworks to Elasticsearch catalog")

    return result


@dg.asset(
    io_manager_key="elasticsearch_io_manager",
    kinds={"polars", "elasticsearch"},
    group_name=AssetGroups.OUTPUT_ELASTICSEARCH,
)
def daily_sales_metrics(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Daily sales metrics for time-series analysis in Kibana.

    Creates a time-series index with daily aggregations for trend analysis.

    Enables:
    - Time-series visualizations in Kibana
    - Sales trends over time
    - Revenue forecasting
    - Peak sales period identification

    Index: honey_duck_daily_sales_metrics

    Kibana Time Series Visualization:
    - X-axis: date (Date Histogram)
    - Y-axis: total_revenue_usd (Sum)
    - Line chart showing sales trends
    """
    # Generate daily metrics (in production, aggregate from sales data)
    result = pl.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
            "total_revenue_usd": [150_000_000.0, 200_000_000.0, 175_000_000.0, 220_000_000.0],
            "total_sales": [30, 40, 35, 44],
            "avg_sale_price_usd": [5_000_000.0, 5_000_000.0, 5_000_000.0, 5_000_000.0],
            "high_value_sales": [5, 8, 6, 9],  # Sales > $10M
        }
    )

    add_dataframe_metadata(
        context,
        result,
        total_revenue=float(result["total_revenue_usd"].sum()),
        avg_daily_revenue=float(result["total_revenue_usd"].mean()),
        total_sales=int(result["total_sales"].sum()),
    )

    context.log.info(f"Indexing {len(result)} days of sales metrics for time-series analysis")

    return result


# ============================================================================
# Example: Reading from Elasticsearch
# ============================================================================


@dg.asset(
    kinds={"polars"},
    group_name=AssetGroups.ANALYTICS,
)
def top_artists_analysis(
    context: dg.AssetExecutionContext,
    sales_searchable: pl.DataFrame,  # ← Auto-loaded from Elasticsearch
) -> pl.DataFrame:
    """Analyze top artists by revenue using data from Elasticsearch.

    Demonstrates reading from Elasticsearch and performing additional analysis.
    """
    with track_timing(context, "aggregation"):
        result = sales_searchable.group_by("artist_name").agg(
            [
                pl.col("sale_price_usd").sum().alias("total_revenue"),
                pl.col("sale_price_usd").mean().alias("avg_sale_price"),
                pl.col("sale_id").count().alias("sale_count"),
            ]
        ).sort("total_revenue", descending=True)

    add_dataframe_metadata(
        context,
        result,
        top_artist=result["artist_name"][0] if len(result) > 0 else None,
        top_revenue=float(result["total_revenue"][0]) if len(result) > 0 else 0,
    )

    context.log.info(f"Analyzed {len(result)} artists from Elasticsearch data")

    return result


# ============================================================================
# Export all assets
# ============================================================================

__all__ = [
    "SALES_MAPPING",
    "ARTWORKS_MAPPING",
    "sales_searchable",
    "artworks_searchable",
    "daily_sales_metrics",
    "top_artists_analysis",
]
