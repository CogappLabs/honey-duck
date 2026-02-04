---
title: Elasticsearch
description: Index pipeline outputs to Elasticsearch with bulk operations, custom mappings, and the ElasticsearchIOManager.
---

# Elasticsearch Integration Guide

Complete guide for using Elasticsearch 8/9 as an output target for Dagster pipelines.

## Why Elasticsearch?

Elasticsearch is ideal for:
- **Full-text search**: Enable powerful search on pipeline outputs
- **Analytics**: Build Kibana dashboards on your data
- **Real-time queries**: Sub-second query latency
- **Time-series data**: Monitor pipeline metrics over time
- **API integration**: Expose pipeline data via REST API

## Quick Start

### 1. Install Elasticsearch Client

```bash
# Add to pyproject.toml or requirements.txt
pip install elasticsearch>=8.0.0
```

### 2. Start Elasticsearch (Docker)

```bash
# docker-compose.elasticsearch.yml
version: '3.8'

services:
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.12.0
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false  # For development only!
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
    ports:
      - "9200:9200"
    volumes:
      - elasticsearch-data:/usr/share/elasticsearch/data

  kibana:
    image: docker.elastic.co/kibana/kibana:8.12.0
    ports:
      - "5601:5601"
    environment:
      - ELASTICSEARCH_HOSTS=http://elasticsearch:9200
    depends_on:
      - elasticsearch

volumes:
  elasticsearch-data:
```

```bash
# Start services
docker-compose -f docker-compose.elasticsearch.yml up -d

# Verify Elasticsearch is running
curl http://localhost:9200

# Access Kibana UI
open http://localhost:5601
```

### 3. Configure IO Manager

```python
# src/honey_duck/defs/definitions.py
import os
from cogapp_libs.dagster import ElasticsearchIOManager

defs = dg.Definitions(
    assets=[sales_output, artworks_output],
    resources={
        "elasticsearch_io_manager": ElasticsearchIOManager(
            hosts=[os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")],
            index_prefix="honey_duck_",
            bulk_size=1000,
        ),
    },
)
```

### 4. Use in Assets

```python
@dg.asset(io_manager_key="elasticsearch_io_manager")
def sales_output(
    context: dg.AssetExecutionContext,
    sales_transform: pl.DataFrame,
) -> pl.DataFrame:
    """High-value sales indexed to Elasticsearch for search."""
    return sales_transform.filter(pl.col("sale_price_usd") > 30_000_000)
```

That's it! When you materialize `sales_output`, data is automatically indexed to Elasticsearch.

---

## Authentication

### Option 1: API Key (Recommended)

```python
# Generate API key in Kibana or via API
# Stack Management → API Keys → Create API Key

import os

ElasticsearchIOManager(
    hosts=["https://my-cluster.es.cloud:9243"],
    api_key=os.getenv("ELASTICSEARCH_API_KEY"),  # Secure!
    verify_certs=True,
)
```

### Option 2: Basic Auth

```python
ElasticsearchIOManager(
    hosts=["http://localhost:9200"],
    basic_auth=(
        os.getenv("ELASTICSEARCH_USER", "elastic"),
        os.getenv("ELASTICSEARCH_PASSWORD", "changeme"),
    ),
)
```

### Option 3: Cloud ID (Elastic Cloud)

```python
from elasticsearch import Elasticsearch

# For Elastic Cloud, use cloud_id
ElasticsearchIOManager(
    hosts=[],  # Not used with cloud_id
    es_kwargs={
        "cloud_id": os.getenv("ELASTIC_CLOUD_ID"),
        "api_key": os.getenv("ELASTICSEARCH_API_KEY"),
    },
)
```

---

## Custom Mappings

Define explicit field types for better control:

```python
# Custom mapping for sales data
sales_mapping = {
    "properties": {
        # Exact-match fields (for filtering/aggregations)
        "sale_id": {"type": "keyword"},
        "artwork_id": {"type": "keyword"},
        "artist_name": {"type": "keyword"},
        "buyer_country": {"type": "keyword"},

        # Numeric fields
        "sale_price_usd": {"type": "float"},
        "sale_year": {"type": "integer"},

        # Date fields
        "sale_date": {
            "type": "date",
            "format": "yyyy-MM-dd||epoch_millis"
        },

        # Full-text search fields
        "artwork_title": {
            "type": "text",
            "fields": {
                "keyword": {"type": "keyword"}  # For exact match
            }
        },
        "sale_notes": {"type": "text"},

        # Nested objects
        "artist": {
            "type": "nested",
            "properties": {
                "name": {"type": "keyword"},
                "birth_year": {"type": "integer"},
            }
        },
    }
}

# Apply mapping to specific assets
ElasticsearchIOManager(
    hosts=["http://localhost:9200"],
    custom_mappings={
        "sales_output": sales_mapping,
        "artworks_output": artworks_mapping,
    },
)
```

---

## Usage Examples

### Example 1: Basic Output to Elasticsearch

```python
@dg.asset(
    io_manager_key="elasticsearch_io_manager",
    kinds={"polars", "elasticsearch"},
)
def sales_searchable(
    context: dg.AssetExecutionContext,
    sales_transform: pl.DataFrame,
) -> pl.DataFrame:
    """Sales data indexed for full-text search."""
    return sales_transform.select([
        "sale_id",
        "artwork_title",
        "artist_name",
        "sale_price_usd",
        "sale_date",
    ])
```

### Example 2: Time-Series Data

```python
@dg.asset(io_manager_key="elasticsearch_io_manager")
def daily_sales_metrics(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    """Daily sales metrics for time-series analysis in Kibana."""
    return pl.DataFrame({
        "date": ["2024-01-01", "2024-01-02"],
        "total_sales": [150_000_000, 200_000_000],
        "avg_sale_price": [5_000_000, 6_000_000],
        "sale_count": [30, 33],
    })
```

### Example 3: Read from Elasticsearch

```python
@dg.asset
def analyze_past_sales(
    context: dg.AssetExecutionContext,
    sales_searchable: pl.DataFrame,  # ← Auto-loaded from Elasticsearch
) -> pl.DataFrame:
    """Analyze sales data from Elasticsearch."""
    # sales_searchable is automatically loaded from ES index
    return sales_searchable.group_by("artist_name").agg([
        pl.col("sale_price_usd").sum().alias("total_revenue"),
    ])
```

---

## Querying Data

### Via Python

```python
from elasticsearch import Elasticsearch

es = Elasticsearch(["http://localhost:9200"])

# Search for artworks by Picasso
response = es.search(
    index="honey_duck_sales_output",
    query={
        "match": {
            "artist_name": "Picasso"
        }
    },
    size=10,
)

for hit in response["hits"]["hits"]:
    print(hit["_source"])
```

### Via Kibana Dev Tools

```json
GET /honey_duck_sales_output/_search
{
  "query": {
    "range": {
      "sale_price_usd": {
        "gte": 50000000
      }
    }
  },
  "sort": [
    { "sale_price_usd": "desc" }
  ]
}
```

### Via cURL

```bash
curl -X GET "http://localhost:9200/honey_duck_sales_output/_search" \
  -H 'Content-Type: application/json' \
  -d '{
  "query": { "match_all": {} },
  "size": 10
}'
```

---

## Performance Optimization

### 1. Bulk Size Tuning

```python
# Small documents (< 1KB each)
ElasticsearchIOManager(bulk_size=5000)

# Medium documents (1-10KB each)
ElasticsearchIOManager(bulk_size=1000)  # Default

# Large documents (> 10KB each)
ElasticsearchIOManager(bulk_size=100)
```

### 2. Index Settings

```python
sales_mapping = {
    "settings": {
        "number_of_shards": 1,      # For small indices
        "number_of_replicas": 0,    # For development
        "refresh_interval": "30s",  # Batch indexing
    },
    "mappings": {
        "properties": {
            # ... field definitions
        }
    }
}
```

### 3. Disable Source Storage (for analytics only)

```python
# If you don't need to retrieve full documents
sales_mapping = {
    "mappings": {
        "_source": {"enabled": False},  # Save storage space
        "properties": {
            # ... field definitions with store=true for needed fields
        }
    }
}
```

### 4. Use Doc Values for Aggregations

```python
# Efficient for aggregations/sorting
{
    "sale_price_usd": {
        "type": "float",
        "doc_values": True  # Default, enables fast aggregations
    }
}
```

---

## Kibana Dashboards

### Create Visualizations

1. Open Kibana: http://localhost:5601
2. Go to **Stack Management** → **Index Patterns**
3. Create index pattern: `honey_duck_*`
4. Go to **Visualize** → **Create visualization**

### Example: Sales Over Time

```
Visualization Type: Line Chart
Index Pattern: honey_duck_sales_output
Metrics:
  - Y-axis: Sum of sale_price_usd
Buckets:
  - X-axis: Date Histogram on sale_date (Monthly)
```

### Example: Top Artists by Revenue

```
Visualization Type: Pie Chart
Index Pattern: honey_duck_sales_output
Metrics:
  - Slice Size: Sum of sale_price_usd
Buckets:
  - Split Slices: Terms on artist_name.keyword (Top 10)
```

---

## Environment Variables

```bash
# .env
ELASTICSEARCH_HOST=http://localhost:9200
ELASTICSEARCH_API_KEY=your-api-key-here

# For production
ELASTICSEARCH_HOST=https://my-cluster.es.cloud:9243
ELASTICSEARCH_API_KEY=production-api-key
ELASTICSEARCH_INDEX_PREFIX=prod_honey_duck_
```

```python
# Use in definitions.py
import os

ElasticsearchIOManager(
    hosts=[os.getenv("ELASTICSEARCH_HOST")],
    api_key=os.getenv("ELASTICSEARCH_API_KEY"),
    index_prefix=os.getenv("ELASTICSEARCH_INDEX_PREFIX", "dagster_"),
)
```

---

## Troubleshooting

### Error: "elasticsearch package not found"

```bash
pip install elasticsearch>=8.0.0
```

### Error: "ConnectionError: Connection refused"

Elasticsearch isn't running. Start it:
```bash
docker-compose -f docker-compose.elasticsearch.yml up -d
```

### Error: "Failed to index documents"

Check Elasticsearch logs:
```bash
docker-compose -f docker-compose.elasticsearch.yml logs elasticsearch
```

### Performance: Slow indexing

1. Increase `bulk_size`:
```python
ElasticsearchIOManager(bulk_size=2000)
```

2. Disable replicas during bulk indexing:
```python
# Set in custom mappings
"settings": {"number_of_replicas": 0}
```

3. Increase refresh interval:
```python
"settings": {"refresh_interval": "60s"}
```

### Index exists with wrong mapping

Delete and recreate:
```bash
# Delete index
curl -X DELETE "http://localhost:9200/honey_duck_sales_output"

# Re-materialize asset
uv run dg launch --assets sales_output
```

---

## Migration from JSON to Elasticsearch

```python
# Before: JSON output
@dg.asset
def sales_output(context, sales_transform: pl.DataFrame) -> pl.DataFrame:
    result = sales_transform.filter(pl.col("price") > 1000000)
    write_json_and_return(result, "data/output/sales.json", context)
    return result

# After: Elasticsearch output
@dg.asset(io_manager_key="elasticsearch_io_manager")
def sales_output(context, sales_transform: pl.DataFrame) -> pl.DataFrame:
    # Same transformation, different storage!
    return sales_transform.filter(pl.col("price") > 1000000)
```

---

## Production Checklist

- [ ] Use API key authentication (not basic auth)
- [ ] Enable SSL/TLS (`verify_certs=True`)
- [ ] Set `number_of_replicas >= 1` for high availability
- [ ] Define custom mappings for all production indices
- [ ] Set up index lifecycle management (ILM) for old data
- [ ] Monitor cluster health via Kibana
- [ ] Set up alerts for indexing failures
- [ ] Test backup/restore procedures
- [ ] Document index naming conventions
- [ ] Set up Kibana role-based access control

---

## Next Steps

1. **Explore Kibana**: Build dashboards on your pipeline outputs
2. **Add Alerting**: Use Elasticsearch Watcher for data-driven alerts
3. **Scale Up**: Add more nodes for production workloads
4. **Advanced Queries**: Learn Elasticsearch DSL for complex searches
5. **Machine Learning**: Use Elastic ML for anomaly detection

---

## Resources

- **Elasticsearch Python Docs**: https://elasticsearch-py.readthedocs.io/
- **Elasticsearch Reference**: https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html
- **Kibana Guide**: https://www.elastic.co/guide/en/kibana/current/index.html
- **Mapping Reference**: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html
- **Bulk API**: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html

---

**Happy indexing!** 
