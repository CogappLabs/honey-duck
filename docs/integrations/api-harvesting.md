---
title: API Harvesting
description: Harvest data from REST APIs using dlt with pagination, rate limiting, and incremental loading into Parquet files.
---

# API Bulk Harvesting with DLT

Guide to harvesting data from AI APIs in bulk using DLT (Data Load Tool) resources.

## Overview

API bulk harvesters enable efficient, cost-effective processing of large datasets through AI APIs:

- **Voyage AI Embeddings**: Generate embeddings for large text collections
- **Automatic batching**: Handles API rate limits and quotas
- **Progress tracking**: Monitor long-running batch jobs
- **Error handling**: Automatic retries and graceful degradation

---

## Installation

Voyage harvesting requires optional dependencies:

```bash
pip install voyageai

# DLT (if not already installed)
pip install dlt>=1.20.0
```

Set environment variables:

```bash
export VOYAGE_API_KEY="pa-..."
```

---

## Voyage AI Embeddings

Generate embeddings for large volumes of text using Voyage AI.

### Quick Start

```python
import dlt
from cogapp_libs.dagster import voyage_embeddings_batch

texts = ["Summarize: The quick brown fox...", "Translate to French: Hello world"]

@dlt.source(name="embeddings")
def build_embeddings():
    return voyage_embeddings_batch(texts=texts, model="voyage-3")

pipeline = dlt.pipeline(
    pipeline_name="voyage_embeddings",
    destination="duckdb",
    dataset_name="ai_embeddings",
)

load_info = pipeline.run(build_embeddings())
print(f"Processed {load_info.load_packages[0].state['row_counts']} texts")
```

### Dagster Integration

```python
import dagster as dg
from dagster_dlt import DagsterDltResource, dlt_assets
from cogapp_libs.dagster import voyage_embeddings_batch, create_duckdb_pipeline

@dlt.source(name="customer_embeddings")
def support_tickets_source(context: dg.AssetExecutionContext):
    tickets = ["Refund request", "Password reset issue", "Billing question"]
    return voyage_embeddings_batch(
        texts=tickets,
        model="voyage-3-lite",
        input_type="document",
    )

@dlt_assets(
    dlt_source=support_tickets_source(),
    dlt_pipeline=create_duckdb_pipeline(
        pipeline_name="support_embeddings",
        dataset_name="customer_insights",
    ),
    name="voyage_embeddings",
    group_name="ai_analysis",
)
def voyage_support_embeddings(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context, write_disposition="append")
```

### Schema

Embeddings are stored with this schema:

| Column | Type | Description |
|--------|------|-------------|
| `embedding_id` | text | Generated ID for the embedding |
| `text` | text | Original text input |
| `embedding` | complex | JSON array of floats |
| `model` | text | Model used (e.g., voyage-3) |
| `dimensions` | bigint | Vector length |
| `total_tokens` | bigint | Tokens used for the request |
| `created_at` | timestamp | Processing timestamp |
| `processing_time_ms` | double | Processing time per text |

### Use Cases

**1. Semantic Search**
```python
texts = load_documents()
yield from voyage_embeddings_batch(texts=texts, model="voyage-3")
```

**2. Classification / Clustering**
```python
texts = load_support_tickets()
yield from voyage_embeddings_batch(texts=texts, model="voyage-3-lite")
```

**3. Code Search**
```python
snippets = load_code_snippets()
yield from voyage_embeddings_batch(texts=snippets, model="voyage-code-3")
```

### Models

Common Voyage models:

- `voyage-3`: Latest general-purpose (1024 dims)
- `voyage-3-lite`: Faster, cheaper (512 dims)
- `voyage-code-3`: Optimized for code (1024 dims)
- `voyage-finance-2`: Finance domain (1024 dims)
- `voyage-law-2`: Legal domain (1024 dims)

### Limitations

- **Batch size**: Controlled by `batch_size` (default 128)
- **Rate limits**: Small delays are added between batches

---

## Troubleshooting

- **Missing API key**: set `VOYAGE_API_KEY` or pass `api_key` explicitly
- **Missing dependency**: `pip install voyageai`
