# API Bulk Harvesting with DLT

Guide to harvesting data from AI APIs in bulk using DLT (Data Load Tool) resources for Claude and Voyage AI.

## Overview

API bulk harvesters enable efficient, cost-effective processing of large datasets through AI APIs:

- **Claude Message Batches**: Process thousands of prompts with 50% cost reduction
- **Voyage AI Embeddings**: Generate embeddings for large text collections
- **Automatic batching**: Handles API rate limits and quotas
- **Progress tracking**: Monitor long-running batch jobs
- **Error handling**: Automatic retries and graceful degradation

---

## Installation

Both harvesters require optional dependencies:

```bash
# Claude API (Anthropic)
pip install anthropic

# Voyage AI
pip install voyageai

# DLT (if not already installed)
pip install dlt>=1.20.0
```

Set environment variables:

```bash
# Claude API
export ANTHROPIC_API_KEY="sk-ant-..."

# Voyage AI
export VOYAGE_API_KEY="pa-..."
```

---

## Claude Message Batches

Process large volumes of prompts using Claude's [Message Batches API](https://docs.anthropic.com/en/docs/build-with-claude/message-batches) for 50% cost savings.

### Quick Start

```python
import dlt
from cogapp_libs.dagster import claude_message_batches

# Define prompts to process
prompts = [
    {"text": "Summarize: The quick brown fox...", "custom_id": "doc_1"},
    {"text": "Translate to French: Hello world", "custom_id": "doc_2"},
    # ... thousands more
]

@dlt.source(name="claude_analysis")
def analyze_documents():
    """Process documents with Claude."""
    return claude_message_batches(
        prompts=prompts,
        model="claude-3-5-sonnet-20241022",
        max_tokens=1024,
    )

# Run pipeline
pipeline = dlt.pipeline(
    pipeline_name="claude_bulk",
    destination="duckdb",
    dataset_name="ai_analysis",
)

load_info = pipeline.run(analyze_documents())
print(f"Processed {load_info.load_packages[0].state['row_counts']} prompts")
```

### Dagster Integration

```python
import dagster as dg
from dagster_dlt import DagsterDltResource, dlt_assets
from cogapp_libs.dagster import claude_message_batches, create_duckdb_pipeline

@dlt.source(name="customer_support")
def support_tickets_source(context: dg.AssetExecutionContext):
    """Analyze customer support tickets with Claude."""

    # Load tickets from upstream asset
    # In practice, this would come from your data warehouse
    tickets = [
        {
            "text": f"Classify sentiment and extract key issues: {ticket}",
            "custom_id": f"ticket_{i}",
        }
        for i, ticket in enumerate(load_tickets())
    ]

    return claude_message_batches(
        prompts=tickets,
        model="claude-3-5-haiku-20241022",  # Fast, cheap for classification
        max_tokens=200,
        system_prompt="You are a customer support analyst. Classify sentiment and extract key issues.",
    )

@dlt_assets(
    dlt_source=support_tickets_source(),
    dlt_pipeline=create_duckdb_pipeline(
        pipeline_name="support_analysis",
        dataset_name="customer_insights",
    ),
    name="claude_ticket_analysis",
    group_name="ai_analysis",
)
def claude_support_analysis(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    """Analyze support tickets with Claude Message Batches."""
    yield from dlt.run(context=context, write_disposition="append")
```

### Schema

Messages are stored with this schema:

| Column | Type | Description |
|--------|------|-------------|
| `batch_id` | text | Claude batch ID |
| `custom_id` | text | Your custom identifier |
| `prompt` | text | Original prompt text |
| `response` | text | Claude's response |
| `model` | text | Model used (e.g., claude-3-5-sonnet-20241022) |
| `input_tokens` | bigint | Tokens in prompt |
| `output_tokens` | bigint | Tokens in response |
| `created_at` | timestamp | Response timestamp |
| `processing_time_ms` | double | Processing time (null for batches) |
| `success` | bool | Whether request succeeded |
| `error` | text | Error message if failed |

### Use Cases

**1. Document Analysis**
```python
# Analyze 10,000 research papers
papers = load_papers()  # Your data source

prompts = [
    {
        "text": f"Extract: title, authors, key findings, methodology from:\n\n{paper['text']}",
        "custom_id": paper['doi'],
        "max_tokens": 500,
    }
    for paper in papers
]

yield from claude_message_batches(
    prompts=prompts,
    model="claude-3-5-sonnet-20241022",
    system_prompt="You are a research paper analyst. Extract structured information in JSON format.",
)
```

**2. Translation Pipeline**
```python
# Translate 50,000 product descriptions
products = load_products()

prompts = [
    {
        "text": f"Translate to Spanish: {product['description']}",
        "custom_id": product['sku'],
    }
    for product in products
]

yield from claude_message_batches(
    prompts=prompts,
    model="claude-3-5-haiku-20241022",  # Haiku is fast and cheap for translation
    max_tokens=300,
)
```

**3. Content Moderation**
```python
# Moderate user-generated content
ugc = load_user_content()

prompts = [
    {
        "text": f"Classify: {content['text']}\n\nCategories: safe, needs_review, unsafe",
        "custom_id": content['content_id'],
        "max_tokens": 50,
    }
    for content in ugc
]

yield from claude_message_batches(
    prompts=prompts,
    model="claude-3-5-haiku-20241022",  # Fast classification
    system_prompt="You are a content moderator. Classify content as: safe, needs_review, or unsafe.",
)
```

### Models and Pricing

Claude Message Batches offer **50% discount** on batch processing:

| Model | Use Case | Regular Price | Batch Price |
|-------|----------|---------------|-------------|
| claude-3-5-sonnet-20241022 | Complex analysis, generation | $3/$15 per MTok | $1.50/$7.50 |
| claude-3-5-haiku-20241022 | Classification, translation | $0.80/$4 per MTok | $0.40/$2 |
| claude-3-opus-20240229 | Highest quality | $15/$75 per MTok | $7.50/$37.50 |

*Prices: input/output per million tokens*

### Limitations

- **Processing time**: Minutes to hours depending on queue
- **Polling interval**: Checks status every 60 seconds
- **Max batch size**: 10,000 requests per batch (create multiple batches if needed)
- **Expiration**: Results expire after 24 hours

---

## Voyage AI Embeddings

Generate embeddings for large text collections using [Voyage AI](https://www.voyageai.com/) with efficient batching.

### Quick Start

```python
import dlt
from cogapp_libs.dagster import voyage_embeddings_batch

# Texts to embed
texts = [
    "The quick brown fox jumps over the lazy dog",
    "Machine learning is transforming industries",
    "Python is a versatile programming language",
    # ... thousands more
]

@dlt.source(name="text_embeddings")
def embed_documents():
    """Generate embeddings for documents."""
    return voyage_embeddings_batch(
        texts=texts,
        model="voyage-3",  # Latest general-purpose model
        input_type="document",  # "document" or "query"
    )

# Run pipeline
pipeline = dlt.pipeline(
    pipeline_name="embeddings",
    destination="duckdb",
    dataset_name="vectors",
)

load_info = pipeline.run(embed_documents())
print(f"Generated {load_info.load_packages[0].state['row_counts']} embeddings")
```

### Dagster Integration

```python
import dagster as dg
from dagster_dlt import DagsterDltResource, dlt_assets
from cogapp_libs.dagster import voyage_embeddings_batch, create_duckdb_pipeline

@dlt.source(name="knowledge_base")
def knowledge_base_embeddings():
    """Generate embeddings for knowledge base articles."""

    # Load articles (in practice, from upstream asset or database)
    articles = load_articles()

    texts = [
        {
            "text": article['title'] + "\n\n" + article['content'],
            "id": article['article_id'],
        }
        for article in articles
    ]

    return voyage_embeddings_batch(
        texts=texts,
        model="voyage-3",
        input_type="document",
        batch_size=128,
    )

@dlt_assets(
    dlt_source=knowledge_base_embeddings(),
    dlt_pipeline=create_duckdb_pipeline(
        pipeline_name="kb_embeddings",
        dataset_name="vectors",
    ),
    name="kb_embeddings",
    group_name="vector_generation",
)
def kb_embedding_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    """Generate knowledge base embeddings with Voyage AI."""
    yield from dlt.run(context=context, write_disposition="replace")
```

### Schema

Embeddings are stored with this schema:

| Column | Type | Description |
|--------|------|-------------|
| `embedding_id` | text | Custom identifier |
| `text` | text | Original text |
| `embedding` | complex (JSON) | Embedding vector as JSON array |
| `model` | text | Model used (e.g., voyage-3) |
| `dimensions` | bigint | Embedding dimensions (e.g., 1024) |
| `total_tokens` | bigint | Approximate tokens used |
| `created_at` | timestamp | Generation timestamp |
| `processing_time_ms` | double | Processing time per text |

### Querying Embeddings

After loading embeddings, query them with DuckDB:

```python
import duckdb
import json

conn = duckdb.connect("embeddings.duckdb")

# Get embedding for query
query_embedding = [0.1, 0.2, ...]  # From Voyage AI

# Find similar documents using cosine similarity
results = conn.execute("""
    SELECT
        embedding_id,
        text,
        -- Cosine similarity
        (
            SELECT SUM(a * b) / (SQRT(SUM(a * a)) * SQRT(SUM(b * b)))
            FROM (
                SELECT
                    UNNEST(CAST(embedding AS DOUBLE[])) AS a,
                    UNNEST(?) AS b
            )
        ) AS similarity
    FROM voyage_embeddings
    ORDER BY similarity DESC
    LIMIT 10
""", [query_embedding]).fetchall()
```

### Models

Voyage AI offers specialized models:

| Model | Dimensions | Use Case | Price |
|-------|-----------|----------|-------|
| voyage-3 | 1024 | General-purpose, latest | $0.06/1M tokens |
| voyage-3-lite | 512 | Faster, cheaper | $0.02/1M tokens |
| voyage-code-3 | 1024 | Code search, documentation | $0.06/1M tokens |
| voyage-finance-2 | 1024 | Financial documents | $0.06/1M tokens |
| voyage-law-2 | 1024 | Legal documents | $0.06/1M tokens |

### Use Cases

**1. Semantic Search**
```python
# Index product catalog
products = load_products()

texts = [
    {
        "text": f"{product['name']}\n{product['description']}\n{product['specs']}",
        "id": product['product_id'],
    }
    for product in products
]

yield from voyage_embeddings_batch(
    texts=texts,
    model="voyage-3",
    input_type="document",
)

# Later, embed search queries with input_type="query"
```

**2. Code Search**
```python
# Index codebase functions
functions = extract_functions_from_codebase()

texts = [
    {
        "text": f"{func['signature']}\n\n{func['docstring']}\n\n{func['code']}",
        "id": func['function_id'],
    }
    for func in functions
]

yield from voyage_embeddings_batch(
    texts=texts,
    model="voyage-code-3",  # Optimized for code
    input_type="document",
)
```

**3. Document Clustering**
```python
# Cluster research papers by topic
papers = load_papers()

texts = [
    {
        "text": f"{paper['title']}\n\n{paper['abstract']}",
        "id": paper['paper_id'],
    }
    for paper in papers
]

yield from voyage_embeddings_batch(
    texts=texts,
    model="voyage-3",
    input_type="document",
)

# Use K-means or HDBSCAN on embeddings for clustering
```

### Limitations

- **Batch size**: Max 128 texts per API call (automatically handled)
- **Token limits**: Text exceeding limits will be truncated (set `truncation=True`)
- **Rate limits**: Exponential backoff on errors (automatic)

---

## Integration Patterns

### Pattern 1: Parallel Processing

Process multiple batches in parallel using Dagster's asset groups:

```python
@dlt.source
def batch_1():
    return claude_message_batches(prompts=batch_1_prompts, ...)

@dlt.source
def batch_2():
    return claude_message_batches(prompts=batch_2_prompts, ...)

# Both materialize in parallel
@dlt_assets(dlt_source=batch_1(), name="batch_1", ...)
def batch_1_asset(context, dlt):
    yield from dlt.run(context=context)

@dlt_assets(dlt_source=batch_2(), name="batch_2", ...)
def batch_2_asset(context, dlt):
    yield from dlt.run(context=context)
```

### Pattern 2: Chained Processing

Use Claude outputs as inputs for Voyage embeddings:

```python
@dlt_assets(name="summaries")
def generate_summaries(...):
    """Generate summaries with Claude."""
    prompts = [{"text": f"Summarize: {doc}", ...} for doc in docs]
    yield from dlt.run(claude_message_batches(prompts=prompts, ...))

@dlt_assets(name="summary_embeddings", deps=["summaries"])
def embed_summaries(context, summaries: pl.DataFrame):
    """Embed summaries with Voyage AI."""
    texts = summaries["response"].to_list()
    yield from dlt.run(voyage_embeddings_batch(texts=texts, ...))
```

### Pattern 3: RAG Pipeline

Complete RAG (Retrieval-Augmented Generation) workflow:

```python
# Step 1: Embed knowledge base
@dlt_assets(name="kb_embeddings")
def embed_knowledge_base(...):
    docs = load_docs()
    yield from dlt.run(voyage_embeddings_batch(texts=docs, ...))

# Step 2: Retrieve relevant docs (custom logic)
def retrieve_context(query: str, top_k: int = 5) -> list[str]:
    query_embedding = voyage_client.embed([query])[0]
    # DuckDB similarity search
    return top_k_docs

# Step 3: Generate answers with context
@dlt_assets(name="rag_answers")
def generate_answers(queries: list[str]):
    prompts = [
        {
            "text": f"Context:\n{retrieve_context(q)}\n\nQuestion: {q}",
            "custom_id": f"query_{i}",
        }
        for i, q in enumerate(queries)
    ]
    yield from dlt.run(claude_message_batches(prompts=prompts, ...))
```

---

## Best Practices

### Cost Optimization

1. **Choose the right model**:
   - Claude Haiku for simple tasks (classification, extraction)
   - Claude Sonnet for complex analysis
   - Voyage 3-lite for fast, cheap embeddings

2. **Batch size tuning**:
   - Claude: Max 10,000 per batch
   - Voyage: 128 per request (handled automatically)

3. **Token limits**:
   - Set conservative `max_tokens` to avoid waste
   - Use prompt caching for repeated context (not in batches)

### Error Handling

```python
@dlt_assets(...)
def resilient_processing(context, dlt):
    """Process with automatic error handling."""
    try:
        yield from dlt.run(
            context=context,
            write_disposition="append",  # Don't overwrite on failure
        )
    except Exception as e:
        context.log.error(f"Batch processing failed: {e}")
        # Alert or retry logic
        raise
```

### Monitoring

```python
@dlt_assets(...)
def monitored_batch(context, dlt):
    """Monitor batch processing metrics."""
    start = time.time()

    result = yield from dlt.run(context=context)

    elapsed = time.time() - start
    context.add_output_metadata({
        "processing_time_seconds": elapsed,
        "records_processed": result.row_counts.get("claude_messages", 0),
        "cost_estimate_usd": calculate_cost(...),
    })
```

---

## Troubleshooting

### Issue: "API key not found"

**Solution**: Set environment variable:
```bash
export ANTHROPIC_API_KEY="sk-ant-..."
export VOYAGE_API_KEY="pa-..."
```

Or pass directly:
```python
claude_message_batches(prompts=prompts, api_key="sk-ant-...")
voyage_embeddings_batch(texts=texts, api_key="pa-...")
```

### Issue: "Rate limit exceeded"

**Solution**: Both harvesters have automatic retry logic. If persistent:
- Reduce batch size
- Add delays between batches
- Upgrade API tier

### Issue: "Batch taking too long"

**Solution**: Claude batches can take hours. Monitor progress:
```python
# Check batch status manually
from anthropic import Anthropic

client = Anthropic()
batch = client.messages.batches.retrieve("batch_id")
print(f"Status: {batch.processing_status}")
print(f"Progress: {batch.request_counts.processing}/{batch.request_counts.total}")
```

### Issue: "Embeddings too large for storage"

**Solution**: DuckDB stores embeddings as JSON. For large-scale:
- Use specialized vector databases (Pinecone, Qdrant, Weaviate)
- Compress embeddings (PCA, quantization)
- Store only IDs and fetch embeddings on-demand

---

## Resources

- **Claude Message Batches API**: https://docs.anthropic.com/en/docs/build-with-claude/message-batches
- **Voyage AI Documentation**: https://docs.voyageai.com/
- **DLT Documentation**: https://dlthub.com/docs/
- **Dagster-DLT Integration**: https://docs.dagster.io/integrations/dlt

---

**Ready to harvest APIs at scale!** 
