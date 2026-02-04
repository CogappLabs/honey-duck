# Cogapp Deps API Reference

Complete reference for the `cogapp_libs` reusable utilities library.

## Table of Contents

- [Data Loading](#data-loading)
- [Validation](#validation)
- [Error Handling](#error-handling)
- [Metadata](#metadata)
- [Visualization](#visualization)
- [IO Managers](#io-managers)
- [DLT Helpers](#dlt-helpers)
- [API Sources](#api-sources)
- [Notifications](#notifications)
- [Sitemaps](#sitemaps)

---

## Data Loading

### `read_harvest_table_lazy()`

Read a single Parquet table as a Polars LazyFrame with validation.

```python
from cogapp_libs.dagster import read_harvest_table_lazy

df = read_harvest_table_lazy(
    base_dir="/path/to/parquet",
    table_name="sales_raw",
    columns=["sale_id", "sale_price_usd"],  # Optional: column pruning
    asset_name="my_asset",  # For error messages
)
```

**Parameters**:
- `base_dir` (Path | str): Directory containing Parquet files
- `table_name` (str): Name of table (file without .parquet)
- `columns` (list[str] | None): Optional column list for pruning
- `asset_name` (str): Asset name for error messages

**Returns**: `pl.LazyFrame`

**Raises**:
- `MissingTableError`: If table file doesn't exist
- `MissingColumnError`: If requested columns don't exist

---

### `read_harvest_tables_lazy()`

Batch read multiple Parquet tables with validation.

```python
from cogapp_libs.dagster import read_harvest_tables_lazy

tables = read_harvest_tables_lazy(
    "/path/to/parquet",
    ("sales_raw", ["sale_id", "sale_price_usd"]),
    ("artworks_raw", None),  # All columns
    ("artists_raw", ["artist_id", "name"]),
    asset_name="my_asset",
)

sales = tables["sales_raw"]
artworks = tables["artworks_raw"]
artists = tables["artists_raw"]
```

**Parameters**:
- `base_dir` (Path | str): Directory containing Parquet files
- `*table_specs` (tuple): Variable args of (table_name, columns_or_None)
- `asset_name` (str): Asset name for error messages

**Returns**: `dict[str, pl.LazyFrame]`

**Raises**: Same as `read_harvest_table_lazy()`

**Benefits**:
- Single validation pass
- Cleaner code
- Better error messages

---

### `read_parquet_table_lazy()`

Read any Parquet file with validation (not limited to harvest dir).

```python
from cogapp_libs.dagster import read_parquet_table_lazy

df = read_parquet_table_lazy(
    file_path="/custom/path/data.parquet",
    columns=["id", "value"],
    asset_name="my_asset",
)
```

---

## Validation

### `validate_dataframe()`

Validate DataFrame schema and contents.

```python
from cogapp_libs.dagster import validate_dataframe, DataValidationError

try:
    validate_dataframe(
        df,
        required_columns=["id", "name", "value"],
        asset_name="my_asset",
    )
except DataValidationError as e:
    context.log.error(str(e))
    raise
```

**Parameters**:
- `df` (pl.DataFrame | pd.DataFrame): DataFrame to validate
- `required_columns` (list[str]): Columns that must exist
- `asset_name` (str): Asset name for error messages

**Raises**: `MissingColumnError` if columns missing

---

## Error Handling

### Exception Classes

**Base Exception**:
```python
from cogapp_libs.dagster import PipelineError

# Base class for all pipeline errors
raise PipelineError("Something went wrong")
```

**Specific Exceptions**:

| Exception | When to Use | Example |
|-----------|-------------|---------|
| `ConfigurationError` | Configuration issues | Invalid paths, bad settings |
| `DataValidationError` | Data quality problems | Missing columns, wrong types |
| `MissingTableError` | Table not found | File doesn't exist |
| `MissingColumnError` | Column not found | Column doesn't exist in table |

**Example Usage**:
```python
from cogapp_libs.dagster import MissingTableError, MissingColumnError

# Table error
raise MissingTableError(
    table_name="sales_raw",
    available_tables=["artworks_raw", "artists_raw"],
    asset_name="my_asset",
)

# Column error
raise MissingColumnError(
    column_name="sale_price_usd",
    available_columns=["sale_id", "sale_date", "amount"],
    table_name="sales_raw",
    asset_name="my_asset",
)
```

---

### `raise_as_dagster_failure()`

Convert exceptions to Dagster failures (stops execution gracefully).

```python
from cogapp_libs.dagster import raise_as_dagster_failure

try:
    risky_operation()
except Exception as e:
    raise_as_dagster_failure(e, context, "Operation failed")
```

---

## Metadata

### `add_dataframe_metadata()`

Add standard DataFrame metadata to asset context.

```python
from cogapp_libs.dagster import add_dataframe_metadata

@dg.asset
def my_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    result = transform()

    # Automatic metadata: record count, preview, columns
    add_dataframe_metadata(context, result)

    # With custom metadata
    add_dataframe_metadata(
        context,
        result,
        unique_users=result["user_id"].n_unique(),
        total_revenue=float(result["revenue"].sum()),
    )

    return result
```

**Parameters**:
- `context` (AssetExecutionContext): Dagster context
- `df` (pl.DataFrame | pd.DataFrame): DataFrame to describe
- `**extra_metadata`: Additional key-value pairs

**Adds to Context**:
- `record_count`: Number of rows
- `columns`: List of column names
- `preview`: Markdown table of first 5 rows
- All `**extra_metadata` values

---

### `track_timing()`

Context manager for automatic timing and logging.

```python
from cogapp_libs.dagster import track_timing

@dg.asset
def my_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    with track_timing(context, "data loading"):
        data = load_data()  # Timed

    with track_timing(context, "transformation"):
        result = transform(data)  # Timed

    return result
```

**Parameters**:
- `context` (AssetExecutionContext): Dagster context
- `operation_name` (str): Description for logs

**Logs**: `"Completed {operation_name} in {time}ms"`

**Adds to Metadata**: `processing_time_ms` for the wrapped operation

---

## Visualization

### `altair_to_metadata()`

Convert an Altair chart to Dagster metadata with embedded PNG.

```python
from cogapp_libs.dagster import altair_to_metadata

@dg.asset
def my_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    result = transform()

    # Create chart using Polars built-in Altair integration
    chart = result.plot.bar(x="category", y="count", color="category")
    chart = chart.properties(title="Distribution", width=300, height=200)

    # Add to metadata as base64-encoded PNG
    context.add_output_metadata(altair_to_metadata(chart, "distribution_chart"))

    return result
```

**Parameters**:
- `chart` (alt.Chart): Altair Chart object (e.g., from `df.plot.bar()`)
- `title` (str): Metadata key name (default: "chart")

**Returns**: `dict[str, MetadataValue]` - Dictionary with embedded PNG in markdown

**Note**: Requires `altair` and `vl-convert-python` packages.

---

### `table_preview_to_metadata()`

Convert a Polars DataFrame to a markdown table for Dagster metadata.

```python
from cogapp_libs.dagster import table_preview_to_metadata

@dg.asset
def my_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    result = transform()

    # Add markdown table preview to metadata
    context.add_output_metadata(
        table_preview_to_metadata(
            result.head(5),
            title="top_results",
            header="Top 5 Results by Value",
        )
    )

    return result
```

**Parameters**:
- `df` (pl.DataFrame): Polars DataFrame to preview
- `title` (str): Metadata key name (default: "preview")
- `header` (str | None): Optional header text above the table
- `max_rows` (int): Maximum rows to include (default: 10)

**Returns**: `dict[str, MetadataValue]` - Dictionary with markdown table

---

### Example: Combined Visualization

```python
from cogapp_libs.dagster import altair_to_metadata, table_preview_to_metadata

@dg.asset
def sales_summary(context: dg.AssetExecutionContext, sales: pl.DataFrame) -> pl.DataFrame:
    # Aggregate by category
    summary = sales.group_by("category").agg(
        pl.len().alias("count"),
        pl.col("value").sum().alias("total_value"),
    ).sort("total_value", descending=True)

    # Bar chart of totals
    chart = summary.plot.bar(x="category", y="total_value")

    # Add both visualizations
    context.add_output_metadata({
        **altair_to_metadata(chart, "value_by_category"),
        **table_preview_to_metadata(summary, "summary_table", "Sales by Category"),
    })

    return summary
```

---

## IO Managers

### `JSONIOManager`

Write and read Polars/Pandas DataFrames as JSON files.

```python
from cogapp_libs.dagster import JSONIOManager

defs = dg.Definitions(
    resources={
        "json_io_manager": JSONIOManager(
            base_dir="data/output/json",
        ),
    },
)

@dg.asset(io_manager_key="json_io_manager")
def my_asset() -> pl.DataFrame:
    return pl.DataFrame({"id": [1, 2, 3]})
```

**Features**:
- Automatic JSON serialization
- Handles both Polars and Pandas
- Creates parent directories automatically

---

### `ElasticsearchIOManager`

Write and read DataFrames from Elasticsearch 8/9.

```python
from cogapp_libs.dagster import ElasticsearchIOManager

defs = dg.Definitions(
    resources={
        "elasticsearch_io_manager": ElasticsearchIOManager(
            hosts=["http://localhost:9200"],
            index_prefix="dagster_",
            api_key=os.getenv("ELASTICSEARCH_API_KEY"),
            bulk_size=1000,
        ),
    },
)

@dg.asset(io_manager_key="elasticsearch_io_manager")
def sales_searchable(sales_transform: pl.DataFrame) -> pl.DataFrame:
    return sales_transform  # Automatically indexed
```

**Parameters**:
- `hosts` (list[str]): Elasticsearch endpoints
- `index_prefix` (str): Prefix for index names
- `api_key` (str | None): API key for auth
- `basic_auth` (tuple[str, str] | None): (user, password)
- `bulk_size` (int): Batch size for bulk indexing
- `custom_mappings` (dict | None): Custom index mappings

**See**: [Elasticsearch Integration Guide](ELASTICSEARCH_INTEGRATION.md)

---

### `OpenSearchIOManager`

Write and read DataFrames from OpenSearch (AWS).

```python
from cogapp_libs.dagster import OpenSearchIOManager

# Self-hosted OpenSearch
opensearch_io_manager = OpenSearchIOManager(
    hosts=["https://localhost:9200"],
    http_auth=("admin", "admin"),
    verify_certs=False,
)

# AWS OpenSearch Service
opensearch_io_manager = OpenSearchIOManager(
    hosts=["https://search-domain.us-east-1.es.amazonaws.com"],
    aws_auth=True,
    region="us-east-1",
)
```

**Parameters**: Similar to `ElasticsearchIOManager` plus:
- `aws_auth` (bool): Use AWS SigV4 auth
- `region` (str): AWS region (required if aws_auth=True)

---

### `PolarsParquetIOManager`

Default IO manager for Parquet storage (built-in).

```python
# Already configured in project - no need to import
@dg.asset
def my_asset() -> pl.DataFrame:
    return pl.DataFrame({"id": [1, 2, 3]})
    # Automatically saved to data/output/storage/my_asset.parquet
```

---

## DLT Helpers

### `create_parquet_pipeline()`

Create a DLT pipeline that writes to Parquet files.

```python
from cogapp_libs.dagster import create_parquet_pipeline

pipeline = create_parquet_pipeline(
    pipeline_name="my_harvest",
    destination_dir="data/harvest",
    dataset_name="raw",
)
```

**Parameters**:
- `pipeline_name` (str): DLT pipeline name
- `destination_dir` (str | Path): Output directory
- `dataset_name` (str): Schema name (default: "raw")

**Returns**: `dlt.Pipeline`

---

### `create_duckdb_pipeline()`

Create a DLT pipeline that writes to DuckDB.

```python
from cogapp_libs.dagster import create_duckdb_pipeline

pipeline = create_duckdb_pipeline(
    pipeline_name="my_harvest",
    database_path="data/output/my_data.duckdb",
    dataset_name="raw",
)
```

---

## API Sources

### `voyage_embeddings_batch()`

DLT resource for bulk embedding generation with Voyage AI.

```python
from cogapp_libs.dagster import voyage_embeddings_batch
import dlt

@dlt.source
def embed_documents():
    texts = [
        "The quick brown fox...",
        "Machine learning is...",
    ]

    return voyage_embeddings_batch(
        texts=texts,
        model="voyage-3",
        input_type="document",
    )
```

**Parameters**:
- `texts` (list[str] | list[dict]): Texts to embed
- `model` (str): Voyage model name
- `input_type` (str): "document" or "query"
- `api_key` (str | None): API key (defaults to VOYAGE_API_KEY)
- `batch_size` (int): Batch size (max 128)

**Yields**: Dicts with keys:
- `embedding_id`, `text`, `embedding`
- `model`, `dimensions`, `total_tokens`
- `created_at`, `processing_time_ms`

---

## Notifications

### `create_slack_notification_asset()`

Factory function for Slack notification assets.

```python
from cogapp_libs.dagster import create_slack_notification_asset

slack_notify = create_slack_notification_asset(
    name="pipeline_slack_notification",
    webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
    channel="#data-alerts",
    message_template="Pipeline completed! {{asset_count}} assets materialized.",
    deps=["sales_output", "artworks_output"],
)
```

**Parameters**:
- `name` (str): Asset name
- `webhook_url` (str): Slack webhook URL
- `channel` (str): Slack channel
- `message_template` (str): Jinja2 template string
- `deps` (list[str]): Asset dependencies
- `group_name` (str): Dagster group

**Returns**: `dg.AssetsDefinition`

---

### `create_email_notification_asset()`

Factory function for email notification assets.

```python
from cogapp_libs.dagster import create_email_notification_asset

email_notify = create_email_notification_asset(
    name="pipeline_email_notification",
    smtp_config={
        "host": "smtp.gmail.com",
        "port": 587,
        "username": os.getenv("EMAIL_USER"),
        "password": os.getenv("EMAIL_PASSWORD"),
    },
    from_addr="pipeline@company.com",
    to_addrs=["team@company.com"],
    subject_template="Pipeline Report: {{status}}",
    body_template="path/to/template.html",
    deps=["sales_output"],
)
```

**See**: Notification Templates in `src/honey_duck/defs/`

---

## Sitemaps

### `create_sitemap_asset()`

Factory function for standard XML sitemap assets.

```python
from cogapp_libs.dagster import create_sitemap_asset

sitemap = create_sitemap_asset(
    name="sitemap_artworks",
    source_asset="artworks_catalog",
    url_column="slug",
    base_url="https://example.com/artworks",
    lastmod_column="updated_at",
    changefreq="monthly",
    priority="0.8",
)
```

**Parameters**:
- `name` (str): Asset name
- `source_asset` (str): Source DataFrame asset
- `url_column` (str): Column with URLs/paths
- `base_url` (str): Base URL to prepend
- `lastmod_column` (str | None): Last modified date column
- `changefreq` (str | None): Update frequency
- `priority` (float | str | None): URL priority (0.0-1.0)
- `output_path` (str | Path | None): Output file path

**Returns**: `dg.AssetsDefinition`

---

### `create_image_sitemap_asset()`

Factory function for image sitemap assets.

```python
from cogapp_libs.dagster import create_image_sitemap_asset

sitemap_images = create_image_sitemap_asset(
    name="sitemap_images",
    source_asset="artworks_with_images",
    page_url_column="artwork_url",
    image_url_column="image_url",
    base_url="https://example.com",
    title_column="artwork_title",
    caption_column="description",
)
```

**See**: [Sitemap Generation Guide](SITEMAP_GENERATION.md)

---

### `generate_sitemap_xml()`

Low-level function to generate sitemap XML.

```python
from cogapp_libs.dagster import generate_sitemap_xml

urls = [
    {
        "loc": "https://example.com/page1",
        "lastmod": "2024-01-15",
        "changefreq": "daily",
        "priority": "0.8",
    },
]

url_count = generate_sitemap_xml(
    urls=urls,
    output_path="sitemap.xml",
    pretty=True,
)
```

---

## Helper Functions

### `write_json_output()`

Write DataFrame to JSON with automatic metadata.

```python
from cogapp_libs.dagster import write_json_output

@dg.asset
def my_output(context, my_transform: pl.DataFrame) -> pl.DataFrame:
    result = my_transform.filter(pl.col("value") > 100)

    write_json_output(
        result,
        "data/output/my_output.json",
        context,
        extra_metadata={
            "filtered_from": len(my_transform),
            "threshold": 100,
        },
    )

    return result
```

**Parameters**:
- `df` (pl.DataFrame | pd.DataFrame): DataFrame to write
- `output_path` (str | Path): Output file path
- `context` (AssetExecutionContext): Dagster context
- `extra_metadata` (dict): Additional metadata

**Adds to Context**:
- `output_path`: File path
- `output_size_bytes`: File size
- `record_count`: Number of records
- All `extra_metadata` values

---

## Usage Patterns

### Pattern: Transform Asset with All Helpers

```python
import dagster as dg
import polars as pl
from cogapp_libs.dagster import (
    read_harvest_tables_lazy,
    track_timing,
    add_dataframe_metadata,
    write_json_output,
)
from honey_duck.defs.shared.helpers import STANDARD_HARVEST_DEPS
from honey_duck.defs.shared.resources import PathsResource

@dg.asset(
    kinds={"polars"},
    deps=STANDARD_HARVEST_DEPS,
    group_name="transform_polars",
)
def complete_example(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Complete example using all helper utilities."""

    # Timing wrapper
    with track_timing(context, "data loading and join"):
        # Batch read with validation
        tables = read_harvest_tables_lazy(
            HARVEST_PARQUET_DIR,
            ("sales_raw", ["sale_id", "sale_price_usd", "artwork_id"]),
            ("artworks_raw", ["artwork_id", "title"]),
            asset_name="complete_example",
        )

        # Transform
        result = (
            tables["sales_raw"]
            .join(tables["artworks_raw"], on="artwork_id")
            .filter(pl.col("sale_price_usd") > 10000)
            .collect()
        )

    # Automatic metadata
    add_dataframe_metadata(
        context,
        result,
        total_value=float(result["sale_price_usd"].sum()),
        unique_artworks=result["artwork_id"].n_unique(),
    )

    return result
```

---

## Environment Variables

### Required for API Sources

```bash
# Voyage AI
export VOYAGE_API_KEY="pa-..."

# Elasticsearch
export ELASTICSEARCH_HOST="http://localhost:9200"
export ELASTICSEARCH_API_KEY="..."

# OpenSearch
export OPENSEARCH_HOST="https://search-domain.us-east-1.es.amazonaws.com"

# Slack
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."

# Email
export SMTP_HOST="smtp.gmail.com"
export SMTP_USER="..."
export SMTP_PASSWORD="..."
```

---

## Testing Your Code

### Unit Test Example

```python
from cogapp_libs.dagster import read_harvest_table_lazy, MissingTableError
import pytest

def test_read_missing_table():
    """Test that missing tables raise proper errors."""
    with pytest.raises(MissingTableError):
        read_harvest_table_lazy(
            "/nonexistent/path",
            "missing_table",
            asset_name="test",
        )
```

---

## Resources

- **Source Code**: `cogapp_libs/dagster/` in repository
- **Examples**: `src/honey_duck/defs/` for usage patterns
- **Tests**: `tests/` for test examples
- **Dagster Docs**: https://docs.dagster.io/

---

**Questions? Check the [Troubleshooting Guide](TROUBLESHOOTING.md)** 🚀
