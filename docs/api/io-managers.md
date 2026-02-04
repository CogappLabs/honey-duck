# IO Managers

Custom IO managers for various storage backends.

!!! info "External Documentation"
    - **Dagster IO Managers**: [docs.dagster.io/concepts/io-management](https://docs.dagster.io/concepts/io-management/io-managers)
    - **Elasticsearch**: [elastic.co/guide](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html) | [Python Client](https://elasticsearch-py.readthedocs.io/)
    - **OpenSearch**: [opensearch.org/docs](https://opensearch.org/docs/latest/) | [Python Client](https://opensearch.org/docs/latest/clients/python-low-level/)

## Elasticsearch

### ElasticsearchIOManager

::: cogapp_libs.dagster.io_managers.ElasticsearchIOManager

**Example:**

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

---

## OpenSearch

### OpenSearchIOManager

::: cogapp_libs.dagster.io_managers.OpenSearchIOManager

**Example (AWS):**

```python
from cogapp_libs.dagster import OpenSearchIOManager

opensearch_io_manager = OpenSearchIOManager(
    hosts=["https://search-domain.us-east-1.es.amazonaws.com"],
    aws_auth=True,
    region="us-east-1",
)
```

---

## JSON

### JSONIOManager

::: cogapp_libs.dagster.io_managers.JSONIOManager

**Example:**

```python
from cogapp_libs.dagster import JSONIOManager

defs = dg.Definitions(
    resources={
        "json_io_manager": JSONIOManager(base_dir="data/output/json"),
    },
)

@dg.asset(io_manager_key="json_io_manager")
def my_asset() -> pl.DataFrame:
    return pl.DataFrame({"id": [1, 2, 3]})
```

---

## Parquet Path

### ParquetPathIOManager

::: cogapp_libs.dagster.io_managers.ParquetPathIOManager

For DuckDB-native pipelines where data stays in parquet format and never materializes as Python DataFrames. Assets write parquet directly and pass file paths to downstream assets.

**Example:**

```python
from cogapp_libs.dagster import ParquetPathIOManager
from dagster_duckdb import DuckDBResource

defs = dg.Definitions(
    resources={
        "parquet_path_io_manager": ParquetPathIOManager(
            base_dir="data/transforms"
        ),
        "duckdb": DuckDBResource(database=":memory:"),
    },
)

@dg.asset(io_manager_key="parquet_path_io_manager")
def sales_transform(duckdb: DuckDBResource, paths: PathsResource) -> str:
    """Write parquet directly, return path."""
    output_path = paths.transforms_dir / "sales.parquet"

    with duckdb.get_connection() as conn:
        conn.sql("SELECT * FROM ...").write_parquet(str(output_path))

    return str(output_path)  # Return path, not DataFrame


@dg.asset(io_manager_key="parquet_path_io_manager")
def sales_output(duckdb: DuckDBResource, sales_transform: str) -> str:
    """Receive path string, query parquet directly."""
    with duckdb.get_connection() as conn:
        # Use path in SQL query
        result = conn.sql(f"SELECT * FROM '{sales_transform}' WHERE ...")
        result.write_parquet("output.parquet")

    return "output.parquet"
```

**Benefits:**

- **Memory efficient**: Data stays in DuckDB/Parquet, never loads into Python
- **SQL-native**: Use `FROM 'path.parquet'` directly in queries
- **Streaming**: DuckDB processes in batches automatically
- **Soda compatible**: Validation checks receive paths to parquet files

See [Soda Validation](../integrations/soda-validation.md) for using this pattern with data quality checks.
