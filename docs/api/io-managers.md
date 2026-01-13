# IO Managers

Custom IO managers for various storage backends.

!!! info "External Documentation"
    - **Dagster IO Managers**: [docs.dagster.io/concepts/io-management](https://docs.dagster.io/concepts/io-management/io-managers)
    - **Elasticsearch**: [elastic.co/guide](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html) | [Python Client](https://elasticsearch-py.readthedocs.io/)
    - **OpenSearch**: [opensearch.org/docs](https://opensearch.org/docs/latest/) | [Python Client](https://opensearch.org/docs/latest/clients/python-low-level/)

## Elasticsearch

### ElasticsearchIOManager

::: cogapp_deps.dagster.io_managers.ElasticsearchIOManager

**Example:**

```python
from cogapp_deps.dagster import ElasticsearchIOManager

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

::: cogapp_deps.dagster.io_managers.OpenSearchIOManager

**Example (AWS):**

```python
from cogapp_deps.dagster import OpenSearchIOManager

opensearch_io_manager = OpenSearchIOManager(
    hosts=["https://search-domain.us-east-1.es.amazonaws.com"],
    aws_auth=True,
    region="us-east-1",
)
```

---

## JSON

### JSONIOManager

::: cogapp_deps.dagster.io_managers.JSONIOManager

**Example:**

```python
from cogapp_deps.dagster import JSONIOManager

defs = dg.Definitions(
    resources={
        "json_io_manager": JSONIOManager(base_dir="data/output/json"),
    },
)

@dg.asset(io_manager_key="json_io_manager")
def my_asset() -> pl.DataFrame:
    return pl.DataFrame({"id": [1, 2, 3]})
```
