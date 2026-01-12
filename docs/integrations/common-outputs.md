# Common Pipeline Outputs - IO Manager Guide

Guide to common output destinations for data pipelines and how to implement them with Dagster IO Managers.

## Currently Implemented

**JSON** - `JSONIOManager` - File-based JSON output
**Parquet** - `PolarsParquetIOManager` - Columnar storage
**DuckDB** - `DuckDBPandasPolarsIOManager` - Analytical database
**Elasticsearch 8/9** - `ElasticsearchIOManager` - Full-text search
**OpenSearch** - `OpenSearchIOManager` - AWS fork of Elasticsearch

---

## Common Outputs to Consider

### 1. Relational Databases

**PostgreSQL** - Production-ready OLTP database
```python
from dagster_postgres import PostgresIOManager

defs = dg.Definitions(
    resources={
        "postgres_io_manager": PostgresIOManager(
            host="localhost",
            port=5432,
            database="mydb",
            user="postgres",
            password=os.getenv("POSTGRES_PASSWORD"),
        ),
    },
)

@dg.asset(io_manager_key="postgres_io_manager")
def sales_table(sales_transform: pl.DataFrame) -> pl.DataFrame:
    return sales_transform
```

**Use Cases:**
- Application databases (CRUD operations)
- ACID-compliant transactions
- Relational integrity constraints
- Traditional ETL destinations

**Trade-offs:**
- Mature, well-understood
- ACID guarantees
- Slower for analytics (vs columnar stores)
- Schema migrations can be complex

---

### 2. Data Warehouses

**BigQuery** - Google's serverless data warehouse
```python
from dagster_gcp import BigQueryIOManager

defs = dg.Definitions(
    resources={
        "bigquery_io_manager": BigQueryIOManager(
            project="my-project",
            dataset="honey_duck",
        ),
    },
)

@dg.asset(io_manager_key="bigquery_io_manager")
def sales_warehouse(sales_transform: pl.DataFrame) -> pl.DataFrame:
    return sales_transform
```

**Use Cases:**
- Large-scale analytics
- Data science workloads
- Business intelligence
- Machine learning feature stores

**Snowflake** - Cloud data warehouse
```python
from dagster_snowflake import SnowflakeIOManager

defs = dg.Definitions(
    resources={
        "snowflake_io_manager": SnowflakeIOManager(
            account="xy12345",
            user="dagster",
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            database="ANALYTICS",
            warehouse="COMPUTE_WH",
            schema="HONEY_DUCK",
        ),
    },
)
```

**Trade-offs:**
- Excellent for analytics queries
- Scalable to petabytes
- Separation of storage and compute
- Can be expensive
- Vendor lock-in

---

### 3. NoSQL Databases

**MongoDB** - Document database
```python
class MongoDBIOManager(dg.IOManager):
    def __init__(self, connection_string: str, database: str):
        self.connection_string = connection_string
        self.database = database

    def handle_output(self, context, obj: pl.DataFrame):
        from pymongo import MongoClient

        client = MongoClient(self.connection_string)
        db = client[self.database]
        collection_name = context.asset_key.path[-1]

        # Convert DataFrame to documents
        documents = obj.to_dicts()

        # Replace collection (for idempotency)
        db[collection_name].delete_many({})
        db[collection_name].insert_many(documents)

        context.add_output_metadata({
            "collection": collection_name,
            "document_count": len(documents),
        })
```

**Use Cases:**
- Flexible schemas
- JSON-like documents
- Rapid prototyping
- Content management systems

**Redis** - In-memory cache/key-value store
```python
class RedisIOManager(dg.IOManager):
    def handle_output(self, context, obj: pl.DataFrame):
        import redis
        import json

        r = redis.Redis(host='localhost', port=6379)
        key = f"asset:{context.asset_key.path[-1]}"

        # Store as JSON
        r.set(key, json.dumps(obj.to_dicts()))
        r.expire(key, 3600)  # 1 hour TTL
```

**Use Cases:**
- Caching pipeline results
- Session storage
- Real-time leaderboards
- Pub/sub messaging

---

### 4. Object Storage

**AWS S3** (works with MinIO, LocalStack)
```python
class S3ParquetIOManager(UPathIOManager):
    def __init__(self, bucket: str, prefix: str = ""):
        super().__init__(base_path=f"s3://{bucket}/{prefix}")

    def dump_to_path(self, context, obj: pl.DataFrame, path):
        obj.write_parquet(path)
```

**Google Cloud Storage**
```python
class GCSParquetIOManager(UPathIOManager):
    def __init__(self, bucket: str, prefix: str = ""):
        super().__init__(base_path=f"gs://{bucket}/{prefix}")
```

**Use Cases:**
- Data lake storage
- Archival
- Sharing data across teams
- Serving static files

---

### 5. Lakehouse Formats

**Apache Iceberg** - Table format for huge analytic datasets
```python
from pyiceberg.catalog import load_catalog

class IcebergIOManager(dg.IOManager):
    def handle_output(self, context, obj: pl.DataFrame):
        catalog = load_catalog("prod")
        table = catalog.load_table("honey_duck.sales")

        # Append or overwrite
        table.overwrite(obj.to_arrow())
```

**Delta Lake** - ACID transactions on data lakes
```python
class DeltaLakeIOManager(dg.IOManager):
    def handle_output(self, context, obj: pl.DataFrame):
        from deltalake import write_deltalake

        table_path = f"s3://my-bucket/delta/sales"
        write_deltalake(table_path, obj.to_pandas(), mode="overwrite")
```

**Use Cases:**
- Large-scale analytics
- Time travel queries
- ACID transactions on object storage
- Schema evolution

---

### 6. Streaming / Messaging

**Apache Kafka** - Event streaming platform
```python
class KafkaIOManager(dg.IOManager):
    def handle_output(self, context, obj: pl.DataFrame):
        from kafka import KafkaProducer
        import json

        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        topic = context.asset_key.path[-1]

        for record in obj.to_dicts():
            producer.send(topic, record)

        producer.flush()
```

**Use Cases:**
- Real-time data pipelines
- Event-driven architectures
- Microservices communication
- Stream processing

---

### 7. Visualization / BI Tools

**Tableau** - Via Hyper files
```python
from tableauhyperapi import HyperProcess, Connection, TableDefinition

class TableauHyperIOManager(dg.IOManager):
    def handle_output(self, context, obj: pl.DataFrame):
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(hyper.endpoint, 'output.hyper', CreateMode.CREATE_AND_REPLACE) as connection:
                # Define table schema
                table_def = TableDefinition(...)
                connection.catalog.create_table(table_def)

                # Insert data
                connection.execute_command(...)
```

**Power BI** - Via CSV/Parquet
```python
# Power BI can read from S3, Azure Blob, or file shares
# Use existing ParquetIOManager with accessible path
```

---

### 8. APIs / Webhooks

**REST API** - POST pipeline results
```python
class RestAPIIOManager(dg.IOManager):
    def __init__(self, endpoint: str, api_key: str):
        self.endpoint = endpoint
        self.api_key = api_key

    def handle_output(self, context, obj: pl.DataFrame):
        import requests

        response = requests.post(
            self.endpoint,
            json=obj.to_dicts(),
            headers={"Authorization": f"Bearer {self.api_key}"}
        )

        response.raise_for_status()

        context.add_output_metadata({
            "status_code": response.status_code,
            "records_sent": len(obj),
        })
```

**GraphQL** - Mutation-based updates
```python
class GraphQLIOManager(dg.IOManager):
    def handle_output(self, context, obj: pl.DataFrame):
        from gql import gql, Client
        from gql.transport.requests import RequestsHTTPTransport

        transport = RequestsHTTPTransport(url="https://api.example.com/graphql")
        client = Client(transport=transport, fetch_schema_from_transport=True)

        for record in obj.to_dicts():
            mutation = gql("""
                mutation CreateSale($input: SaleInput!) {
                    createSale(input: $input) {
                        id
                    }
                }
            """)
            client.execute(mutation, variable_values={"input": record})
```

---

### 9. Email / Reports

**Email with Attachments**
```python
class EmailReportIOManager(dg.IOManager):
    def handle_output(self, context, obj: pl.DataFrame):
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.base import MIMEBase
        from email import encoders
        import io

        # Write DataFrame to Excel in memory
        buffer = io.BytesIO()
        obj.write_excel(buffer)
        buffer.seek(0)

        # Create email
        msg = MIMEMultipart()
        msg['Subject'] = 'Daily Sales Report'
        msg['From'] = 'pipeline@company.com'
        msg['To'] = 'team@company.com'

        # Attach Excel file
        part = MIMEBase('application', 'vnd.ms-excel')
        part.set_payload(buffer.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename=sales.xlsx')
        msg.attach(part)

        # Send
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(user, password)
            server.send_message(msg)
```

---

### 10. File Formats for Users

**Excel** - For business users
```python
class ExcelIOManager(UPathIOManager):
    extension = ".xlsx"

    def dump_to_path(self, context, obj: pl.DataFrame, path):
        obj.write_excel(path)
```

**CSV** - Universal compatibility
```python
class CSVIOManager(UPathIOManager):
    extension = ".csv"

    def dump_to_path(self, context, obj: pl.DataFrame, path):
        obj.write_csv(path)
```

**Feather/Arrow** - Fast DataFrame interchange
```python
class FeatherIOManager(UPathIOManager):
    extension = ".feather"

    def dump_to_path(self, context, obj: pl.DataFrame, path):
        obj.write_ipc(path)
```

---

## Decision Matrix

| Output Target | Best For | Avoid If |
|--------------|----------|----------|
| **PostgreSQL** | OLTP, ACID transactions | Large analytics queries |
| **BigQuery/Snowflake** | Analytics, BI, ML | Small datasets, cost-sensitive |
| **MongoDB** | Flexible schemas, rapid dev | Complex joins, transactions |
| **Redis** | Caching, real-time | Persistent storage needed |
| **S3/GCS** | Data lakes, archival | Low-latency queries |
| **Iceberg/Delta** | Large-scale analytics, ACID on cloud | Simple use cases |
| **Kafka** | Real-time streaming | Batch-only workflows |
| **Elasticsearch** | Full-text search | Only structured queries |
| **CSV/Excel** | Business users | Programmatic access |

---

## Implementation Pattern

All IO Managers follow this pattern:

```python
class MyIOManager(dg.IOManager):
    def __init__(self, **config):
        # Store configuration
        self.config = config

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        # Write DataFrame to destination
        # Add metadata
        context.add_output_metadata({...})

    def load_input(self, context: InputContext) -> pl.DataFrame:
        # Read DataFrame from destination
        return df
```

---

## Choosing an Output

### Questions to Ask:

1. **Query Pattern**: OLTP or OLAP?
2. **Scale**: MB, GB, TB, PB?
3. **Latency**: Real-time or batch?
4. **Users**: Technical or business?
5. **Budget**: Self-hosted or managed?
6. **Schema**: Fixed or flexible?
7. **Transactions**: ACID needed?
8. **Integration**: Existing tools?

### Common Combinations:

**Analytics Pipeline:**
```
Source → DuckDB (transform) → Parquet (storage) → BigQuery (analytics)
```

**Search Application:**
```
Source → PostgreSQL (OLTP) → Elasticsearch (search) → Redis (cache)
```

**ML Pipeline:**
```
Source → Parquet (raw) → S3 (features) → BigQuery (training data)
```

**Real-time Dashboard:**
```
Source → Kafka (streaming) → TimescaleDB (time-series) → Grafana
```

---

## Resources

- **Dagster IO Manager Docs**: https://docs.dagster.io/concepts/io-management/io-managers
- **Custom IO Manager Guide**: https://docs.dagster.io/concepts/io-management/io-managers#writing-a-custom-io-manager
- **Community IO Managers**: https://github.com/dagster-io/dagster/tree/master/python_modules/libraries

---

**Need a specific IO Manager implemented? Let me know!** 
