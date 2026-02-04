# Soda Data Quality Validation

Validate data quality using Soda Core v4 with DuckDB. Parquet files are validated directly via SQL - no data loads into Python memory.

## Why Soda?

Soda provides:

- **Contract-based validation**: Define expected schema and quality rules in YAML
- **SQL-native execution**: Validation runs as DuckDB queries, not Python loops
- **Blocking checks**: Prevent downstream assets from running if validation fails
- **Rich metadata**: Check results appear in Dagster UI

## Installation

Soda v4 is distributed via a private PyPI index:

```bash
pip install -i https://pypi.cloud.soda.io/simple soda-duckdb>=4
```

!!! warning "DuckDB Version Compatibility"
    The standard `soda-core-duckdb` package requires `duckdb<1.1.0`. Use the v4 package
    from `pypi.cloud.soda.io` for compatibility with modern DuckDB versions.

## Quick Start

### 1. Define a Contract

Create a YAML contract defining expected schema and quality rules:

```yaml
# contracts/sales_transform.yml
dataset: duckdb_check/sales_transform

columns:
  - name: sale_id
  - name: sale_price_usd
    checks:
      - missing:
          must_be: 0
      - invalid:
          must_be: 0
          valid_min: 1
  - name: artist_name
    checks:
      - missing:
          must_be: 0

checks:
  - row_count:
      must_be_greater_than: 0
  - duplicate:
      columns: [sale_id]
      must_be: 0
```

### 2. Create an Asset Check

```python
import dagster as dg
from .assets import sales_transform_soda

@dg.asset_check(asset=sales_transform_soda, blocking=True)
def check_sales_transform_soda(
    context: dg.AssetCheckExecutionContext,
    sales_transform_soda: str,  # Receives path from ParquetPathIOManager
) -> dg.AssetCheckResult:
    """Validate parquet against Soda contract.

    Blocking: If this fails, downstream assets won't materialize.
    """
    from soda_core.contracts import verify_contract_locally

    # Create temp data source config pointing to parquet
    ds_config = f"""
name: duckdb_check
type: duckdb
connection:
  database: "{sales_transform_soda}"
"""

    result = verify_contract_locally(
        data_source_file_path=ds_config_path,
        contract_file_path="contracts/sales_transform.yml",
        publish=False,
    )

    return dg.AssetCheckResult(
        passed=result.is_passed,
        metadata={
            "checks_passed": result.number_of_checks_passed,
            "checks_failed": result.number_of_checks_failed,
        },
    )
```

### 3. Use ParquetPathIOManager

The DuckDB+Soda pipeline uses `ParquetPathIOManager` to pass file paths (not DataFrames) between assets:

```python
from cogapp_libs.dagster import ParquetPathIOManager

defs = dg.Definitions(
    resources={
        "parquet_path_io_manager": ParquetPathIOManager(
            base_dir="data/transforms"
        ),
    },
)
```

## Contract Syntax

### Column Checks

```yaml
columns:
  - name: price
    checks:
      - missing:
          must_be: 0
      - invalid:
          must_be: 0
          valid_min: 0
          valid_max: 1000000
```

### Row-Level Checks

```yaml
checks:
  - row_count:
      must_be_greater_than: 0
  - duplicate:
      columns: [id]
      must_be: 0
```

### Freshness Checks

```yaml
checks:
  - freshness:
      column: updated_at
      fail_when_missing: true
      freshness_limit: P1D  # ISO 8601 duration
```

## Complete Pipeline Example

Here's the full DuckDB+Soda pipeline pattern used in honey-duck:

```python
import dagster as dg
from dagster_duckdb import DuckDBResource
from cogapp_libs.dagster.lineage import get_example_row

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
) -> str:
    """Transform sales data with column lineage."""
    output_path = Path(paths.transforms_dir) / "sales.parquet"

    with duckdb.get_connection() as conn:
        # Register source views
        conn.execute(f"CREATE VIEW sales AS SELECT * FROM '{paths.harvest_dir}/raw/sales/**/*.parquet'")

        # Transform and write directly to parquet
        conn.sql(TRANSFORM_SQL).write_parquet(str(output_path), compression="zstd")

        # Get example row for lineage display
        examples = get_example_row(conn, str(output_path), "sale_id", 2)

    context.add_output_metadata({
        "dagster/column_lineage": _sales_lineage(),
        "lineage_examples": examples,
        "record_count": row_count,
    })

    return str(output_path)  # Return path, not DataFrame
```

## Memory Efficiency

The ParquetPathIOManager pattern keeps data in DuckDB:

```
Traditional Pattern:
  Asset 1 → DataFrame in memory → IO Manager → Parquet
  Asset 2 ← DataFrame in memory ← IO Manager ← Parquet

Path-based Pattern (DuckDB+Soda):
  Asset 1 → DuckDB writes parquet → returns path string
  Asset 2 ← receives path string → DuckDB reads parquet
```

Benefits:

- **Zero Python memory**: Data stays in DuckDB/Parquet
- **Streaming**: DuckDB processes in batches
- **Direct SQL**: Use `FROM 'path'` in queries

## Column Lineage

The pipeline tracks column-level data flow:

```python
from dagster import TableColumnLineage, TableColumnDep

def _sales_lineage():
    return TableColumnLineage(
        deps_by_column={
            # Direct pass-through
            "sale_id": [TableColumnDep("dlt_harvest_sales_raw", "sale_id")],
            # Computed columns
            "price_diff": [
                TableColumnDep("dlt_harvest_sales_raw", "sale_price_usd"),
                TableColumnDep("dlt_harvest_artworks_raw", "price_usd"),
            ],
            # Joined columns
            "artist_name": [TableColumnDep("dlt_harvest_artists_raw", "name")],
        }
    )
```

### Example Values

Each asset emits `lineage_examples` metadata showing actual values:

```python
from cogapp_libs.dagster.lineage import get_example_row

# Fetch one row formatted for display
examples = get_example_row(conn, parquet_path, "sale_id", 2)
# {"sale_id": "2", "sale_price_usd": "$86.3M", "title": "Day Dream"}

context.add_output_metadata({
    "lineage_examples": examples,
})
```

## Troubleshooting

### Soda Not Installed

If Soda is not installed, checks pass with a warning:

```python
try:
    from soda_core.contracts import verify_contract_locally
except ImportError:
    return dg.AssetCheckResult(
        passed=True,
        metadata={"warning": "Soda not installed - check skipped"},
    )
```

### Contract File Not Found

Ensure contract paths are relative to your project root or use absolute paths:

```python
CONTRACTS_DIR = Path(__file__).parent / "contracts"
contract_path = CONTRACTS_DIR / "sales_transform.yml"
```

## Resources

- **Soda Docs**: [docs.soda.io](https://docs.soda.io/)
- **DuckDB Data Source**: [Soda DuckDB Reference](https://docs.soda.io/reference/data-source-reference-for-soda-core/duckdb/)
- **Contracts**: [Soda Contracts](https://docs.soda.io/soda/contracts/)
