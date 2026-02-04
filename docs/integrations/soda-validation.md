---
title: Soda Validation
description: Validate data quality with Soda Core contracts, blocking asset checks, and DuckDB-native validation.
---

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
import time
import dagster as dg
from dagster_duckdb import DuckDBResource
from cogapp_libs.dagster.lineage import (
    build_lineage,
    collect_parquet_metadata,
    register_harvest_views,
)

# Define column lineage using the DSL
SALES_RAW = dg.AssetKey("dlt_harvest_sales_raw")
ARTWORKS_RAW = dg.AssetKey("dlt_harvest_artworks_raw")

SALES_TRANSFORM_LINEAGE = build_lineage(
    passthrough={
        "sale_id": SALES_RAW,
        "artwork_id": SALES_RAW,
        "title": ARTWORKS_RAW,
    },
    rename={
        "artwork_year": (ARTWORKS_RAW, "year"),
        "list_price_usd": (ARTWORKS_RAW, "price_usd"),
    },
    computed={
        "price_diff": [(SALES_RAW, "sale_price_usd"), (ARTWORKS_RAW, "price_usd")],
    },
)

STATS_SQL = """
SELECT count(*) AS record_count, sum(sale_price_usd) AS total_value
FROM '{path}'
"""

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
    start = time.perf_counter()
    output_path = Path(paths.transforms_dir) / "sales.parquet"

    with duckdb.get_connection() as conn:
        # Register source views (single line replaces manual CREATE VIEW statements)
        register_harvest_views(conn, paths.harvest_dir, ["sales", "artworks", "artists"])

        # Transform and write directly to parquet
        conn.sql(TRANSFORM_SQL).write_parquet(str(output_path), compression="zstd")

        # Collect all metadata in one call
        collect_parquet_metadata(
            context, conn, str(output_path),
            lineage=SALES_TRANSFORM_LINEAGE,
            stats_sql=STATS_SQL.format(path=output_path),
            example_id=("sale_id", 2),
            elapsed_ms=(time.perf_counter() - start) * 1000,
        )

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

The pipeline tracks column-level data flow using the lineage DSL helpers:

```python
from cogapp_libs.dagster.lineage import build_lineage, passthrough_lineage
import dagster as dg

SALES_RAW = dg.AssetKey("dlt_harvest_sales_raw")
ARTWORKS_RAW = dg.AssetKey("dlt_harvest_artworks_raw")
ARTISTS_RAW = dg.AssetKey("dlt_harvest_artists_raw")
SALES_TRANSFORM = dg.AssetKey("sales_transform_soda")

# Transform asset: joins multiple sources
SALES_TRANSFORM_LINEAGE = build_lineage(
    passthrough={
        "sale_id": SALES_RAW,
        "title": ARTWORKS_RAW,
    },
    rename={
        "artist_name": (ARTISTS_RAW, "name"),
    },
    computed={
        "price_diff": [(SALES_RAW, "sale_price_usd"), (ARTWORKS_RAW, "price_usd")],
    },
)

# Output asset: columns pass through from single source
SALES_OUTPUT_LINEAGE = passthrough_lineage(
    source=SALES_TRANSFORM,
    columns=["sale_id", "title", "artist_name", "pct_change"],
    renames={"price_difference": "price_diff"},  # output_col: source_col
)
```

The DSL is more compact than manual `TableColumnLineage` construction and groups columns by their lineage type (passthrough, rename, computed).

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
