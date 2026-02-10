---
title: Pandera Validation
description: Schema validation for Polars DataFrames with Pandera, including lazy validation, row filtering, and Dagster asset check integration.
---

# Pandera Schema Validation

Validate Polars DataFrames against typed schemas using [Pandera](https://pandera.readthedocs.io/). Schemas define expected columns, types, and constraints as Python classes. Failed validation can block downstream assets or surface warnings.

!!! info "Pandera vs Soda"
    Both validate data quality, but serve different layers:

    - **Pandera** validates **Polars DataFrames in Python** — used for transform assets where data is already in memory
    - **[Soda](soda-validation.md)** validates **Parquet files via DuckDB SQL** — used for path-based pipelines where data never enters Python

    See [Logging & Reporting](../user-guide/logging-and-reporting.md) for how to surface validation results as alerts and notifications.

## Installation

Pandera is included in the project dependencies. It uses the `pandera.polars` backend for native Polars support:

```python
import pandera.polars as pa
```

See the [Pandera Polars docs](https://pandera.readthedocs.io/en/latest/polars.html) for full API details.

## Defining Schemas

!!! note "Why Pandera over Pydantic?"
    Pydantic validates individual records (row-by-row). Pandera validates entire DataFrames as columnar data, which is fundamentally different:

    - **Column-level checks**: Constraints like "all prices > 0" run as vectorised operations, not per-row loops
    - **Statistical checks**: Validate distributions, uniqueness, nullability across the full dataset
    - **Native Polars/Pandas support**: Validates DataFrames directly without converting to dicts or model instances
    - **Lazy validation**: Collect all errors across all columns in one pass
    - **Dagster integration**: Returns structured error metadata suitable for asset checks

    Pydantic is the right choice for API request/response validation and config objects. Pandera is the right choice for validating DataFrames in ETL pipelines.

Pandera offers two APIs: `DataFrameSchema` (object-based, imperative) and `DataFrameModel` (class-based, declarative). Both compile to the same validation logic - `DataFrameModel` is syntactic sugar that calls `.to_schema()` under the hood. This project uses `DataFrameModel` throughout for its cleaner, Pydantic-style syntax. Use `DataFrameSchema` only if you need to build schemas dynamically at runtime.

Schemas are Python classes that declare expected columns with types and constraints. Define them in `shared/schemas.py`:

```python
import pandera.polars as pa

class SalesTransformSchema(pa.DataFrameModel):
    """Schema for sales_transform output."""

    sale_id: int
    artwork_id: int
    sale_date: str
    sale_price_usd: int = pa.Field(gt=0)
    buyer_country: str
    title: str
    artist_name: str = pa.Field(nullable=False)
    artwork_year: float = pa.Field(nullable=True)
    price_diff: int
    pct_change: float = pa.Field(nullable=True)

    class Config:
        coerce = True    # Cast compatible types automatically
        strict = False   # Allow extra columns not in schema
```

### Field Constraints

| Constraint | Example | Description |
|-----------|---------|-------------|
| `gt`, `ge`, `lt`, `le` | `pa.Field(gt=0)` | Numeric bounds (greater than, greater or equal, etc.) |
| `nullable` | `pa.Field(nullable=True)` | Allow null values |
| `isin` | `pa.Field(isin=["a", "b"])` | Value must be in list |
| `str_matches` | `pa.Field(str_matches=r"^\d+$")` | Regex match |
| `eq` | `pa.Field(eq=100)` | Exact value |

See the [Pandera Field reference](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.api.checks.Check.html) for all available checks.

### Config Options

| Option | Default | Description |
|--------|---------|-------------|
| `coerce` | `False` | Attempt to cast columns to declared types before validation |
| `strict` | `False` | Controls handling of columns not defined in the schema (see below) |

**`strict` modes:**

| Value | Behaviour |
|-------|-----------|
| `False` (default) | Extra columns are ignored — they pass through untouched |
| `True` | Raises `SchemaError` if the DataFrame has any columns not in the schema |
| `"filter"` | Silently drops columns not defined in the schema from the validated output |

Use `strict="filter"` when you want the schema to act as a column whitelist, keeping only the declared columns:

```python
class CleanOutputSchema(pa.DataFrameModel):
    sale_id: int
    sale_price_usd: int = pa.Field(gt=0)
    artist_name: str

    class Config:
        coerce = True
        strict = "filter"  # Only these 3 columns survive validation
```

## Basic Validation

### Fail-Fast (Default)

Standard validation raises on the first error. Use this in [blocking asset checks](https://docs.dagster.io/guides/test/data-contracts) to prevent downstream assets from materializing:

```python
import pandera.polars as pa

try:
    SalesTransformSchema.validate(df)
except pa.errors.SchemaError as e:
    print(f"Validation failed: {e}")
```

### Lazy Validation

Lazy validation collects **all** errors before returning, rather than stopping on the first failure. Use `validate_lazy()` when you need a complete picture of data quality. See the [Pandera lazy validation docs](https://pandera.readthedocs.io/en/stable/lazy_validation.html) for background.

```python
from honey_duck.defs.shared.schemas import validate_lazy

result = validate_lazy(df, SalesTransformSchema)

if not result.valid:
    for err in result.errors:
        print(f"  Column '{err['column']}': {err['check']} — {err['failure_case']}")
```

`ValidationResult` fields:

| Field | Type | Description |
|-------|------|-------------|
| `valid` | `bool` | Whether all rows passed |
| `error_count` | `int` | Total number of errors |
| `errors` | `list[dict]` | Each error's column, check name, and failure case |
| `invalid_indices` | `set[int]` | Row indices that failed |

### Row Filtering

Remove invalid rows instead of failing. Useful when source data has known quality issues and you want to process the clean subset:

```python
from honey_duck.defs.shared.schemas import filter_invalid_rows

clean_df, removed_count = filter_invalid_rows(df, SalesTransformSchema)
print(f"Removed {removed_count} invalid rows, {len(clean_df)} remaining")
```

### Validation Report

Get a structured report suitable for Dagster metadata:

```python
from honey_duck.defs.shared.schemas import validate_with_report

passed, metadata = validate_with_report(df, SalesTransformSchema, asset_name="sales_transform")
context.add_output_metadata(metadata)
```

Returns a dict with `schema`, `record_count`, `valid`, `error_count`, and (when invalid) `error_summary` grouped by column.

## Dagster Integration

### Blocking Asset Checks

Use Pandera with [Dagster asset checks](https://docs.dagster.io/api/dagster/asset-checks) to gate downstream assets on schema validity:

```python
import dagster as dg
import pandera.polars as pa
import polars as pl

@dg.asset_check(asset=sales_transform, blocking=True)
def check_sales_transform_schema(sales_transform: pl.DataFrame) -> dg.AssetCheckResult:
    """Validate sales_transform against Pandera schema.

    Blocking: If this fails, sales_output will not materialize.
    """
    try:
        SalesTransformSchema.validate(sales_transform)
        return dg.AssetCheckResult(
            passed=True,
            metadata={
                "record_count": dg.MetadataValue.int(len(sales_transform)),
                "schema": dg.MetadataValue.text("SalesTransformSchema"),
            },
        )
    except pa.errors.SchemaError as e:
        return dg.AssetCheckResult(
            passed=False,
            metadata={
                "error": dg.MetadataValue.text(str(e)),
                "schema": dg.MetadataValue.text("SalesTransformSchema"),
            },
        )
```

### Non-Blocking Checks (Warnings)

Omit `blocking=True` to record validation issues without stopping the pipeline. These appear in the Dagster UI checks tab and can be picked up by sensors for alerting:

```python
@dg.asset_check(asset=sales_transform)  # Not blocking
def check_sales_data_quality(sales_transform: pl.DataFrame) -> dg.AssetCheckResult:
    """Warn about data quality issues without stopping the pipeline."""
    result = validate_lazy(sales_transform, SalesTransformSchema)

    return dg.AssetCheckResult(
        passed=result.valid,
        metadata={
            "error_count": result.error_count,
            "errors": dg.MetadataValue.json(result.errors) if result.errors else None,
            "invalid_row_count": len(result.invalid_indices),
        },
    )
```

See [Logging & Reporting: Data Quality Warnings](../user-guide/logging-and-reporting.md#data-quality-warnings) for patterns on alerting and emailing validation warnings.

### In-Asset Validation with Filtering

Validate inside the asset itself, filter bad rows, and attach warnings as metadata:

```python
@dg.asset
def sales_transform(context: dg.AssetExecutionContext) -> pl.DataFrame:
    result = transform()

    clean_df, removed = filter_invalid_rows(result, SalesTransformSchema)

    if removed > 0:
        validation = validate_lazy(result, SalesTransformSchema)
        context.log.warning(f"Filtered {removed} invalid rows ({validation.error_count} errors)")
        context.add_output_metadata({
            "validation_warnings": dg.MetadataValue.json({
                "removed_rows": removed,
                "error_count": validation.error_count,
                "errors": validation.errors,
            }),
        })

    return clean_df
```

## Project Schemas

The project defines two schemas in `shared/schemas.py`:

### SalesTransformSchema

Validates the `sales_transform` asset output:

| Column | Type | Constraints |
|--------|------|-------------|
| `sale_id` | `int` | Required |
| `artwork_id` | `int` | Required |
| `sale_date` | `str` | Required |
| `sale_price_usd` | `int` | `> 0` |
| `buyer_country` | `str` | Required |
| `title` | `str` | Required |
| `artist_name` | `str` | Not nullable |
| `artwork_year` | `float` | Nullable |
| `list_price_usd` | `int` | `> 0` |
| `price_diff` | `int` | Required |
| `pct_change` | `float` | Nullable |

### ArtworksTransformSchema

Validates the `artworks_transform` asset output:

| Column | Type | Constraints |
|--------|------|-------------|
| `artwork_id` | `int` | Required |
| `title` | `str` | Required |
| `year` | `float` | Nullable |
| `medium` | `str` | Required |
| `list_price_usd` | `int` | `> 0` |
| `artist_name` | `str` | Not nullable |
| `sale_count` | `int` | `>= 0` |
| `total_sales_value` | `float` | `>= 0` |
| `avg_sale_price` | `float` | Nullable |
| `has_sold` | `bool` | Required |
| `price_tier` | `str` | One of: `budget`, `mid`, `premium` |
| `sales_rank` | `int` | `>= 1` |
| `primary_image` | `str` | Nullable |
| `media_count` | `int` | `>= 0` |

## Adding a New Schema

1. Define the schema class in `src/honey_duck/defs/shared/schemas.py`:

    ```python
    class MyTransformSchema(pa.DataFrameModel):
        id: int
        name: str = pa.Field(nullable=False)
        value: float = pa.Field(ge=0)

        class Config:
            coerce = True
            strict = False
    ```

2. Create a blocking asset check in `src/honey_duck/defs/shared/checks.py`:

    ```python
    @dg.asset_check(asset=my_transform, blocking=True)
    def check_my_transform_schema(my_transform: pl.DataFrame) -> dg.AssetCheckResult:
        try:
            MyTransformSchema.validate(my_transform)
            return dg.AssetCheckResult(passed=True, metadata={"record_count": len(my_transform)})
        except pa.errors.SchemaError as e:
            return dg.AssetCheckResult(passed=False, metadata={"error": str(e)})
    ```

3. Register the check in `definitions.py` under `asset_checks=[...]`.

!!! abstract "Helper opportunity: `pandera_asset_check` factory"
    The try/validate/catch pattern is identical for every Pandera-based check. A factory could generate checks from a schema class:

    ```python
    def pandera_asset_check(
        asset: dg.AssetsDefinition,
        schema: type[pa.DataFrameModel],
        blocking: bool = True,
    ) -> dg.AssetChecksDefinition:
        @dg.asset_check(asset=asset, blocking=blocking)
        def _check(context, **kwargs) -> dg.AssetCheckResult:
            df = next(iter(kwargs.values()))
            try:
                schema.validate(df)
                return dg.AssetCheckResult(
                    passed=True,
                    metadata={"record_count": len(df), "schema": schema.__name__},
                )
            except pa.errors.SchemaError as e:
                return dg.AssetCheckResult(
                    passed=False,
                    metadata={"error": str(e), "schema": schema.__name__},
                )
        _check.__name__ = f"check_{asset.key.to_python_identifier()}_schema"
        return _check

    # Usage - 2 lines instead of 20:
    check_sales = pandera_asset_check(sales_transform, SalesTransformSchema)
    check_artworks = pandera_asset_check(artworks_transform, ArtworksTransformSchema)
    ```

## LazyFrame Validation

Pandera can validate a `LazyFrame`, but by default it only checks the **schema** (column names and types), not data-level constraints like `gt=0` or `isin`. This is because data-level checks require materializing the frame.

```python
# Schema-only validation (no collect, fast)
validated_lf = SalesTransformSchema.validate(lazy_frame)

# Chain with .pipe()
result = (
    pl.scan_parquet(path)
    .pipe(SalesTransformSchema.validate)
    .filter(...)
    .collect()
)
```

### Forcing Full Validation on LazyFrames

Set the `PANDERA_VALIDATION_DEPTH` environment variable to run data-level checks on LazyFrames. Pandera will collect under the hood:

```bash
export PANDERA_VALIDATION_DEPTH=SCHEMA_AND_DATA
```

Or set it in Python:

```python
import pandera as pa

pa.config.validation_depth = pa.ValidationDepth.SCHEMA_AND_DATA
```

!!! warning
    With `SCHEMA_AND_DATA`, validating a LazyFrame triggers a `.collect()` internally. For large datasets, this may cause memory issues.

### Validation Depth Summary

| Mode | LazyFrame behaviour | DataFrame behaviour |
|------|--------------------|--------------------|
| `SCHEMA_ONLY` (default for LazyFrame) | Column names + types only | Column names + types only |
| `SCHEMA_AND_DATA` | Collects, then validates data | Full validation |

### Large Datasets: Chunked Validation

For datasets that don't fit in memory, validate in chunks:

```python
import polars as pl
import pandera.polars as pa

CHUNK_SIZE = 500_000

def validate_in_chunks(
    path: str,
    schema: type[pa.DataFrameModel],
    chunk_size: int = CHUNK_SIZE,
) -> list[dict]:
    """Validate a large parquet file in chunks to avoid memory issues."""
    all_errors = []
    lf = pl.scan_parquet(path)
    total_rows = lf.select(pl.len()).collect().item()

    for offset in range(0, total_rows, chunk_size):
        chunk = lf.slice(offset, chunk_size).collect()
        try:
            schema.validate(chunk, lazy=True)
        except pa.errors.SchemaErrors as e:
            for failure in e.failure_cases.to_dicts():
                failure["chunk_offset"] = offset
                all_errors.append(failure)

    return all_errors
```

!!! tip "Consider Soda for large datasets"
    If your data is too large for in-memory validation, [Soda](soda-validation.md) may be a better fit. Soda validates parquet files via DuckDB SQL queries and never loads data into Python memory.

## Cross-File Referential Integrity

Pandera validates a single DataFrame at a time - it can't natively check that a foreign key in one file exists in another. Use Polars anti-joins to validate referential integrity across parquet files:

```python
import polars as pl
import dagster as dg

def check_referential_integrity(
    source_path: str,
    reference_path: str,
    key: str,
    source_label: str = "source",
    reference_label: str = "reference",
) -> tuple[bool, dict]:
    """Check that all values of `key` in source exist in reference.

    Uses an anti-join on lazy scans. Polars pushes the column
    projection down so only the key column is read from each file.

    Returns:
        Tuple of (passed, metadata_dict)
    """
    orphans = (
        pl.scan_parquet(source_path)
        .select(key)
        .join(
            pl.scan_parquet(reference_path).select(key),
            on=key,
            how="anti",
        )
        .unique()
        .collect()
    )

    passed = len(orphans) == 0
    metadata = {
        "key": key,
        "orphan_count": len(orphans),
        "source": source_label,
        "reference": reference_label,
    }

    if not passed:
        # Include sample of orphaned keys for debugging
        sample = orphans[key].head(10).to_list()
        metadata["orphan_sample"] = sample

    return passed, metadata
```

### As a Dagster Asset Check

```python
@dg.asset_check(asset=sales_transform)
def check_artist_ids_exist(
    context, sales_transform: pl.DataFrame, paths: PathsResource,
) -> dg.AssetCheckResult:
    """Verify all artist_ids in sales exist in the artists source."""
    passed, metadata = check_referential_integrity(
        source_path=f"{paths.storage_dir}/sales_transform.parquet",
        reference_path=f"{paths.harvest_dir}/raw/artists_raw/**/*.parquet",
        key="artist_id",
        source_label="sales_transform",
        reference_label="artists_raw",
    )

    if not passed:
        context.log.warning(
            f"Found {metadata['orphan_count']} artist_ids in sales "
            f"missing from artists: {metadata.get('orphan_sample', [])}"
        )

    return dg.AssetCheckResult(
        passed=passed,
        metadata={k: dg.MetadataValue.json(v) if isinstance(v, list) else v for k, v in metadata.items()},
    )
```

### Multiple Foreign Keys

Validate several relationships at once:

```python
REFERENTIAL_CHECKS = [
    ("artwork_id", "raw/artworks_raw/**/*.parquet", "artworks_raw"),
    ("artist_id", "raw/artists_raw/**/*.parquet", "artists_raw"),
]

@dg.asset_check(asset=sales_transform)
def check_foreign_keys(
    context, sales_transform: pl.DataFrame, paths: PathsResource,
) -> dg.AssetCheckResult:
    """Verify all foreign keys in sales reference valid source records."""
    all_orphans = {}
    source_path = f"{paths.storage_dir}/sales_transform.parquet"

    for key, ref_glob, ref_label in REFERENTIAL_CHECKS:
        ref_path = f"{paths.harvest_dir}/{ref_glob}"
        passed, metadata = check_referential_integrity(
            source_path, ref_path, key,
            source_label="sales_transform", reference_label=ref_label,
        )
        if not passed:
            all_orphans[key] = metadata

    if all_orphans:
        context.log.warning(f"Referential integrity failures: {list(all_orphans.keys())}")

    return dg.AssetCheckResult(
        passed=len(all_orphans) == 0,
        metadata={
            "checks_run": len(REFERENTIAL_CHECKS),
            "checks_failed": len(all_orphans),
            "failures": dg.MetadataValue.json(all_orphans) if all_orphans else None,
        },
    )
```

### DuckDB Alternative

For path-based pipelines where data stays in DuckDB, use SQL instead:

```sql
-- Find artwork_ids in sales that don't exist in artworks
SELECT DISTINCT s.artwork_id
FROM 'data/storage/sales_transform.parquet' s
WHERE s.artwork_id NOT IN (
    SELECT artwork_id FROM 'data/harvest/raw/artworks_raw/**/*.parquet'
)
```

```python
@dg.asset_check(asset=sales_transform_soda)
def check_artwork_ids_soda(
    context, sales_transform_soda: str, database: DatabaseResource,
) -> dg.AssetCheckResult:
    """SQL-based referential integrity check via DuckDB."""
    with database.get_connection() as conn:
        orphans = conn.sql(f"""
            SELECT DISTINCT artwork_id
            FROM '{sales_transform_soda}'
            WHERE artwork_id NOT IN (
                SELECT artwork_id FROM 'data/harvest/raw/artworks_raw/**/*.parquet'
            )
        """).fetchall()

    return dg.AssetCheckResult(
        passed=len(orphans) == 0,
        metadata={
            "orphan_count": len(orphans),
            "orphan_sample": [r[0] for r in orphans[:10]] if orphans else None,
        },
    )
```

## Limitations

- The [Polars integration](https://pandera.readthedocs.io/en/latest/polars.html) is less mature than the Pandas backend - some features may not be available yet
- Data synthesis strategies (hypothesis testing) are not supported for Polars
- Custom checks receive a `PolarsData` object (not a raw Series) - see the [Polars docs](https://pandera.readthedocs.io/en/latest/polars.html) for details

## Resources

- **Pandera docs**: [pandera.readthedocs.io](https://pandera.readthedocs.io/)
- **Polars backend**: [Pandera Polars integration](https://pandera.readthedocs.io/en/latest/polars.html)
- **Lazy validation**: [Collecting all errors](https://pandera.readthedocs.io/en/stable/lazy_validation.html)
- **Dagster asset checks**: [Data Contracts guide](https://docs.dagster.io/guides/test/data-contracts) | [API reference](https://docs.dagster.io/api/dagster/asset-checks)
- **Related**: [Soda Validation](soda-validation.md) (SQL-based validation for DuckDB pipelines)
- **Related**: [Logging & Reporting](../user-guide/logging-and-reporting.md) (alerting on validation failures)
