# Dagster Best Practices - Improvements for honey-duck

Analysis of honey-duck project against official Dagster best practices with actionable recommendations.

**Based on**:
- [Dagster Best Practice Guides](https://docs.dagster.io/guides/best-practices)
- [Organizing Projects](https://docs.dagster.io/guides/dagster/recommended-project-structure)
- [Testing Assets](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops)
- [Asset Checks](https://docs.dagster.io/guides/test/asset-checks)
- [Declarative Automation](https://docs.dagster.io/guides/automate/declarative-automation)
- [Resources Best Practices](https://dagster.io/blog/a-practical-guide-to-dagster-resources)

---

## ✅ Current Strengths

**What we're already doing well**:

1. **Asset-based architecture** - Using `@dg.asset` throughout
2. **Clear dependency management** - Explicit `deps` declarations
3. **Rich metadata** - Using `add_output_metadata()` extensively
4. **Group organization** - Assets grouped logically (harvest, transform, output)
5. **Multiple implementations** - Different patterns for learning (polars, duckdb, ops)
6. **Comprehensive documentation** - Good README, guides, examples
7. **IO Manager pattern** - Custom Parquet/DuckDB IO managers
8. **Testing** - 46 unit tests covering core functionality

---

## 🔧 Recommended Improvements

### 1. Add Asset Checks for Data Quality

**Current State**: We use `add_output_metadata()` but no formal asset checks

**Best Practice**: Use `@asset_check` for data validation

**Implementation**:

```python
# honey_duck/defs/checks.py

from dagster import asset_check, AssetCheckResult

@asset_check(asset="sales_transform_polars")
def check_sales_no_nulls(sales_transform_polars: pl.DataFrame) -> AssetCheckResult:
    """Verify no null values in critical columns."""
    null_counts = sales_transform_polars.null_count()

    critical_cols = ["sale_id", "sale_price_usd", "artwork_id"]
    nulls_found = {
        col: null_counts[col][0]
        for col in critical_cols
        if null_counts[col][0] > 0
    }

    passed = len(nulls_found) == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "null_counts": nulls_found if nulls_found else {"message": "No nulls"},
            "total_rows": len(sales_transform_polars),
        },
        description="No null values in critical columns" if passed else f"Found nulls: {nulls_found}",
    )

@asset_check(asset="sales_transform_polars")
def check_sales_price_range(sales_transform_polars: pl.DataFrame) -> AssetCheckResult:
    """Verify sale prices are within reasonable bounds."""
    min_price = sales_transform_polars["sale_price_usd"].min()
    max_price = sales_transform_polars["sale_price_usd"].max()

    # Reasonable art price range
    valid = 0 < min_price and max_price < 1_000_000_000

    return AssetCheckResult(
        passed=valid,
        metadata={
            "min_price": float(min_price),
            "max_price": float(max_price),
            "price_range": f"${min_price:,.0f} - ${max_price:,.0f}",
        },
    )

@asset_check(asset="artworks_transform_polars")
def check_artworks_schema(artworks_transform_polars: pl.DataFrame) -> AssetCheckResult:
    """Verify schema matches expected contract."""
    expected_cols = {
        "artwork_id", "title", "artist_name", "year",
        "medium", "price_tier", "total_sales_count"
    }

    actual_cols = set(artworks_transform_polars.columns)
    missing = expected_cols - actual_cols
    extra = actual_cols - expected_cols

    return AssetCheckResult(
        passed=len(missing) == 0,
        metadata={
            "missing_columns": list(missing) if missing else [],
            "extra_columns": list(extra) if extra else [],
            "total_columns": len(actual_cols),
        },
    )
```

**Register in definitions.py**:
```python
from .checks import (
    check_sales_no_nulls,
    check_sales_price_range,
    check_artworks_schema,
)

defs = dg.Definitions(
    assets=[...],
    asset_checks=[
        check_sales_no_nulls,
        check_sales_price_range,
        check_artworks_schema,
    ],
)
```

**Benefits**:
- ✅ UI shows check status on asset page
- ✅ Fails materializations if checks fail
- ✅ Historical tracking of data quality
- ✅ Can set severity (WARN vs ERROR)

**References**:
- [Testing with Asset Checks](https://docs.dagster.io/guides/test/asset-checks)
- [Data Contracts](https://docs.dagster.io/guides/test/data-contracts)

---

### 2. Use ConfigurableResource Instead of Plain Classes

**Current State**: Resources defined as plain Python classes

**Best Practice**: Use `ConfigurableResource` for type safety and config management

**Current** (`honey_duck/defs/resources.py`):
```python
# Current approach
HARVEST_PARQUET_DIR = Path(__file__).parent.parent.parent / "data" / "output" / "dlt"
```

**Improved**:
```python
# honey_duck/defs/resources.py

from dagster import ConfigurableResource
from pydantic import Field

class HoneyDuckPathsResource(ConfigurableResource):
    """Centralized path configuration with environment variable support."""

    harvest_parquet_dir: str = Field(
        default="data/output/dlt",
        description="Directory for DLT harvest Parquet files",
    )

    json_output_dir: str = Field(
        default="data/output/json",
        description="Directory for JSON output files",
    )

    storage_dir: str = Field(
        default="data/output/storage",
        description="Directory for IO manager Parquet storage",
    )

    @property
    def harvest_path(self) -> Path:
        return Path(self.harvest_parquet_dir)

    @property
    def json_path(self) -> Path:
        return Path(self.json_output_dir)

    @property
    def storage_path(self) -> Path:
        return Path(self.storage_dir)


class HoneyDuckConfigResource(ConfigurableResource):
    """Business logic configuration with validation."""

    min_sale_value_usd: int = Field(
        default=30_000_000,
        description="Minimum sale value for high-value filtering",
        gt=0,  # Pydantic validation: must be > 0
    )

    price_tier_budget_max_usd: int = Field(default=50_000, gt=0)
    price_tier_mid_max_usd: int = Field(default=500_000, gt=0)

    def validate_tiers(self) -> None:
        """Validate tier ordering."""
        if not (self.price_tier_budget_max_usd < self.price_tier_mid_max_usd):
            raise ValueError("Budget tier max must be < Mid tier max")
```

**Usage in assets**:
```python
from dagster import AssetExecutionContext

@dg.asset
def sales_output_polars(
    context: AssetExecutionContext,
    sales_transform_polars: pl.DataFrame,
    paths: HoneyDuckPathsResource,
    config: HoneyDuckConfigResource,
) -> pl.DataFrame:
    """Filter high-value sales using injected resources."""

    result = sales_transform_polars.filter(
        pl.col("sale_price_usd") >= config.min_sale_value_usd
    )

    output_path = paths.json_path / "sales_output.json"
    write_json_output(result, output_path, context)

    return result
```

**Register in definitions.py**:
```python
defs = dg.Definitions(
    assets=[...],
    resources={
        "paths": HoneyDuckPathsResource(
            harvest_parquet_dir=EnvVar("HARVEST_DIR"),  # From .env
            json_output_dir=EnvVar("OUTPUT_DIR"),
        ),
        "config": HoneyDuckConfigResource(
            min_sale_value_usd=EnvVar.int("MIN_SALE_VALUE"),
        ),
    },
)
```

**Benefits**:
- ✅ Type safety with Pydantic validation
- ✅ Environment variable support with `EnvVar`
- ✅ Config schema visible in UI
- ✅ Testable (mock resources in tests)
- ✅ Reusable across assets, sensors, schedules

**References**:
- [Defining Resources](https://docs.dagster.io/guides/build/external-resources/defining-resources)
- [Configuring Resources](https://docs.dagster.io/guides/build/external-resources/configuring-resources)
- [Resources Best Practices Blog](https://dagster.io/blog/a-practical-guide-to-dagster-resources)

---

### 3. Add Declarative Automation (Replaces FreshnessPolicy)

**Current State**: Using `FreshnessPolicy` (deprecated in 1.6+)

**Best Practice**: Use declarative automation conditions

**Current**:
```python
@dg.asset(
    freshness_policy=dg.FreshnessPolicy(maximum_lag_minutes=24 * 60),
)
def sales_output_polars(...):
    ...
```

**Improved**:
```python
from dagster import AutomationCondition

@dg.asset(
    # Auto-materialize when upstream changes
    automation_condition=AutomationCondition.on_upstream_asset_updates(),
)
def sales_transform_polars(sales_joined_polars: pl.DataFrame):
    ...

@dg.asset(
    # Auto-materialize on cron schedule
    automation_condition=AutomationCondition.on_cron("0 */6 * * *"),  # Every 6 hours
)
def sales_output_polars(...):
    ...

@dg.asset(
    # Combine conditions: eager on upstream + max once per hour
    automation_condition=(
        AutomationCondition.on_upstream_asset_updates() &
        AutomationCondition.on_cron("0 * * * *")
    ),
)
def artworks_output_polars(...):
    ...
```

**Complex example**:
```python
# Only materialize if:
# - Upstream updated AND
# - Haven't materialized in last hour AND
# - Not currently materializing
automation_condition = (
    AutomationCondition.on_upstream_asset_updates() &
    ~AutomationCondition.in_latest_time_window("1 hour") &
    ~AutomationCondition.in_progress()
)
```

**Benefits**:
- ✅ More expressive than FreshnessPolicy
- ✅ Composable with `&`, `|`, `~` operators
- ✅ Future-proof (FreshnessPolicy deprecated)
- ✅ Works with sensors and schedules

**References**:
- [Declarative Automation](https://docs.dagster.io/guides/automate/declarative-automation)
- [Auto-Materialize Glossary](https://dagster.io/glossary/auto-materialization)
- [Declarative Scheduling Blog](https://dagster.io/blog/declarative-scheduling)

---

### 4. Add Asset Observations for External Data

**Current State**: No observations for data we don't control

**Best Practice**: Use `observe` decorator for external data sources

**Implementation**:
```python
# honey_duck/defs/observations.py

from dagster import observable_source_asset, ObserveResult
import requests

@observable_source_asset
def external_artwork_api() -> ObserveResult:
    """Monitor external artwork API for changes."""

    response = requests.get("https://api.example.com/artworks/count")
    current_count = response.json()["total"]

    # Store count as data version for change detection
    return ObserveResult(
        metadata={
            "artwork_count": current_count,
            "last_checked": datetime.now().isoformat(),
        },
        data_version=str(current_count),  # Changes trigger downstream
    )

@dg.asset(
    deps=[external_artwork_api],
    automation_condition=AutomationCondition.on_upstream_asset_updates(),
)
def artworks_from_api(context):
    """Fetches artworks when external API changes."""
    # Only materializes when data_version changes
    response = requests.get("https://api.example.com/artworks")
    return pl.DataFrame(response.json())
```

**With sensors**:
```python
from dagster import sensor, RunRequest

@sensor(target=AssetSelection.assets(external_artwork_api))
def artwork_api_sensor():
    """Check API every 5 minutes."""
    # Sensor triggers observation
    # Observation updates data_version
    # Downstream assets auto-materialize if version changed
    yield RunRequest()
```

**Benefits**:
- ✅ Track external data without materializing
- ✅ Data version changes trigger downstream
- ✅ Historical tracking of external state
- ✅ Decouples observation from materialization

**References**:
- [Asset Observations](https://docs.dagster.io/guides/build/assets/metadata-and-tags/asset-observations)

---

### 5. Improve Testing with build_asset_context

**Current State**: Tests use basic assertions

**Best Practice**: Use Dagster testing utilities

**Current** (`tests/`):
```python
def test_transform():
    df = transform_data()
    assert len(df) > 0
```

**Improved**:
```python
# tests/test_assets_polars.py

from dagster import build_asset_context, materialize
import pytest

def test_sales_transform_with_context():
    """Test asset with full context for metadata validation."""

    # Mock upstream data
    mock_sales_joined = pl.DataFrame({
        "sale_id": [1, 2],
        "sale_price_usd": [100, 200],
        "artist_name": ["  Monet  ", "picasso"],
    })

    # Build context
    context = build_asset_context()

    # Import asset function directly
    from honey_duck.defs.assets_polars import sales_transform_polars

    # Execute with context
    result = sales_transform_polars(context, mock_sales_joined)

    # Validate result
    assert len(result) == 2
    assert result["artist_name"][0] == "MONET"  # Uppercase and stripped
    assert result["artist_name"][1] == "PICASSO"

    # Validate metadata was added
    # (Note: metadata only added to actual runs, not in tests)

def test_sales_transform_with_materialize():
    """Test asset with full materialization (includes IO manager)."""

    from honey_duck.defs import defs
    from honey_duck.defs.assets_polars import sales_transform_polars

    # Materialize upstream first
    result = materialize(
        [sales_transform_polars],
        resources=defs.get_resource_defs_dict(),
    )

    assert result.success
    assert result.output_for_node("sales_transform_polars") is not None

@pytest.fixture
def mock_harvest_data():
    """Fixture for consistent test data."""
    return {
        "sales_raw": pl.DataFrame({
            "sale_id": [1, 2, 3],
            "sale_price_usd": [100, 200, 300],
            "artwork_id": ["AW1", "AW2", "AW3"],
        }),
        "artworks_raw": pl.DataFrame({
            "artwork_id": ["AW1", "AW2", "AW3"],
            "title": ["Art 1", "Art 2", "Art 3"],
        }),
    }

def test_asset_with_mock_upstream(mock_harvest_data):
    """Test with mocked upstream dependencies."""

    context = build_asset_context()

    # Pass mock data directly
    result = sales_joined_polars(
        context,
        # Override upstream deps with mocks
        sales_raw=mock_harvest_data["sales_raw"],
        artworks_raw=mock_harvest_data["artworks_raw"],
    )

    assert len(result) == 3
```

**Test asset checks**:
```python
from dagster import build_asset_check_context

def test_sales_no_nulls_check():
    """Test asset check logic."""

    # Valid data
    valid_df = pl.DataFrame({
        "sale_id": [1, 2],
        "sale_price_usd": [100, 200],
        "artwork_id": ["AW1", "AW2"],
    })

    result = check_sales_no_nulls(valid_df)
    assert result.passed

    # Invalid data (with nulls)
    invalid_df = pl.DataFrame({
        "sale_id": [1, None],
        "sale_price_usd": [100, 200],
        "artwork_id": ["AW1", "AW2"],
    })

    result = check_sales_no_nulls(invalid_df)
    assert not result.passed
    assert "sale_id" in result.metadata["null_counts"]
```

**Benefits**:
- ✅ Full context with logging and metadata
- ✅ Test with IO managers
- ✅ Mock upstream dependencies
- ✅ Validate metadata output
- ✅ Test asset checks independently

**References**:
- [Unit Testing Assets](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops)
- [Testing Asset Checks](https://docs.dagster.io/guides/test/asset-checks)

---

### 6. Add Partitions for Time-Series Data

**Current State**: No partitioning

**Best Practice**: Partition time-series assets for incremental processing

**Implementation**:
```python
from dagster import DailyPartitionsDefinition, StaticPartitionsDefinition

# Daily partitions for sales
daily_partitions = DailyPartitionsDefinition(start_date="2020-01-01")

@dg.asset(
    partitions_def=daily_partitions,
    deps=HARVEST_DEPS,
)
def sales_by_date_polars(
    context: AssetExecutionContext,
) -> pl.DataFrame:
    """Sales data partitioned by date for incremental processing."""

    # Get partition key (e.g., "2024-01-15")
    partition_date = context.partition_key

    # Read only data for this partition
    sales = read_harvest_table_lazy(
        HARVEST_PARQUET_DIR,
        "sales_raw",
        asset_name="sales_by_date_polars",
    )

    result = sales.filter(
        pl.col("sale_date") == partition_date
    ).collect()

    context.log.info(f"Processed {len(result)} sales for {partition_date}")

    return result

# Backfill partitions: dagster partition backfill --partition-selector 2024-01-01:2024-12-31
```

**Multi-dimensional partitions**:
```python
from dagster import MultiPartitionsDefinition

# Partition by date AND country
multi_partitions = MultiPartitionsDefinition({
    "date": daily_partitions,
    "country": StaticPartitionsDefinition(["US", "UK", "FR", "DE"]),
})

@dg.asset(partitions_def=multi_partitions)
def sales_by_date_and_country(context: AssetExecutionContext):
    # Get partition keys
    date = context.partition_key_range.start  # "2024-01-15"
    country = context.partition_key_range.end  # "US"

    # Process only this slice
    ...
```

**Benefits**:
- ✅ Incremental processing (only new dates)
- ✅ Parallel backfills
- ✅ UI shows partition status
- ✅ Reprocess specific dates easily

**References**:
- [Partitioning Assets](https://docs.dagster.io/concepts/partitions-schedules-sensors/partitioning-assets)
- [Partitions and Backfills](https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions)

---

## 📋 Implementation Priority

### High Priority (Do Now)

1. **Asset Checks** - Critical for data quality
   - Immediate value in UI
   - Catches issues early
   - Minimal code change

2. **ConfigurableResource** - Architecture improvement
   - Better testing
   - Environment config
   - Type safety

3. **Testing Improvements** - Foundation for quality
   - Use `build_asset_context`
   - Test asset checks
   - Add more coverage

### Medium Priority (Next Sprint)

4. **Declarative Automation** - Replace deprecated FreshnessPolicy
   - Future-proof
   - More flexible scheduling
   - Better auto-materialization

5. **Asset Observations** - For external data
   - Only if monitoring external APIs
   - Useful for RSS feeds, external DBs

### Low Priority (Consider Later)

6. **Partitions** - For large-scale time-series
   - Only if processing historical data
   - Useful for incremental loads
   - Adds complexity

---

## 🎯 Quick Wins

### 1. Add One Asset Check (5 minutes)

```bash
# Add to honey_duck/defs/checks.py
@asset_check(asset="sales_output_polars")
def check_output_not_empty(sales_output_polars: pl.DataFrame):
    passed = len(sales_output_polars) > 0
    return AssetCheckResult(
        passed=passed,
        metadata={"row_count": len(sales_output_polars)},
    )

# Register in definitions.py
from .checks import check_output_not_empty

defs = dg.Definitions(
    assets=[...],
    asset_checks=[check_output_not_empty],
)
```

### 2. Convert One Resource (10 minutes)

```python
# honey_duck/defs/resources.py
from dagster import ConfigurableResource

class PathsResource(ConfigurableResource):
    harvest_dir: str = "data/output/dlt"
    json_dir: str = "data/output/json"

# definitions.py
defs = dg.Definitions(
    resources={
        "paths": PathsResource(),
    },
)
```

### 3. Add Automation Condition (2 minutes)

```python
# Replace freshness_policy
@dg.asset(
    automation_condition=AutomationCondition.on_cron("0 */6 * * *"),
)
def sales_output_polars(...):
    ...
```

---

## 📚 Additional Resources

**Official Dagster Documentation**:
- [Best Practice Guides](https://docs.dagster.io/guides/best-practices)
- [Data Quality Guide](https://dagster.io/guides/data-observability-in-2025-pillars-pros-cons-best-practices)
- [Testing Guides](https://docs.dagster.io/guides/test)
- [Resources Guide](https://dagster.io/blog/a-practical-guide-to-dagster-resources)

**Community Resources**:
- [Dagster Slack](https://dagster.io/slack)
- [GitHub Discussions](https://github.com/dagster-io/dagster/discussions)
- [Blog Posts](https://dagster.io/blog)

---

**Current Dagster Version**: 1.12.10 (January 2026)

**Next Review**: Check for new features in Dagster 1.13+
