# Best Practices for Dagster Development

Guidelines and recommendations for writing maintainable, performant Dagster pipelines in honey-duck.

## Code Organization

### ✅ DO: Keep Assets Focused and Single-Purpose

```python
# ✅ GOOD - Clear, focused asset
@dg.asset(kinds={"polars"}, group_name="transform")
def sales_with_discounts(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Add discount calculations to sales data."""
    tables = read_harvest_tables_lazy(...)
    result = tables["sales_raw"].with_columns(
        pl.col("sale_price_usd") * 0.1).alias("discount")
    ).collect()
    return result
```

```python
# ❌ BAD - Doing too many things
@dg.asset
def process_everything(context):
    """Process sales, artworks, artists, media, and generate reports."""
    # 200 lines of mixed logic...
    # Hard to test, debug, and reuse
```

**Why**: Single-purpose assets are easier to test, debug, and understand.

---

### ✅ DO: Use Descriptive Asset Names

```python
# ✅ GOOD - Clear what it does
@dg.asset
def high_value_sales_over_1m(context) -> pl.DataFrame:
    pass

@dg.asset
def artworks_with_primary_images(context) -> pl.DataFrame:
    pass
```

```python
# ❌ BAD - Vague names
@dg.asset
def process_data(context) -> pl.DataFrame:
    pass

@dg.asset
def step2(context) -> pl.DataFrame:
    pass
```

**Why**: Clear names make the asset graph self-documenting.

---

### ✅ DO: Group Related Assets

```python
@dg.asset(
    kinds={"polars"},
    group_name="transform_polars",  # ← Groups in UI
)
def sales_transform_polars(context) -> pl.DataFrame:
    pass

@dg.asset(
    kinds={"polars", "json"},
    group_name="output_polars",  # ← Different group
)
def sales_output_polars(context) -> pl.DataFrame:
    pass
```

**Why**: Groups help organize assets in the UI and make navigation easier.

---

## Data Processing

### ✅ DO: Use Lazy Evaluation (Polars)

```python
# ✅ GOOD - Lazy operations, collect once
@dg.asset
def my_transform(context) -> pl.DataFrame:
    result = (
        pl.scan_parquet(path)          # ← Lazy
        .filter(pl.col("price") > 1000)
        .select(["id", "price", "date"])
        .sort("date")
        .collect()                      # ← Execute once
    )
    return result
```

```python
# ❌ BAD - Eager operations, multiple materializations
@dg.asset
def my_transform(context) -> pl.DataFrame:
    df = pl.read_parquet(path)         # ← Loads all data
    df = df.filter(pl.col("price") > 1000)  # ← Filters in memory
    df = df.select(["id", "price", "date"])
    df = df.sort("date")
    return df
```

**Why**: Lazy evaluation lets Polars optimize the entire query plan, often 10-100x faster.

---

### ✅ DO: Consolidate `with_columns` Chains

```python
# ✅ GOOD - All expressions in single batch run in parallel
result = df.with_columns(
    (pl.col("price") * 0.1).alias("tax"),
    pl.col("name").str.to_uppercase(),
    pl.col("date").dt.year().alias("year"),
)
```

```python
# ❌ BAD - Sequential execution (each waits for previous)
result = df.with_columns((pl.col("price") * 0.1).alias("tax"))
result = result.with_columns(pl.col("name").str.to_uppercase())
result = result.with_columns(pl.col("date").dt.year().alias("year"))
```

**Why**: Expressions within a single `with_columns` call are parallelized. Chaining forces sequential execution.

---

### ✅ DO: Use `sort_by` Inside Aggregations

```python
# ✅ GOOD - Sort inside aggregation preserves order within groups
result = df.group_by("category").agg(
    pl.struct("id", "value", "date")
      .sort_by("date")
      .alias("items_by_date")
)
```

```python
# ❌ BAD - Sort before group_by doesn't guarantee order within groups
result = df.sort("date").group_by("category").agg(
    pl.struct("id", "value", "date").alias("items_by_date")
)
```

**Why**: `group_by` doesn't preserve input order. Use `sort_by` inside `agg` or set `maintain_order=True`.

---

### ✅ DO: Prefer Semi-Joins Over `is_in()`

```python
# ✅ GOOD - Semi-join stays lazy, no early materialization
valid_sales = sales.join(
    valid_products.select("product_id"),
    on="product_id",
    how="semi",
)
```

```python
# ❌ BAD - is_in() forces collection of the filter list
valid_ids = valid_products.collect()["product_id"]  # Materializes!
valid_sales = sales.filter(pl.col("product_id").is_in(valid_ids))
```

**Why**: Semi-joins keep both sides lazy and allow query optimization. `is_in()` with a DataFrame column requires collecting.

---

### ✅ DO: Validate Data Early

```python
# ✅ GOOD - Validate inputs
@dg.asset
def sales_enriched(context) -> pl.DataFrame:
    tables = read_harvest_tables_lazy(
        HARVEST_PARQUET_DIR,
        ("sales_raw", ["sale_id", "sale_price_usd"]),  # ← Validates columns
        asset_name="sales_enriched",
    )

    result = tables["sales_raw"].filter(
        pl.col("sale_price_usd") > 0  # ← Business rule validation
    ).collect()

    # Validate outputs
    if len(result) == 0:
        raise ValueError("No valid sales data after filtering")

    return result
```

**Why**: Fail fast with clear error messages rather than silent data corruption.

---

### ✅ DO: Add Rich Metadata

```python
# ✅ GOOD - Rich metadata
@dg.asset
def sales_transform(context) -> pl.DataFrame:
    with track_timing(context, "transformation"):
        result = transform_sales()

    add_dataframe_metadata(
        context,
        result,
        # Business metrics
        total_revenue=float(result["sale_price_usd"].sum()),
        avg_sale=float(result["sale_price_usd"].mean()),
        unique_customers=result["customer_id"].n_unique(),
        date_range=f"{result['sale_date'].min()} to {result['sale_date'].max()}",
        # Data quality metrics
        null_prices=int(result["sale_price_usd"].is_null().sum()),
    )

    return result
```

**Why**: Metadata makes debugging easier and provides observability in the UI.

---

## Error Handling

### ✅ DO: Use Descriptive Error Messages

```python
# ✅ GOOD - Clear, actionable error
@dg.asset
def validate_sales(context) -> pl.DataFrame:
    result = load_sales()

    invalid_prices = result.filter(pl.col("price") < 0)
    if len(invalid_prices) > 0:
        sample_ids = invalid_prices["sale_id"].head(5).to_list()
        raise ValueError(
            f"Found {len(invalid_prices)} sales with negative prices. "
            f"Sample IDs: {sample_ids}. "
            f"Check data source for corruption."
        )

    return result
```

```python
# ❌ BAD - Vague error
@dg.asset
def validate_sales(context) -> pl.DataFrame:
    result = load_sales()
    if len(result.filter(pl.col("price") < 0)) > 0:
        raise ValueError("Invalid data")  # ← What's invalid? How to fix?

    return result
```

**Why**: Specific errors save hours of debugging time.

---

### ✅ DO: Log Progress for Long Operations

```python
# ✅ GOOD - Progress logging
@dg.asset
def process_large_dataset(context) -> pl.DataFrame:
    context.log.info("Loading data from source...")
    data = load_data()

    context.log.info(f"Loaded {len(data):,} rows")

    context.log.info("Applying transformations...")
    result = transform(data)

    context.log.info(f"Transformations complete. Output: {len(result):,} rows")

    return result
```

**Why**: Logs help you understand where time is spent and identify bottlenecks.

---

## Performance

### ✅ DO: Profile Before Optimizing

```python
# ✅ GOOD - Measure, then optimize
@dg.asset
def my_transform(context) -> pl.DataFrame:
    with track_timing(context, "loading"):
        data = load_data()

    with track_timing(context, "filtering"):
        filtered = data.filter(...)

    with track_timing(context, "aggregating"):
        result = filtered.group_by(...).agg(...)

    # Check metadata to see which step is slow
    return result
```

**Why**: Optimization without measurement is guesswork. Measure first!

---

### ✅ DO: Use Appropriate Data Types

```python
# ✅ GOOD - Efficient types
df = pl.DataFrame({
    "id": pl.Series([1, 2, 3], dtype=pl.UInt32),      # Not Int64
    "price": pl.Series([1.5, 2.5], dtype=pl.Float32), # Not Float64
    "category": pl.Series(["A", "B"], dtype=pl.Categorical),  # Not String
})
```

**Why**: Smaller types = less memory = faster processing. Use UInt32 for IDs, Float32 for prices, Categorical for categories.

---

### ✅ DO: Filter Early, Select Late

```python
# ✅ GOOD - Filter first (reduces data), select last
result = (
    pl.scan_parquet(path)
    .filter(pl.col("price") > 1000)     # ← Reduce rows first
    .filter(pl.col("country") == "US")
    .select(["id", "price", "date"])    # ← Reduce columns last
    .collect()
)
```

```python
# ❌ BAD - Select first, filter last
result = (
    pl.scan_parquet(path)
    .select(["id", "price", "date", "country", "many", "other", "cols"])  # ← Too many columns
    .filter(pl.col("price") > 1000)     # ← Filtering large dataset
    .filter(pl.col("country") == "US")
    .collect()
)
```

**Why**: Filtering early reduces data volume, making subsequent operations faster.

---

## Testing

### ✅ DO: Write Tests for Business Logic

```python
# tests/test_sales_logic.py
def test_discount_calculation():
    """Test that discounts are calculated correctly."""
    # Arrange
    sales = pl.DataFrame({
        "sale_id": [1, 2],
        "price": [100.0, 200.0],
    })

    # Act
    result = apply_discount(sales, discount_pct=10)

    # Assert
    assert result["discounted_price"].to_list() == [90.0, 180.0]
```

**Why**: Tests catch bugs early and make refactoring safer.

---

### ✅ DO: Test Edge Cases

```python
def test_handles_empty_dataframe():
    """Test that pipeline handles empty input gracefully."""
    empty_df = pl.DataFrame(schema={"sale_id": pl.Int64, "price": pl.Float64})

    # Should not crash
    result = process_sales(empty_df)

    assert len(result) == 0
    assert result.schema == expected_schema
```

**Why**: Edge cases often cause production failures.

---

## Documentation

### ✅ DO: Write Helpful Docstrings

```python
# ✅ GOOD - Clear docstring
@dg.asset(kinds={"polars"}, group_name="transform")
def sales_with_price_tiers(
    context: dg.AssetExecutionContext,
    sales_transform: pl.DataFrame,
) -> pl.DataFrame:
    """Categorize sales into price tiers (budget/mid/premium).

    Applies the following tiers:
    - Budget: < $500k
    - Mid: $500k - $3M
    - Premium: >= $3M

    Returns:
        DataFrame with additional 'price_tier' column
    """
    pass
```

```python
# ❌ BAD - No docstring
@dg.asset
def process(context, data):
    pass  # What does this do? What are the tiers?
```

**Why**: Good docstrings help teammates (and future you) understand the code.

---

### ✅ DO: Document Business Rules

```python
@dg.asset
def filter_valid_sales(context) -> pl.DataFrame:
    """Filter sales to include only valid transactions.

    Business rules (as of 2024-01):
    - Sale price must be positive
    - Sale date must be within last 10 years
    - Must have valid artwork_id reference
    - Excludes test/demo sales (buyer_country != 'XX')

    See: https://wiki.company.com/sales-validation-rules
    """
    pass
```

**Why**: Business logic changes over time. Documentation preserves context.

---

## Dependencies

### ✅ DO: Use `deps` for External Dependencies

```python
# ✅ GOOD - Clear external dependency
from honey_duck.defs.helpers import STANDARD_HARVEST_DEPS

@dg.asset(
    deps=STANDARD_HARVEST_DEPS,  # ← External CSV/SQLite files
    kinds={"dlt"},
)
def my_harvest(context):
    """Load data from external CSV files."""
    pass
```

**Why**: Makes it clear which assets depend on external data sources.

---

### ✅ DO: Keep Dependency Graphs Shallow

```python
# ✅ GOOD - Shallow graph (3 levels)
csv_sales → harvest_sales → sales_transform → sales_output

# ❌ BAD - Deep graph (10+ levels)
raw → clean → validate → enrich → normalize → categorize →
    aggregate → summarize → filter → output
```

**Why**: Shallow graphs are easier to understand and faster to recompute.

---

## Configuration

### ✅ DO: Use Environment Variables for Config

```python
# ✅ GOOD - Environment-based config
import os

OUTPUT_PATH = Path(os.getenv("SALES_OUTPUT_PATH", "data/output/json/sales.json"))
THRESHOLD = int(os.getenv("SALES_THRESHOLD", "1000"))
```

```python
# ❌ BAD - Hardcoded values
OUTPUT_PATH = Path("/home/user/data/sales.json")  # ← Breaks on other machines
THRESHOLD = 1000  # ← Can't override without code change
```

**Why**: Environment variables make code portable and configurable.

---

### ✅ DO: Use Constants for Business Rules

```python
# honey_duck/defs/constants.py
MIN_SALE_VALUE_USD = 10_000
PRICE_TIER_BUDGET_MAX_USD = 500_000
PRICE_TIER_MID_MAX_USD = 3_000_000

# In asset code
from honey_duck.defs.constants import MIN_SALE_VALUE_USD

result = df.filter(pl.col("price") >= MIN_SALE_VALUE_USD)
```

**Why**: Centralized constants are easier to update and audit.

---

## Anti-Patterns to Avoid

### ❌ DON'T: Modify Input Data In-Place

```python
# ❌ BAD
@dg.asset
def my_asset(context, input_df: pl.DataFrame) -> pl.DataFrame:
    input_df = input_df.with_columns(...)  # ← Modifies parameter
    return input_df
```

```python
# ✅ GOOD
@dg.asset
def my_asset(context, input_df: pl.DataFrame) -> pl.DataFrame:
    result = input_df.with_columns(...)  # ← New variable
    return result
```

---

### ❌ DON'T: Use Magic Numbers

```python
# ❌ BAD
df = df.filter(pl.col("price") > 1000000)  # What does 1M mean?
df = df.filter(pl.col("age_days") < 365)   # Why 365?
```

```python
# ✅ GOOD
PREMIUM_PRICE_THRESHOLD = 1_000_000  # Prices above 1M are "premium"
MAX_AGE_DAYS = 365  # Only include sales from last year

df = df.filter(pl.col("price") > PREMIUM_PRICE_THRESHOLD)
df = df.filter(pl.col("age_days") < MAX_AGE_DAYS)
```

---

### ❌ DON'T: Ignore Errors Silently

```python
# ❌ BAD
try:
    result = risky_operation()
except Exception:
    result = None  # ← Silent failure!
```

```python
# ✅ GOOD
try:
    result = risky_operation()
except ValueError as e:
    context.log.error(f"Operation failed: {e}")
    raise  # Re-raise for visibility
```

---

### ❌ DON'T: Create God Assets

```python
# ❌ BAD - One asset doing everything
@dg.asset
def complete_pipeline(context):
    # Load
    # Transform
    # Validate
    # Aggregate
    # Write
    # Notify
    # 500 lines of code...
```

```python
# ✅ GOOD - Split into focused assets
@dg.asset
def load_data(context): ...

@dg.asset
def transform_data(context, load_data): ...

@dg.asset
def validate_data(context, transform_data): ...
```

---

## Quick Checklist

Before committing code, check:

- [ ] Asset has a clear, descriptive name
- [ ] Asset has a docstring explaining what it does
- [ ] Uses helper functions (`read_harvest_tables_lazy`, `add_dataframe_metadata`)
- [ ] Returns correct type (usually `pl.DataFrame`)
- [ ] Adds useful metadata (record counts, business metrics)
- [ ] Uses lazy operations where possible (Polars `scan_*`)
- [ ] Validates inputs and outputs
- [ ] Has logging for long operations
- [ ] Has tests for business logic
- [ ] No hardcoded paths or magic numbers
- [ ] Follows honey-duck naming conventions

---

**Remember**: Good code is code that's easy to understand, test, and change. When in doubt, prefer clarity over cleverness! 🎯
