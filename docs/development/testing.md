# Testing Guide

Comprehensive guide to testing Dagster pipelines in honey-duck.

## Running Tests

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run specific test file
uv run pytest tests/test_validation.py

# Run excluding slow tests
uv run pytest -m "not slow"

# Run only integration tests
uv run pytest -m integration
```

## Test Categories

### Unit Tests (`test_validation.py`, `test_processors.py`)

Test individual functions in isolation:

```python
def test_validate_dataframe():
    df = pl.DataFrame({"a": [1], "b": [2]})
    # Should not raise
    validate_dataframe(df, ["a", "b"], "test_asset")
```

### Smoke Tests (`test_smoke.py`)

Verify imports and basic configuration work:

```python
def test_definitions_imports():
    from honey_duck.defs.definitions import defs
    assert defs is not None
```

### Integration Tests (`test_integration.py`)

Test full pipeline execution with real data:

```python
@pytest.mark.slow
def test_polars_ops_pipeline_executes(self, tmp_path: Path):
    job = defs.get_job_def("polars_ops_pipeline")
    result = job.execute_in_process()
    assert result.success
```

### Linting Tests (`test_linting.py`)

Enforce best practices without running pipelines:

```python
def test_all_assets_have_group_names(self, asset_graph):
    for key in asset_graph.get_all_asset_keys():
        node = asset_graph.get(key)
        if node.is_materializable:
            assert node.group_name is not None
```

## Writing Asset Tests

### Testing Assets Directly

Call asset functions with mock inputs:

```python
from honey_duck.defs.polars.assets import sales_joined_polars

def test_sales_joined_polars(smoke_paths):
    # Assets can be called as regular functions
    result = sales_joined_polars(context, paths=smoke_paths)
    assert isinstance(result, pl.LazyFrame)
```

### Testing with `dg.materialize()`

For assets with dependencies, use Dagster's materialize:

```python
import dagster as dg

def test_asset_chain():
    result = dg.materialize(
        assets=[upstream_asset, downstream_asset],
        resources={"paths": mock_paths},
    )
    assert result.success

    # Check output values
    output = result.output_for_node("downstream_asset")
    assert len(output) > 0
```

### Testing with Context

For assets requiring `AssetExecutionContext`:

```python
def test_asset_with_context():
    context = dg.build_asset_context()
    result = my_asset(context, paths=mock_paths)
    assert result is not None
```

## Test Fixtures

Common fixtures are defined in `tests/conftest.py`:

| Fixture | Purpose |
|---------|---------|
| `temp_harvest_dir` | Temporary directory with empty Parquet schema files |
| `smoke_paths` | `PathsResource` pointing to temp directories |
| `smoke_output_paths` | `OutputPathsResource` for temp output files |
| `noop_io_manager` | IO manager that accepts but doesn't persist data |
| `mock_resources` | Complete resource dict for `materialize()` |
| `sample_sales_df` | Sample sales DataFrame for unit tests |
| `sample_artworks_df` | Sample artworks DataFrame for unit tests |

### Using Fixtures

```python
def test_with_fixtures(temp_harvest_dir, smoke_paths):
    result = read_harvest_table_lazy(
        temp_harvest_dir,
        "sales_raw",
        asset_name="test",
    )
    assert isinstance(result, pl.LazyFrame)
```

## Pytest Markers

Tests can be marked for selective execution:

```python
@pytest.mark.slow
def test_full_pipeline():
    """This test takes a while to run."""
    ...

@pytest.mark.integration
def test_database_connection():
    """This test requires external services."""
    ...
```

Run specific markers:

```bash
# Skip slow tests during development
uv run pytest -m "not slow"

# Run only integration tests
uv run pytest -m integration
```

## Best Practices

### 1. Separate Calculations from IO

Test pure transformation logic separately from IO:

```python
# Good: Test the transformation logic
def test_calculate_price_tier():
    assert calculate_price_tier(500) == "budget"
    assert calculate_price_tier(5000) == "mid"
    assert calculate_price_tier(50000) == "premium"

# Separate test for asset that uses it
def test_asset_applies_price_tiers(mock_resources):
    result = dg.materialize([my_asset], resources=mock_resources)
    assert result.success
```

### 2. Use NoOpIOManager for Smoke Tests

Avoid writing to disk during quick tests:

```python
class NoOpIOManager(IOManager):
    def handle_output(self, context, obj):
        context.log.info(f"NoOp: Would persist {len(obj)} records")

    def load_input(self, context):
        return pl.DataFrame().lazy()
```

### 3. Test Error Conditions

Verify assets fail gracefully:

```python
def test_raises_on_missing_table(temp_harvest_dir):
    with pytest.raises(dg.Failure) as exc_info:
        read_harvest_table_lazy(
            temp_harvest_dir / "raw",
            "nonexistent_table",
            asset_name="test",
        )
    assert "nonexistent_table" in str(exc_info.value)
```

### 4. Test Asset Graph Structure

Verify dependencies are correct:

```python
def test_transform_depends_on_harvest():
    asset_graph = defs.resolve_asset_graph()
    node = asset_graph.get(dg.AssetKey("sales_transform_polars"))

    assert any("harvest" in str(p) for p in node.parent_keys)
```

## References

- [Dagster Testing Guide](https://docs.dagster.io/guides/test)
- [Testing with Dagster (Dagster University)](https://dagster.io/blog/dagster-university-presents-testing-with-dagster)
- [How I Write Tests for Dagster](https://blog.rmhogervorst.nl/blog/2024/02/27/how-i-write-tests-for-dagster/)
