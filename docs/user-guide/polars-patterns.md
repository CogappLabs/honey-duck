# Polars Patterns

Best practices for writing efficient Polars code in Dagster pipelines.

## Lazy Evaluation

Always prefer lazy operations and collect once at the end:

```python
# GOOD - Lazy operations, collect once
result = (
    pl.scan_parquet(path)
    .filter(pl.col("price") > 1000)
    .select(["id", "price", "date"])
    .sort("date")
    .collect()  # Execute once
)

# BAD - Eager operations
df = pl.read_parquet(path)  # Loads all data
df = df.filter(pl.col("price") > 1000)
df = df.select(["id", "price", "date"])
```

## Consolidate `with_columns` Chains

Multiple expressions in a single `with_columns` run in parallel:

```python
# GOOD - Parallel execution
result = df.with_columns(
    (pl.col("price") * 0.1).alias("tax"),
    pl.col("name").str.to_uppercase(),
    pl.col("date").dt.year().alias("year"),
)

# BAD - Sequential execution
result = df.with_columns((pl.col("price") * 0.1).alias("tax"))
result = result.with_columns(pl.col("name").str.to_uppercase())
result = result.with_columns(pl.col("date").dt.year().alias("year"))
```

!!! tip "When to split"
    Only split `with_columns` when later expressions depend on earlier ones:
    ```python
    result = (
        df.with_columns(pl.col("a").fill_null(0))  # First: fill nulls
        .with_columns((pl.col("a") > 0).alias("has_a"))  # Then: use filled value
    )
    ```

## Sort Inside Aggregations

`sort()` before `group_by()` doesn't guarantee order within groups:

```python
# GOOD - Sort inside aggregation
result = df.group_by("category").agg(
    pl.struct("id", "value", "date")
      .sort_by("date")
      .alias("items_by_date")
)

# BAD - Sort before group_by
result = df.sort("date").group_by("category").agg(
    pl.struct("id", "value", "date").alias("items_by_date")
)
```

## Semi-Joins Over `is_in()`

Semi-joins stay lazy and avoid early materialization:

```python
# GOOD - Semi-join stays lazy
valid_sales = sales.join(
    valid_products.select("product_id"),
    on="product_id",
    how="semi",
)

# BAD - is_in() forces collection
valid_ids = valid_products.collect()["product_id"]  # Materializes!
valid_sales = sales.filter(pl.col("product_id").is_in(valid_ids))
```

## Streaming with LazyFrames

Return `LazyFrame` from transform assets to enable streaming writes:

```python
@dg.asset
def my_transform(context) -> pl.LazyFrame:
    """Returns LazyFrame for streaming via sink_parquet."""
    result = (
        pl.scan_parquet(path)
        .filter(...)
        .with_columns(...)
    )
    # Don't collect! Return lazy for streaming
    return result
```

The IO manager uses `sink_parquet()` instead of `write_parquet()`, never materializing the full DataFrame.

## Visualization in Metadata

Add charts and tables to Dagster asset metadata:

```python
from cogapp_deps.dagster import altair_to_metadata, table_preview_to_metadata

@dg.asset
def my_output(context, data: pl.LazyFrame) -> pl.DataFrame:
    result = data.collect()

    # Bar chart
    chart = result.plot.bar(x="category", y="count")

    # Add to metadata
    context.add_output_metadata({
        **altair_to_metadata(chart, "distribution"),
        **table_preview_to_metadata(result.head(5), "preview", "Top 5"),
    })

    return result
```

## Quick Reference

| Pattern | Do | Don't |
|---------|-----|-------|
| Evaluation | `scan_parquet().collect()` | `read_parquet()` |
| Multiple columns | Single `with_columns()` | Chained `with_columns()` |
| Sorted groups | `sort_by()` in `agg()` | `sort()` before `group_by()` |
| Filtering by list | `how="semi"` join | `is_in()` with DataFrame |
| Streaming | Return `LazyFrame` | Return `DataFrame` |

---

## External Resources

- **Polars User Guide**: [pola.rs/docs](https://docs.pola.rs/)
- **Polars API Reference**: [pola.rs/api/python](https://docs.pola.rs/api/python/stable/reference/)
- **Polars Lazy API**: [Lazy API Guide](https://docs.pola.rs/user-guide/lazy/using/)
- **Polars Expressions**: [Expression Guide](https://docs.pola.rs/user-guide/expressions/)
- **Polars GitHub**: [github.com/pola-rs/polars](https://github.com/pola-rs/polars)
