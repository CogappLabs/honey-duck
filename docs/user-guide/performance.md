# Performance Tuning Guide

Comprehensive guide to optimizing Dagster pipelines with Polars, DuckDB, and Parquet for maximum performance.

## Table of Contents

- [Quick Wins](#quick-wins)
- [Lazy Evaluation](#lazy-evaluation)
- [Parquet Optimizations](#parquet-optimizations)
- [DuckDB Performance](#duckdb-performance)
- [Memory Management](#memory-management)
- [Profiling and Monitoring](#profiling-and-monitoring)
- [Common Bottlenecks](#common-bottlenecks)

---

## Quick Wins

### 1. Use Lazy Evaluation

**Eager (Slow)**:
```python
df = pl.read_parquet("data.parquet")
df = df.filter(pl.col("value") > 100)
df = df.select(["id", "value"])
df = df.group_by("id").agg(pl.sum("value"))
result = df.collect()  # Everything executes here
```

**Lazy (Fast)**:
```python
result = (
    pl.scan_parquet("data.parquet")  # Lazy scan
    .filter(pl.col("value") > 100)   # Pushdown filter
    .select(["id", "value"])          # Projection pushdown
    .group_by("id").agg(pl.sum("value"))
    .collect()  # Execute optimized query plan
)
```

**Why it's faster**:
- **Predicate pushdown**: Filter before reading data
- **Projection pushdown**: Read only needed columns
- **Query optimization**: Polars optimizes the entire plan
- **Memory efficient**: Streams data instead of loading all

**Performance gain**: 2-10x depending on dataset size and filters

---

### 2. Use Column Selection Early

**Bad - Reads all columns**:
```python
df = pl.scan_parquet("large_data.parquet")
result = df.select(["id", "name"]).collect()
```

**Good - Reads only needed columns**:
```python
result = pl.scan_parquet(
    "large_data.parquet",
    columns=["id", "name"],  # Parquet column pruning
).collect()
```

**Or with read_harvest_table_lazy**:
```python
from cogapp_deps.dagster import read_harvest_table_lazy

df = read_harvest_table_lazy(
    HARVEST_PARQUET_DIR,
    "sales_raw",
    columns=["sale_id", "sale_price_usd"],  # Only read these
    asset_name="my_asset",
)
```

**Performance gain**: 3-20x for wide tables (depends on column count)

---

### 3. Batch Read Multiple Tables

**Bad - Multiple function calls**:
```python
sales = read_harvest_table_lazy(dir, "sales_raw", asset_name="asset")
artworks = read_harvest_table_lazy(dir, "artworks_raw", asset_name="asset")
artists = read_harvest_table_lazy(dir, "artists_raw", asset_name="asset")
```

**Good - Single batch read**:
```python
tables = read_harvest_tables_lazy(
    HARVEST_PARQUET_DIR,
    ("sales_raw", ["sale_id", "sale_price_usd"]),
    ("artworks_raw", ["artwork_id", "title"]),
    ("artists_raw", ["artist_id", "name"]),
    asset_name="my_asset",
)

sales = tables["sales_raw"]
artworks = tables["artworks_raw"]
artists = tables["artists_raw"]
```

**Performance gain**: Reduces validation overhead, cleaner code

---

### 4. Use Streaming for Large Files

**Bad - Loads entire file into memory**:
```python
df = pl.read_parquet("100gb_file.parquet")
result = df.filter(pl.col("value") > 100).collect()
```

**Good - Streams with lazy evaluation**:
```python
result = (
    pl.scan_parquet("100gb_file.parquet")
    .filter(pl.col("value") > 100)
    .collect(streaming=True)  # Process in chunks
)
```

**Performance gain**: Works with datasets larger than RAM

---

## Lazy Evaluation

### Understanding Lazy vs Eager

| Operation | Eager (`DataFrame`) | Lazy (`LazyFrame`) |
|-----------|---------------------|-------------------|
| Read | `pl.read_parquet()` | `pl.scan_parquet()` |
| Execution | Immediate | On `.collect()` |
| Memory | Full dataset in RAM | Streams data |
| Optimization | None | Query plan optimized |
| Use case | Small data, exploration | Production pipelines |

### When to Use Lazy

**Use lazy for**:
- Production pipelines
- Large datasets (>1GB)
- Multiple transformations
- Filter-heavy operations

**Use eager for**:
- Exploration in notebooks
- Very small data (<100MB)
- When you need immediate results
- Debugging specific operations

### Lazy Evaluation Patterns

**Pattern: Filter Early, Collect Late**
```python
@dg.asset
def sales_transform(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Lazy evaluation pushes filters down to scan."""

    result = (
        pl.scan_parquet(HARVEST_PARQUET_DIR / "sales_raw")
        .filter(pl.col("sale_date") >= "2023-01-01")  # Predicate pushdown
        .filter(pl.col("sale_price_usd") > 10000)     # Combined with above
        .join(
            pl.scan_parquet(HARVEST_PARQUET_DIR / "artworks_raw"),
            on="artwork_id",
            how="left",
        )
        .select([  # Projection pushdown
            "sale_id",
            "sale_price_usd",
            "title",
            "artist_name",
        ])
        .collect()  # Execute optimized plan
    )

    return result
```

**Pattern: Lazy Join with Selective Columns**
```python
# Only read columns needed for join and output
sales = (
    pl.scan_parquet(HARVEST_PARQUET_DIR / "sales_raw")
    .select(["sale_id", "artwork_id", "sale_price_usd"])
)

artworks = (
    pl.scan_parquet(HARVEST_PARQUET_DIR / "artworks_raw")
    .select(["artwork_id", "title", "artist_id"])
)

result = (
    sales
    .join(artworks, on="artwork_id")
    .filter(pl.col("sale_price_usd") > 100000)
    .collect()
)
```

---

## Parquet Optimizations

### Partitioning

**For time-series data, partition by date**:

```python
# Write partitioned Parquet
df.write_parquet(
    "data/sales/",
    partition_by="sale_year",  # Creates subdirectories: 2023/, 2024/
)

# Read only 2024 data (reads only 2024/ partition)
df = pl.scan_parquet("data/sales/sale_year=2024/*.parquet").collect()
```

**Performance gain**: 10-100x for filtered queries on partitioned columns

### Row Group Size

```python
# Optimize row group size for your access pattern
df.write_parquet(
    "output.parquet",
    row_group_size=50_000,  # Default: 128K rows
    # Smaller = better for selective reads
    # Larger = better for full scans
)
```

**Guidelines**:
- **Selective queries** (filter heavy): 50K-100K rows per group
- **Full scans** (analytics): 500K-1M rows per group
- **Default** (balanced): 128K rows per group

### Compression

```python
df.write_parquet(
    "output.parquet",
    compression="zstd",  # Default: "zstd"
    compression_level=3,  # 1-22, higher = better compression, slower
)
```

**Compression comparison**:
| Codec | Speed | Ratio | Use Case |
|-------|-------|-------|----------|
| `snappy` | Fastest | 2-3x | Real-time processing |
| `zstd` (default) | Fast | 3-5x | Balanced (recommended) |
| `gzip` | Slow | 4-6x | Archival storage |
| `lz4` | Very fast | 2-3x | Low-latency systems |

---

## DuckDB Performance

### Query Optimization

**Bad - Cartesian product**:
```python
result = conn.execute("""
    SELECT * FROM sales, artworks
    WHERE sales.artwork_id = artworks.artwork_id
""").pl()
```

**Good - Explicit JOIN**:
```python
result = conn.execute("""
    SELECT *
    FROM sales
    JOIN artworks USING (artwork_id)
""").pl()
```

### Indexes and Statistics

```python
# DuckDB automatically gathers statistics on Parquet files
# No manual indexing needed!

# But you can analyze tables for better query plans
conn.execute("ANALYZE sales")
conn.execute("ANALYZE artworks")
```

### Parallel Execution

```python
import duckdb

# DuckDB uses all CPU cores by default
conn = duckdb.connect()

# Adjust if needed
conn.execute("SET threads=4")  # Limit to 4 threads
conn.execute("SET threads=8")  # Use 8 threads
```

### Memory Limits

```python
# Set memory limit for DuckDB
conn.execute("SET memory_limit='4GB'")

# Disable memory limit (use all available)
conn.execute("SET memory_limit='-1'")
```

---

## Memory Management

### Polars Memory Tips

**1. Use streaming for large datasets**:
```python
result = (
    pl.scan_parquet("large_file.parquet")
    .group_by("category")
    .agg(pl.sum("amount"))
    .collect(streaming=True)  # Process in chunks
)
```

**2. Avoid unnecessary copies**:
```python
# Creates copy
df2 = df.clone()

# No copy (shares data)
df2 = df
```

**3. Clear memory after intermediate steps**:
```python
@dg.asset
def my_asset(context):
    # Large intermediate result
    temp = expensive_computation()

    # Extract what we need
    result = temp.select(["id", "value"])

    # Free memory (Python GC will collect temp)
    del temp

    return result
```

### DuckDB Memory

**1. Use views instead of materialized tables**:
```python
# Materializes entire result
conn.execute("CREATE TABLE temp AS SELECT * FROM large_table WHERE ...")

# Virtual view (no materialization)
conn.execute("CREATE VIEW temp AS SELECT * FROM large_table WHERE ...")
```

**2. Limit result size**:
```python
# Don't fetch huge results into Python
result = conn.execute("""
    SELECT * FROM sales
    WHERE sale_date = '2024-01-01'
    LIMIT 10000  -- Reasonable limit
""").pl()
```

---

## Profiling and Monitoring

### Polars Profiling

```python
# Enable query plan visualization
df = pl.scan_parquet("data.parquet")

# Show optimized query plan
print(
    df
    .filter(pl.col("value") > 100)
    .select(["id", "value"])
    .explain()  # Shows execution plan
)
```

**Example output**:
```
PROJECT [id, value]
  FILTER value > 100
    SCAN data.parquet
      PREDICATE PUSHDOWN: value > 100
      PROJECTION: [id, value]
```

### DuckDB Profiling

```python
import duckdb

conn = duckdb.connect()

# Enable profiling
conn.execute("PRAGMA enable_profiling='json'")
conn.execute("PRAGMA profiling_output='profile.json'")

# Run query
conn.execute("SELECT * FROM sales JOIN artworks USING (artwork_id)")

# View profile
conn.execute("PRAGMA disable_profiling")

# Check profile.json for detailed timing
```

### Dagster Timing

```python
from cogapp_deps.dagster import track_timing

@dg.asset
def my_asset(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Asset with timing tracking."""

    with track_timing(context, "data loading"):
        data = load_data()

    with track_timing(context, "transformation"):
        result = transform(data)

    with track_timing(context, "aggregation"):
        final = aggregate(result)

    return final
```

**Output in logs**:
```
Completed data loading in 234.5ms
Completed transformation in 567.8ms
Completed aggregation in 123.4ms
```

---

## Common Bottlenecks

### 1. Reading Entire Dataset

**Problem**: Reading all columns when only a few are needed
```python
# Slow - reads all columns
df = pl.read_parquet("wide_table.parquet")  # 100 columns
result = df.select(["id", "name"])  # Uses only 2
```

**Solution**: Column pruning
```python
# Fast - reads only needed columns
result = pl.scan_parquet(
    "wide_table.parquet",
    columns=["id", "name"],
).collect()
```

**Performance gain**: 5-50x for wide tables

---

### 2. Collecting Too Early

**Problem**: Breaking lazy evaluation chain
```python
# Slow - multiple collects
df1 = pl.scan_parquet("data.parquet").filter(...).collect()  # Collect 1
df2 = df1.lazy().select(...).collect()  # Collect 2
df3 = df2.lazy().group_by(...).agg(...).collect()  # Collect 3
```

**Solution**: Chain operations, collect once
```python
# Fast - single collect
result = (
    pl.scan_parquet("data.parquet")
    .filter(...)
    .select(...)
    .group_by(...).agg(...)
    .collect()  # One optimized execution
)
```

**Performance gain**: 2-5x

---

### 3. Multiple Small Reads

**Problem**: Reading many small files individually
```python
# Slow - N file opens
for file in files:
    df = pl.read_parquet(file)
    process(df)
```

**Solution**: Read all files at once
```python
# Fast - single parallel read
df = pl.scan_parquet("data/*.parquet").collect()
```

**Performance gain**: 10-100x for many small files

---

### 4. Unnecessary String Operations

**Problem**: String operations on entire dataset
```python
# Slow - operates on millions of rows
df = df.with_columns(
    pl.col("name").str.to_uppercase()  # Even rows we'll filter out
)
result = df.filter(pl.col("value") > 1000)
```

**Solution**: Filter first, then transform
```python
# Fast - operates only on filtered rows
result = (
    df
    .filter(pl.col("value") > 1000)  # Reduce rows first
    .with_columns(
        pl.col("name").str.to_uppercase()  # Fewer operations
    )
)
```

**Performance gain**: Proportional to filter selectivity

---

### 5. Inefficient Joins

**Problem**: Joining on non-unique keys without aggregation
```python
# Slow - Cartesian product if keys not unique
result = sales.join(artworks, on="category")  # category not unique!
```

**Solution**: Ensure unique keys or use proper join strategy
```python
# Option 1: Aggregate first
artworks_agg = artworks.group_by("category").agg(
    pl.col("price").mean().alias("avg_price")
)
result = sales.join(artworks_agg, on="category")

# Option 2: Use specific join
result = sales.join(artworks, on="artwork_id")  # Primary key
```

---

## Performance Checklist

Before deploying a pipeline, check:

- [ ] Using `scan_parquet()` instead of `read_parquet()`?
- [ ] Collecting lazily with `.collect()` at the end?
- [ ] Reading only required columns?
- [ ] Filtering early in the query plan?
- [ ] Using appropriate Parquet compression (zstd)?
- [ ] Avoiding unnecessary `.clone()` calls?
- [ ] Using `track_timing()` to identify bottlenecks?
- [ ] Batch reading multiple tables with `read_harvest_tables_lazy()`?
- [ ] Streaming large datasets with `streaming=True`?
- [ ] Using DuckDB for complex SQL instead of Python loops?

---

## Benchmarking Examples

### Example 1: Lazy vs Eager

**Dataset**: 10M rows, 50 columns

```python
import time

# Eager (slow)
start = time.time()
df = pl.read_parquet("10m_rows.parquet")
df = df.filter(pl.col("value") > 100)
df = df.select(["id", "value"])
df = df.group_by("id").agg(pl.sum("value"))
print(f"Eager: {time.time() - start:.2f}s")  # 12.3s

# Lazy (fast)
start = time.time()
result = (
    pl.scan_parquet("10m_rows.parquet")
    .filter(pl.col("value") > 100)
    .select(["id", "value"])
    .group_by("id").agg(pl.sum("value"))
    .collect()
)
print(f"Lazy: {time.time() - start:.2f}s")  # 1.8s (6.8x faster)
```

### Example 2: Column Pruning

**Dataset**: 10M rows, 100 columns

```python
# Read all columns
start = time.time()
df = pl.read_parquet("wide_table.parquet")
result = df.select(["id", "name"])
print(f"All columns: {time.time() - start:.2f}s")  # 8.5s

# Read only needed columns
start = time.time()
result = pl.scan_parquet(
    "wide_table.parquet",
    columns=["id", "name"],
).collect()
print(f"Column pruning: {time.time() - start:.2f}s")  # 0.3s (28x faster)
```

---

## Resources

- **Polars Performance Guide**: https://pola-rs.github.io/polars-book/user-guide/lazy/intro/
- **DuckDB Performance Tips**: https://duckdb.org/docs/guides/performance/how_to_tune_workloads
- **Parquet Format**: https://parquet.apache.org/docs/file-format/
- **Dagster Asset Monitoring**: https://docs.dagster.io/concepts/assets/asset-checks

---

**Profile. Measure. Optimize.** 
