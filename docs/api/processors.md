# Processors

Reusable DataFrame transformation classes for data pipelines.

!!! info "External Documentation"
    - **DuckDB**: [duckdb.org/docs](https://duckdb.org/docs/) | [SQL Reference](https://duckdb.org/docs/sql/introduction)
    - **Polars**: [pola.rs/docs](https://docs.pola.rs/) | [API Reference](https://docs.pola.rs/api/python/stable/reference/)

---

## DuckDB Processors

### DuckDBQueryProcessor

::: cogapp_deps.processors.duckdb.DuckDBQueryProcessor

### DuckDBSQLProcessor

::: cogapp_deps.processors.duckdb.DuckDBSQLProcessor

### DuckDBWindowProcessor

::: cogapp_deps.processors.duckdb.DuckDBWindowProcessor

### DuckDBAggregateProcessor

::: cogapp_deps.processors.duckdb.DuckDBAggregateProcessor

### DuckDBJoinProcessor

::: cogapp_deps.processors.duckdb.DuckDBJoinProcessor

---

## Polars Processors

### PolarsFilterProcessor

::: cogapp_deps.processors.polars.PolarsFilterProcessor

### PolarsStringProcessor

::: cogapp_deps.processors.polars.PolarsStringProcessor

---

## Chaining

### Chain

::: cogapp_deps.processors.Chain

Process multiple transformations in sequence with lazy optimization for Polars.

```python
from cogapp_deps.processors import Chain
from cogapp_deps.processors.polars import PolarsFilterProcessor, PolarsStringProcessor

chain = Chain([
    PolarsStringProcessor("name", "upper"),
    PolarsFilterProcessor("price", 1000, ">="),
])

result = chain.process(df)  # Single optimized query
```
