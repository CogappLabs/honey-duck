# Processors

Reusable DataFrame transformation classes for data pipelines.

!!! info "External Documentation"
    - **DuckDB**: [duckdb.org/docs](https://duckdb.org/docs/) | [SQL Reference](https://duckdb.org/docs/sql/introduction)
    - **Polars**: [pola.rs/docs](https://docs.pola.rs/) | [API Reference](https://docs.pola.rs/api/python/stable/reference/)

---

## DuckDB Processors

### DuckDBQueryProcessor

::: cogapp_libs.processors.duckdb.DuckDBQueryProcessor

### DuckDBSQLProcessor

::: cogapp_libs.processors.duckdb.DuckDBSQLProcessor

### DuckDBWindowProcessor

::: cogapp_libs.processors.duckdb.DuckDBWindowProcessor

### DuckDBAggregateProcessor

::: cogapp_libs.processors.duckdb.DuckDBAggregateProcessor

### DuckDBJoinProcessor

::: cogapp_libs.processors.duckdb.DuckDBJoinProcessor

---

## Polars Processors

### PolarsFilterProcessor

::: cogapp_libs.processors.polars.PolarsFilterProcessor

### PolarsStringProcessor

::: cogapp_libs.processors.polars.PolarsStringProcessor

---

## Chaining

### Chain

::: cogapp_libs.processors.Chain

Process multiple transformations in sequence with lazy optimization for Polars.

```python
from cogapp_libs.processors import Chain
from cogapp_libs.processors.polars import PolarsFilterProcessor, PolarsStringProcessor

chain = Chain([
    PolarsStringProcessor("name", "upper"),
    PolarsFilterProcessor("price", 1000, ">="),
])

result = chain.process(df)  # Single optimized query
```
