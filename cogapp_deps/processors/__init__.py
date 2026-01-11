"""Cogapp data processors organized by engine type.

Processor Selection Guide
-------------------------

┌─────────────────────────┬──────────────────────────────────────────────────┐
│ Use case                │ Processor                                        │
├─────────────────────────┼──────────────────────────────────────────────────┤
│ Query database tables   │ DuckDBQueryProcessor (reads from configured DB)  │
│ Transform DataFrame SQL │ DuckDBSQLProcessor (in-memory, needs df input)   │
│ Multi-table joins       │ DuckDBSQLProcessor with tables={...} parameter   │
│ Window functions        │ DuckDBWindowProcessor or DuckDBSQLProcessor      │
│ Aggregations (GROUP BY) │ DuckDBAggregateProcessor or DuckDBSQLProcessor   │
│ String transforms       │ PolarsStringProcessor (upper, lower, strip)      │
│ Numeric filtering       │ PolarsFilterProcessor (>, <, >=, ==, etc.)       │
│ Chain Polars operations │ Chain([...]) for single optimized query          │
└─────────────────────────┴──────────────────────────────────────────────────┘

Return Types:
    - DuckDB processors return pandas DataFrames
    - Polars processors return Polars DataFrames
    - Chain returns Polars DataFrames
    - The DuckDBPandasPolarsIOManager handles both seamlessly

Examples
--------

Query from database:

    from cogapp_deps.processors.duckdb import DuckDBQueryProcessor, configure
    configure(db_path="data.duckdb", read_only=True)

    df = DuckDBQueryProcessor(sql="SELECT * FROM raw.sales").process()

Transform a DataFrame with SQL:

    from cogapp_deps.processors.duckdb import DuckDBSQLProcessor

    result = DuckDBSQLProcessor(sql=\"\"\"
        SELECT *, price * 1.1 AS price_with_tax FROM _input
    \"\"\").process(df)

Chain Polars processors:

    from cogapp_deps.processors import Chain
    from cogapp_deps.processors.polars import PolarsStringProcessor, PolarsFilterProcessor

    result = Chain([
        PolarsStringProcessor("name", "upper"),
        PolarsFilterProcessor("price", 1000, ">="),
    ]).process(df)  # single optimized query
"""

from . import duckdb
from .chain import Chain

try:
    from . import polars

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

__all__ = [
    "Chain",
    "duckdb",
    "polars",
    "POLARS_AVAILABLE",
]
