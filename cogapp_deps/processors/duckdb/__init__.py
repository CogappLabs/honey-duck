"""DuckDB SQL processors for efficient data transformations.

All processors in this module generate or execute DuckDB SQL queries.
DuckDB provides excellent performance for analytical queries and joins.

Processors:
    - DuckDBSQLProcessor: Execute raw SQL queries with table registration
    - DuckDBJoinProcessor: Multi-table joins with computed columns
    - DuckDBWindowProcessor: Window functions (ranking, running totals)
    - DuckDBAggregateProcessor: Group-by aggregations
"""

from .aggregate import DuckDBAggregateProcessor
from .join import DuckDBJoinProcessor
from .sql import DuckDBSQLProcessor
from .window import DuckDBWindowProcessor

__all__ = [
    "DuckDBAggregateProcessor",
    "DuckDBJoinProcessor",
    "DuckDBSQLProcessor",
    "DuckDBWindowProcessor",
]
