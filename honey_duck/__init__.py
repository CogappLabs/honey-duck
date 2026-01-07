"""DuckDB-backed DataFrame pipeline supporting pandas and polars processors."""

from honey_duck.base import DuckDBProcessor, PolarsProcessor, Processor
from honey_duck.pipeline import DuckDBPipeline
from honey_duck.processors import (
    DuckDBAggregateProcessor,
    DuckDBLookupProcessor,
    DuckDBSQLProcessor,
    PandasFilterProcessor,
    PandasUppercaseProcessor,
    PolarsAggregateProcessor,
    PolarsWindowProcessor,
)

__all__ = [
    # Core
    "DuckDBPipeline",
    "Processor",
    "PolarsProcessor",
    "DuckDBProcessor",
    # Pandas processors
    "PandasFilterProcessor",
    "PandasUppercaseProcessor",
    # Polars processors
    "PolarsAggregateProcessor",
    "PolarsWindowProcessor",
    # DuckDB SQL processors
    "DuckDBSQLProcessor",
    "DuckDBLookupProcessor",
    "DuckDBAggregateProcessor",
]
