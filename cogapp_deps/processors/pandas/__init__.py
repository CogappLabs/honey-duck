"""Pandas-based DataFrame processors.

All processors in this module operate on pandas DataFrames and return
modified DataFrames. They handle in-memory transformations efficiently.

For string transforms and filtering, prefer Polars processors (faster, chainable).
For joins and aggregations, prefer DuckDB processors.

Processors:
    - PandasReplaceOnConditionProcessor: Replace values based on conditions
"""

from .replace_on_condition import PandasReplaceOnConditionProcessor

__all__ = [
    "PandasReplaceOnConditionProcessor",
]
