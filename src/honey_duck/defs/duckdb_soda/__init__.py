"""DuckDB + Soda implementation of honey-duck pipeline.

This module implements the full pipeline using:
- DuckDB for all transformations (SQL-based, memory-efficient)
- Soda Core for data validation (SQL-based, no data in Python memory)
- Parquet IO manager for intermediate storage

Key Features:
- Pure SQL transformations (reuses existing DuckDB queries)
- Soda contract YAML files define data quality expectations
- Validation runs as SQL queries against DuckDB
- Memory-efficient: DuckDB handles spill-to-disk

Asset Graph:
    dlt_harvest_* (shared) --> sales_transform_soda --> sales_output_soda
                           └--> artworks_transform_soda --> artworks_output_soda

Soda Checks (blocking):
    - check_sales_transform_soda: Validates sales transform schema
    - check_artworks_transform_soda: Validates artworks transform schema
"""

from .assets import (
    artworks_output_soda,
    artworks_transform_soda,
    sales_output_soda,
    sales_transform_soda,
)
from .checks import (
    check_artworks_transform_soda,
    check_sales_transform_soda,
)

__all__ = [
    # Assets
    "sales_transform_soda",
    "sales_output_soda",
    "artworks_transform_soda",
    "artworks_output_soda",
    # Checks
    "check_sales_transform_soda",
    "check_artworks_transform_soda",
]
