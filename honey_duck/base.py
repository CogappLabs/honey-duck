"""Base processor classes."""

from abc import ABC, abstractmethod

import pandas as pd
import polars as pl


class Processor(ABC):
    """Base processor - takes a pandas DataFrame, returns a pandas DataFrame."""

    @abstractmethod
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        pass


class PolarsProcessor(ABC):
    """Base processor for polars - takes a polars DataFrame, returns a polars DataFrame."""

    @abstractmethod
    def process(self, data: pl.DataFrame) -> pl.DataFrame:
        pass


class DuckDBProcessor(ABC):
    """
    Base processor that runs SQL directly on DuckDB.

    No data loading/unloading - operates directly on the pipeline's DuckDB connection.
    The SQL should read from 'pipeline_data' and the result replaces it.
    """

    @abstractmethod
    def get_sql(self) -> str:
        """Return the SQL query to execute. Should SELECT from pipeline_data."""
        pass
