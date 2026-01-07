"""Built-in processors."""

import pandas as pd
import polars as pl

from honey_duck.base import DuckDBProcessor, PolarsProcessor, Processor


# --- Pandas ---


class PandasUppercaseProcessor(Processor):
    def __init__(self, column: str):
        self.column = column

    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        data[self.column] = data[self.column].str.upper()
        return data


class PandasFilterProcessor(Processor):
    def __init__(self, column: str, min_value: float):
        self.column = column
        self.min_value = min_value

    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        return data[data[self.column] >= self.min_value]


# --- Polars ---


class PolarsAggregateProcessor(PolarsProcessor):
    def __init__(self, group_by: str, agg_column: str):
        self.group_by = group_by
        self.agg_column = agg_column

    def process(self, data: pl.DataFrame) -> pl.DataFrame:
        return data.group_by(self.group_by).agg(
            pl.col(self.agg_column).sum().alias(f"{self.agg_column}_total"),
            pl.col(self.agg_column).mean().alias(f"{self.agg_column}_mean"),
            pl.len().alias("count"),
        )


class PolarsWindowProcessor(PolarsProcessor):
    def __init__(self, partition_by: str, order_by: str):
        self.partition_by = partition_by
        self.order_by = order_by

    def process(self, data: pl.DataFrame) -> pl.DataFrame:
        return data.with_columns(
            pl.col(self.order_by).rank().over(self.partition_by).alias("rank_in_group")
        )


# --- DuckDB SQL ---


class DuckDBSQLProcessor(DuckDBProcessor):
    def __init__(self, sql: str):
        self.sql = sql

    def get_sql(self) -> str:
        return self.sql


class DuckDBLookupProcessor(DuckDBProcessor):
    def __init__(
        self,
        lookup_table: str,
        left_on: str,
        right_on: str,
        columns: list[str] | None = None,
        how: str = "LEFT",
    ):
        self.lookup_table = lookup_table
        self.left_on = left_on
        self.right_on = right_on
        self.columns = columns
        self.how = how

    def get_sql(self) -> str:
        if self.columns:
            lookup_cols = ", ".join(f"l.{c}" for c in self.columns)
            select = f"p.*, {lookup_cols}"
        else:
            select = "p.*, l.*"

        return f"""
            SELECT {select}
            FROM pipeline_data p
            {self.how} JOIN {self.lookup_table} l
            ON p.{self.left_on} = l.{self.right_on}
        """


class DuckDBAggregateProcessor(DuckDBProcessor):
    def __init__(self, group_by: list[str], aggregations: dict[str, str]):
        self.group_by = group_by
        self.aggregations = aggregations

    def get_sql(self) -> str:
        group_cols = ", ".join(self.group_by)
        agg_exprs = ", ".join(
            f"{expr} AS {name}" for name, expr in self.aggregations.items()
        )
        return f"""
            SELECT {group_cols}, {agg_exprs}
            FROM pipeline_data
            GROUP BY {group_cols}
        """
