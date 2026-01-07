"""DuckDB-backed DataFrame pipeline."""

import gc

import duckdb
import pandas as pd

from honey_duck.base import DuckDBProcessor, PolarsProcessor, Processor


class DuckDBPipeline:
    """
    Pipeline that uses DuckDB for intermediate storage.

    - Stores data in DuckDB between processor steps
    - Loads as pandas or polars depending on processor type
    - DuckDBProcessor runs SQL directly without loading data
    """

    def __init__(self, duckdb_path: str = ":memory:"):
        self.con = duckdb.connect(duckdb_path)
        self.table_name = "pipeline_data"
        self.processors: list[Processor | PolarsProcessor | DuckDBProcessor] = []

    # --- Harvest ---

    def harvest_csv(self, path: str, table_name: str | None = None) -> "DuckDBPipeline":
        target = table_name or self.table_name
        self.con.execute(
            f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM read_csv_auto('{path}')"
        )
        return self

    def harvest_json(self, path: str, table_name: str | None = None) -> "DuckDBPipeline":
        target = table_name or self.table_name
        self.con.execute(
            f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM read_json_auto('{path}')"
        )
        return self

    def harvest_parquet(self, path: str, table_name: str | None = None) -> "DuckDBPipeline":
        target = table_name or self.table_name
        self.con.execute(
            f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM read_parquet('{path}')"
        )
        return self

    # --- Processors ---

    def add_processor(
        self, processor: Processor | PolarsProcessor | DuckDBProcessor
    ) -> "DuckDBPipeline":
        self.processors.append(processor)
        return self

    def run(self) -> pd.DataFrame:
        for processor in self.processors:
            if isinstance(processor, DuckDBProcessor):
                sql = processor.get_sql()
                self.con.execute(
                    f"CREATE OR REPLACE TABLE {self.table_name} AS {sql}"
                )
            elif isinstance(processor, PolarsProcessor):
                df = self.con.execute(f"SELECT * FROM {self.table_name}").pl()
                result = processor.process(df)
                self.con.execute(
                    f"CREATE OR REPLACE TABLE {self.table_name} AS SELECT * FROM result"
                )
                del df, result
                gc.collect()
            else:
                df = self.con.execute(f"SELECT * FROM {self.table_name}").fetchdf()
                result = processor.process(df)
                self.con.execute(
                    f"CREATE OR REPLACE TABLE {self.table_name} AS SELECT * FROM result"
                )
                del df, result
                gc.collect()

        return self.con.execute(f"SELECT * FROM {self.table_name}").fetchdf()

    # --- Output ---

    def output_json(self, path: str, array: bool = True) -> None:
        fmt = "ARRAY true" if array else "ARRAY false"
        self.con.execute(
            f"COPY {self.table_name} TO '{path}' (FORMAT JSON, {fmt})"
        )

    def output_csv(self, path: str, header: bool = True) -> None:
        hdr = "HEADER true" if header else "HEADER false"
        self.con.execute(f"COPY {self.table_name} TO '{path}' (FORMAT CSV, {hdr})")

    def output_parquet(self, path: str) -> None:
        self.con.execute(f"COPY {self.table_name} TO '{path}' (FORMAT PARQUET)")

    # --- Debug ---

    def query(self, sql: str) -> pd.DataFrame:
        return self.con.execute(sql).fetchdf()

    def tables(self) -> list[str]:
        return [row[0] for row in self.con.execute("SHOW TABLES").fetchall()]
