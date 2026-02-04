#!/usr/bin/env python3
"""Test script for processor equivalents.

Runs Honeysuckle processors alongside their Polars/DuckDB equivalents
to verify they produce matching outputs.

Usage:
    uv run scripts/test_processor_equivalents.py
    uv run scripts/test_processor_equivalents.py --verbose
    uv run scripts/test_processor_equivalents.py --processor FillEmptyProcessor

Environment:
    HONEYSUCKLE_PATH: Path to honeysuckle repo (default: ~/git/honeysuckle)
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import duckdb
import pandas as pd
import polars as pl

# ---------------------------------------------------------------------------
# Honeysuckle Import Setup
# ---------------------------------------------------------------------------

# Minimal dependencies required for Honeysuckle processors used in tests
# (not included in honey-duck's pyproject.toml)
HONEYSUCKLE_DEPS = ["natsort"]

HONEYSUCKLE_PATH = Path(os.environ.get("HONEYSUCKLE_PATH", Path.home() / "git" / "honeysuckle"))


def _ensure_honeysuckle_deps() -> bool:
    """Ensure Honeysuckle dependencies are installed. Returns True if successful."""
    missing = []
    for dep in HONEYSUCKLE_DEPS:
        try:
            __import__(dep)
        except ImportError:
            missing.append(dep)

    if not missing:
        return True

    print(f"Installing Honeysuckle dependencies: {', '.join(missing)}")
    try:
        # Use uv pip install to install into the current environment
        subprocess.run(
            ["uv", "pip", "install", *missing],
            check=True,
            capture_output=True,
            text=True,
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"Failed to install dependencies: {e.stderr}")
        return False
    except FileNotFoundError:
        # uv not available, try pip
        try:
            subprocess.run(
                [sys.executable, "-m", "pip", "install", *missing],
                check=True,
                capture_output=True,
                text=True,
            )
            return True
        except subprocess.CalledProcessError as e:
            print(f"Failed to install dependencies: {e.stderr}")
            return False


if HONEYSUCKLE_PATH.exists():
    sys.path.insert(0, str(HONEYSUCKLE_PATH))
    HONEYSUCKLE_AVAILABLE = _ensure_honeysuckle_deps()
    if HONEYSUCKLE_AVAILABLE:
        try:
            from honeysuckle.components.processors.fill_empty_processor import FillEmptyProcessor
            from honeysuckle.components.processors.remove_multivalue_nulls_processor import (
                RemoveMultivalueNullsProcessor,
            )
            from honeysuckle.components.processors.strip_string_processor import (
                StripStringProcessor,
            )
            from honeysuckle.components.processors.extract_first_processor import (
                ExtractFirstProcessor,
            )
            from honeysuckle.components.processors.extract_on_condition_processor import (
                ExtractOnConditionProcessor,
            )
            from honeysuckle.components.processors.contains_bool_processor import (
                ContainsBoolProcessor,
            )
            from honeysuckle.components.processors.string_constant_dataframe_processor import (
                StringConstantDataframeProcessor,
            )
            from honeysuckle.components.processors.append_on_condition_processor import (
                AppendOnConditionProcessor,
            )

            from honeysuckle.components.processors.implode_processor import ImplodeProcessor
            from honeysuckle.components.processors.concat_processor import ConcatProcessor
            from honeysuckle.components.processors.rename_processor import RenameProcessor
            from honeysuckle.components.processors.is_empty_processor import IsEmptyProcessor
            from honeysuckle.components.processors.explode_columns_processor import (
                ExplodeColumnsProcessor,
            )
            from honeysuckle.components.processors.replace_on_condition_processor import (
                ReplaceOnConditionProcessor,
            )
            from honeysuckle.components.processors.lower_string_processor import (
                LowerStringProcessor,
            )
            from honeysuckle.components.processors.keep_only_processor import KeepOnlyProcessor
            from honeysuckle.components.processors.drop_null_columns_processor import (
                DropNullColumnsProcessor,
            )
            from honeysuckle.components.processors.drop_columns_processor import (
                DropColumnsProcessor,
            )
            from honeysuckle.components.processors.copy_field_processor import (
                CopyFieldDataframeProcessor,
            )
            from honeysuckle.components.processors.replace_processor import ReplaceProcessor
            from honeysuckle.components.processors.merge_processor import MergeProcessor
            from honeysuckle.components.processors.fill_missing_tuples_processor import (
                FillMissingTuplesProcessor,
            )
            from honeysuckle.components.processors.conditional_bool_processor import (
                ConditionalBoolProcessor,
            )
            from honeysuckle.components.processors.capitalize_processor import CapitalizeProcessor
            from honeysuckle.components.processors.append_string_processor import (
                AppendStringProcessor,
            )
            from honeysuckle.components.processors.add_number_processor import AddNumberProcessor
            from honeysuckle.components.processors.subtract_number_processor import (
                SubtractNumberProcessor,
            )
            from honeysuckle.components.processors.split_string_processor import (
                SplitStringProcessor,
            )
            from honeysuckle.components.processors.as_type_processor import AsTypeProcessor
            from honeysuckle.components.processors.drop_rows_on_condition_processor import (
                DropRowsOnConditionProcessor,
            )
            from honeysuckle.components.post_validation_processors.columns_to_dicts_processor import (
                ColumnsToDictsProcessor,
            )
        except ImportError as e:
            print(f"Warning: Could not import some Honeysuckle processors: {e}")
            HONEYSUCKLE_AVAILABLE = False
else:
    HONEYSUCKLE_AVAILABLE = False
    print(f"Warning: Honeysuckle not found at {HONEYSUCKLE_PATH}")
    print("Set HONEYSUCKLE_PATH environment variable to the honeysuckle repo path")


# ---------------------------------------------------------------------------
# Test Result Types
# ---------------------------------------------------------------------------


@dataclass
class ProcessorTestResult:
    """Result of a single processor test."""

    name: str
    passed: bool
    honeysuckle_output: Any = None
    polars_output: Any = None
    duckdb_output: Any = None
    error: str | None = None


# ---------------------------------------------------------------------------
# Test Fixtures
# ---------------------------------------------------------------------------

# --8<-- [start:fill_empty_data]
FILL_EMPTY_DATA = {
    "title": [None, "Main Title", None],
    "alt_title": ["Backup", "Alt", None],
    "price": [None, 100, 50],
}
# --8<-- [end:fill_empty_data]

# --8<-- [start:remove_nulls_data]
REMOVE_NULLS_DATA = {
    "tags": [["a", None, "b"], [None, None], ["x", "y"]],
}
# --8<-- [end:remove_nulls_data]

# --8<-- [start:strip_string_data]
STRIP_STRING_DATA = {
    "name": ["  hello  ", "\tworld\n", "no_space"],
}
# --8<-- [end:strip_string_data]

# --8<-- [start:extract_first_data]
EXTRACT_FIRST_DATA = {
    "authors": [["Alice", "Bob"], ["Charlie"], []],
}
# --8<-- [end:extract_first_data]

# --8<-- [start:extract_on_condition_data]
EXTRACT_ON_CONDITION_DATA = {
    "price": [100, 200, 150],
    "is_on_sale": [True, False, True],
}
# --8<-- [end:extract_on_condition_data]

# --8<-- [start:contains_bool_data]
CONTAINS_BOOL_DATA = {
    "description": ["This is important", "Nothing here", "Also important stuff"],
}
# --8<-- [end:contains_bool_data]

# --8<-- [start:string_constant_data]
STRING_CONSTANT_DATA = {
    "id": [1, 2, 3],
}
# --8<-- [end:string_constant_data]

# --8<-- [start:append_on_condition_data]
APPEND_ON_CONDITION_DATA = {
    "tags": [("art",), ("photo",)],
    "category": ["new", "old"],
    "is_featured": [True, False],
}
# --8<-- [end:append_on_condition_data]

# --8<-- [start:implode_data]
IMPLODE_DATA = {
    "artwork_id": [1, 1, 2, 2, 2],
    "tag": ["oil", "portrait", "landscape", "nature", "photo"],
}
# --8<-- [end:implode_data]

# --8<-- [start:concat_data]
CONCAT_DATA = {
    "first": ["John", "Jane", None],
    "last": ["Doe", "Smith", "Unknown"],
}
# --8<-- [end:concat_data]

# --8<-- [start:rename_data]
RENAME_DATA = {
    "old_name": ["value1", "value2"],
    "title": ["Art", "Photo"],
}
# --8<-- [end:rename_data]

# --8<-- [start:is_empty_data]
IS_EMPTY_DATA = {
    "email": [None, "a@b.com", None, "x@y.com"],
}
# --8<-- [end:is_empty_data]

# --8<-- [start:explode_data]
EXPLODE_DATA = {
    "id": [1, 2],
    "tags": [["a", "b"], ["x", "y", "z"]],
}
# --8<-- [end:explode_data]

# --8<-- [start:replace_on_condition_data]
REPLACE_ON_CONDITION_DATA = {
    "status": ["", "active", ""],
    "price": [-10, 50, -5],
}
# --8<-- [end:replace_on_condition_data]

# --8<-- [start:lower_string_data]
LOWER_STRING_DATA = {
    "email": ["John@Example.COM", "JANE@TEST.ORG", "mixed@Case.Net"],
}
# --8<-- [end:lower_string_data]

# --8<-- [start:keep_only_data]
KEEP_ONLY_DATA = {
    "id": [1, 2],
    "title": ["Art", "Photo"],
    "author": ["Jane", "John"],
    "internal_code": ["X1", "X2"],
}
# --8<-- [end:keep_only_data]

# --8<-- [start:drop_null_columns_data]
DROP_NULL_COLUMNS_DATA = {
    "id": [1, 2],
    "empty_col": [None, None],
    "title": ["Art", "Photo"],
}
# --8<-- [end:drop_null_columns_data]

# --8<-- [start:drop_columns_data]
DROP_COLUMNS_DATA = {
    "id": [1, 2],
    "internal_id": ["abc", "def"],
    "title": ["Art", "Photo"],
}
# --8<-- [end:drop_columns_data]

# --8<-- [start:copy_field_data]
COPY_FIELD_DATA = {
    "original_title": ["Mona Lisa", "Starry Night"],
}
# --8<-- [end:copy_field_data]

# --8<-- [start:replace_data]
REPLACE_DATA = {
    "status": ["", "active", ""],
}
# --8<-- [end:replace_data]

# --8<-- [start:merge_data]
MERGE_SALES_DATA = {
    "id": [1, 2, 3],
    "artist_id": [100, 101, 100],
}
MERGE_ARTISTS_DATA = {
    "id": [100, 101],
    "artist_name": ["Monet", "Van Gogh"],
}
# --8<-- [end:merge_data]

# --8<-- [start:fill_missing_tuples_data]
FILL_MISSING_TUPLES_DATA = {
    "names": [("Alice", "Bob"), None],
    "roles": [None, ("editor", "viewer")],
}
# --8<-- [end:fill_missing_tuples_data]

# --8<-- [start:conditional_bool_data]
CONDITIONAL_BOOL_DATA = {
    "price": [1500, 500, 2000],
    "status": ["active", "active", "inactive"],
}
# --8<-- [end:conditional_bool_data]

# --8<-- [start:capitalize_data]
CAPITALIZE_DATA = {
    "title": ["hello world", "UPPERCASE", "mIxEd CaSe"],
}
# --8<-- [end:capitalize_data]

# --8<-- [start:append_string_data]
APPEND_STRING_DATA = {
    "id": ["123", "456"],
    "filename": ["image", "photo"],
}
# --8<-- [end:append_string_data]

# --8<-- [start:add_number_data]
ADD_NUMBER_DATA = {
    "year": [2024, 2023, 2022],
}
# --8<-- [end:add_number_data]

# --8<-- [start:subtract_number_data]
SUBTRACT_NUMBER_DATA = {
    "year": [2024, 2023, 2022],
}
# --8<-- [end:subtract_number_data]

# --8<-- [start:split_string_data]
SPLIT_STRING_DATA = {
    "tags": ["a,b,c", "x,y", "single"],
}
# --8<-- [end:split_string_data]

# --8<-- [start:as_type_data]
AS_TYPE_DATA = {
    "year": ["2024", "2023", "2022"],
    "price": ["99.99", "49.50", "10.00"],
}
# --8<-- [end:as_type_data]

# --8<-- [start:drop_rows_data]
DROP_ROWS_DATA = {
    "status": ["active", "deleted", "active", "deleted"],
    "price": [100, 50, -10, 200],
}
# --8<-- [end:drop_rows_data]

# --8<-- [start:columns_to_dicts_data]
COLUMNS_TO_DICTS_DATA = {
    "width": [10, 15],
    "height": [20, 30],
    "depth": [5, 8],
}
# --8<-- [end:columns_to_dicts_data]


# ---------------------------------------------------------------------------
# Polars Equivalents
# ---------------------------------------------------------------------------


# --8<-- [start:fill_empty_polars]
def fill_empty_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Fill nulls: title from alt_title, price with 0."""
    return df.with_columns(
        pl.col("title").fill_null(pl.col("alt_title")),
        pl.col("price").fill_null(0),
    )


# --8<-- [end:fill_empty_polars]


def remove_nulls_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Remove null values from list column."""
    # --8<-- [start:remove_nulls_polars]
    # Step 1: Remove nulls from list
    df = df.with_columns(pl.col("tags").list.eval(pl.element().drop_nulls()).alias("tags"))
    # Step 2: Convert empty lists to null
    return df.with_columns(
        pl.when(pl.col("tags").list.len() == 0).then(None).otherwise(pl.col("tags")).alias("tags")
    )
    # --8<-- [end:remove_nulls_polars]


# --8<-- [start:strip_string_polars]
def strip_string_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Strip whitespace from strings."""
    return df.with_columns(pl.col("name").str.strip_chars().alias("name_clean"))


# --8<-- [end:strip_string_polars]


# --8<-- [start:extract_first_polars]
def extract_first_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Extract first element from list."""
    return df.with_columns(pl.col("authors").list.first().alias("primary_author"))


# --8<-- [end:extract_first_polars]


# --8<-- [start:extract_on_condition_polars]
def extract_on_condition_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Extract price to sale_price when is_on_sale is True."""
    return df.with_columns(
        pl.when(pl.col("is_on_sale")).then(pl.col("price")).otherwise(None).alias("sale_price")
    )


# --8<-- [end:extract_on_condition_polars]


# --8<-- [start:contains_bool_polars]
def contains_bool_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Check if description contains 'important'."""
    return df.with_columns(pl.col("description").str.contains("important").alias("has_keyword"))


# --8<-- [end:contains_bool_polars]


# --8<-- [start:string_constant_polars]
def string_constant_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Add constant 'source' column."""
    return df.with_columns(pl.lit("huntington").alias("source"))


# --8<-- [end:string_constant_polars]


def append_on_condition_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Append category to tags when featured."""
    # --8<-- [start:append_on_condition_polars]
    # Step 1: Wrap scalar in list
    df = df.with_columns(pl.concat_list(pl.col("category")).alias("category_list"))
    # Step 2: Conditionally append
    return df.with_columns(
        pl.when(pl.col("is_featured"))
        .then(pl.col("tags").list.concat(pl.col("category_list")))
        .otherwise(pl.col("tags"))
        .alias("tags")
    ).drop("category_list")
    # --8<-- [end:append_on_condition_polars]


# --8<-- [start:implode_polars]
def implode_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate tags into list per artwork_id."""
    return df.group_by("artwork_id").agg(pl.col("tag").alias("tag")).sort("artwork_id")


# --8<-- [end:implode_polars]


# --8<-- [start:concat_polars]
def concat_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Concatenate first and last name with space."""
    return df.with_columns(
        pl.concat_str(
            [
                pl.col("first").fill_null("None"),
                pl.col("last").fill_null("None"),
            ],
            separator=" ",
            ignore_nulls=False,
        ).alias("full_name")
    )


# --8<-- [end:concat_polars]


# --8<-- [start:rename_polars]
def rename_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Rename columns."""
    return df.rename({"old_name": "new_name", "title": "artwork_title"})


# --8<-- [end:rename_polars]


# --8<-- [start:is_empty_polars]
def is_empty_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Check if email is null."""
    return df.with_columns(
        pl.col("email").is_null().alias("missing_email"),
        pl.col("email").is_not_null().alias("has_email"),
    )


# --8<-- [end:is_empty_polars]


# --8<-- [start:explode_polars]
def explode_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Explode tags list into rows."""
    return df.explode("tags")


# --8<-- [end:explode_polars]


# --8<-- [start:replace_on_condition_polars]
def replace_on_condition_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Replace empty status with 'unknown', negative price with 0."""
    return df.with_columns(
        pl.when(pl.col("status") == "")
        .then(pl.lit("unknown"))
        .otherwise(pl.col("status"))
        .alias("status"),
        pl.max_horizontal(pl.col("price"), pl.lit(0)).alias("price"),
    )


# --8<-- [end:replace_on_condition_polars]


# --8<-- [start:lower_string_polars]
def lower_string_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Convert email to lowercase."""
    return df.with_columns(pl.col("email").str.to_lowercase())


# --8<-- [end:lower_string_polars]


# --8<-- [start:keep_only_polars]
def keep_only_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Keep only id and title columns."""
    return df.select(["id", "title"])


# --8<-- [end:keep_only_polars]


# --8<-- [start:drop_null_columns_polars]
def drop_null_columns_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Drop columns where all values are null."""
    null_cols = [col for col in df.columns if df[col].is_null().all()]
    return df.drop(null_cols)


# --8<-- [end:drop_null_columns_polars]


# --8<-- [start:drop_columns_polars]
def drop_columns_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Drop internal_id column."""
    return df.drop(["internal_id"])


# --8<-- [end:drop_columns_polars]


# --8<-- [start:copy_field_polars]
def copy_field_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Copy original_title to display_title."""
    return df.with_columns(pl.col("original_title").alias("display_title"))


# --8<-- [end:copy_field_polars]


# --8<-- [start:replace_polars]
def replace_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Replace exact empty status with 'unknown'."""
    return df.with_columns(
        pl.when(pl.col("status") == "")
        .then(pl.lit("unknown"))
        .otherwise(pl.col("status"))
        .alias("status")
    )


# --8<-- [end:replace_polars]


def merge_polars(sales: pl.DataFrame, artists: pl.DataFrame) -> pl.DataFrame:
    """Left join sales with artists."""
    # --8<-- [start:merge_polars]
    # Use suffix parameter instead of manual id_y workaround
    merged = sales.join(
        artists.select(["id", "artist_name"]),
        left_on="artist_id",
        right_on="id",
        how="left",
        suffix="_y",
    )
    return merged.rename({"id": "id_x"})
    # --8<-- [end:merge_polars]


# --8<-- [start:fill_missing_tuples_polars]
def fill_missing_tuples_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Fill missing tuple columns using list literals."""
    return df.with_columns(
        pl.when(pl.col("names").is_null())
        .then(pl.lit([None, None]))
        .otherwise(pl.col("names"))
        .alias("names"),
        pl.when(pl.col("roles").is_null())
        .then(pl.lit([None, None]))
        .otherwise(pl.col("roles"))
        .alias("roles"),
    )


# --8<-- [end:fill_missing_tuples_polars]


# --8<-- [start:conditional_bool_polars]
def conditional_bool_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Set is_premium based on price > 1000 and status == 'active'."""
    return df.with_columns(
        ((pl.col("price") > 1000) & (pl.col("status") == "active")).alias("is_premium")
    )


# --8<-- [end:conditional_bool_polars]


def capitalize_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Capitalize first character of title."""
    # --8<-- [start:capitalize_polars]
    # Step 1: Get first char and rest
    df = df.with_columns(
        pl.col("title").str.head(1).str.to_uppercase().alias("_first"),
        pl.col("title").str.slice(1).str.to_lowercase().alias("_rest"),
    )
    # Step 2: Combine and clean up
    return df.with_columns((pl.col("_first") + pl.col("_rest")).alias("title")).drop(
        ["_first", "_rest"]
    )
    # --8<-- [end:capitalize_polars]


# --8<-- [start:append_string_polars]
def append_string_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Prepend 'ID-' to id, append '.jpg' to filename."""
    return df.with_columns(
        (pl.lit("ID-") + pl.col("id")).alias("id"),
        (pl.col("filename") + pl.lit(".jpg")).alias("filename"),
    )


# --8<-- [end:append_string_polars]


# --8<-- [start:add_number_polars]
def add_number_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Add 1 to year."""
    return df.with_columns((pl.col("year") + 1).alias("next_year"))


# --8<-- [end:add_number_polars]


# --8<-- [start:subtract_number_polars]
def subtract_number_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Subtract 1 from year."""
    return df.with_columns((pl.col("year") - 1).alias("prev_year"))


# --8<-- [end:subtract_number_polars]


# --8<-- [start:split_string_polars]
def split_string_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Split tags by comma."""
    return df.with_columns(pl.col("tags").str.split(","))


# --8<-- [end:split_string_polars]


# --8<-- [start:as_type_polars]
def as_type_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Cast year to int, price to float."""
    return df.with_columns(
        pl.col("year").cast(pl.Int64),
        pl.col("price").cast(pl.Float64),
    )


# --8<-- [end:as_type_polars]


# --8<-- [start:drop_rows_polars]
def drop_rows_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Drop rows where status == 'deleted' or price < 0."""
    return df.filter((pl.col("status") != "deleted") & (pl.col("price") >= 0))


# --8<-- [end:drop_rows_polars]


# --8<-- [start:columns_to_dicts_polars]
def columns_to_dicts_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Combine width, height, depth into dimensions struct list."""
    return df.with_columns(
        pl.concat_list(
            pl.struct(
                pl.col("width").alias("w"),
                pl.col("height").alias("h"),
                pl.col("depth").alias("d"),
            )
        ).alias("dimensions")
    ).drop(["width", "height", "depth"])


# --8<-- [end:columns_to_dicts_polars]


# ---------------------------------------------------------------------------
# DuckDB SQL Queries (source of truth for documentation)
# ---------------------------------------------------------------------------

FILL_EMPTY_SQL = """
-- --8<-- [start:fill_empty_sql]
SELECT
    coalesce(title, alt_title) AS title,
    alt_title,
    ifnull(price, 0) AS price
-- --8<-- [end:fill_empty_sql]
FROM 'source.parquet'
"""

REMOVE_NULLS_SQL = """
-- --8<-- [start:remove_nulls_sql]
SELECT nullif(list_filter(tags, x -> x IS NOT NULL), []) AS tags
-- --8<-- [end:remove_nulls_sql]
FROM 'source.parquet'
"""

STRIP_STRING_SQL = """
-- --8<-- [start:strip_string_sql]
SELECT name, name.trim() AS name_clean
-- --8<-- [end:strip_string_sql]
FROM 'source.parquet'
"""

EXTRACT_FIRST_SQL = """
-- --8<-- [start:extract_first_sql]
SELECT authors, authors[1] AS primary_author
-- --8<-- [end:extract_first_sql]
FROM 'source.parquet'
"""

EXTRACT_ON_CONDITION_SQL = """
-- --8<-- [start:extract_on_condition_sql]
SELECT price, is_on_sale, if(is_on_sale, price, NULL) AS sale_price
-- --8<-- [end:extract_on_condition_sql]
FROM 'source.parquet'
"""

CONTAINS_BOOL_SQL = """
-- --8<-- [start:contains_bool_sql]
SELECT description, description.contains('important') AS has_keyword
-- --8<-- [end:contains_bool_sql]
FROM 'source.parquet'
"""

STRING_CONSTANT_SQL = """
-- --8<-- [start:string_constant_sql]
SELECT *, 'huntington' AS source
-- --8<-- [end:string_constant_sql]
FROM 'source.parquet'
"""

APPEND_ON_CONDITION_SQL = """
-- --8<-- [start:append_on_condition_sql]
SELECT
    CASE WHEN is_featured
        THEN list_concat(tags, [category])
        ELSE tags
    END AS tags,
    category,
    is_featured
-- --8<-- [end:append_on_condition_sql]
FROM 'source.parquet'
"""

# Implode needs FROM for GROUP BY context
IMPLODE_SQL = """
-- --8<-- [start:implode_sql]
SELECT artwork_id, list(tag) AS tag
FROM 'source.parquet'
GROUP BY ALL
ORDER BY artwork_id
-- --8<-- [end:implode_sql]
"""

CONCAT_SQL = """
-- --8<-- [start:concat_sql]
SELECT
    first,
    last,
    concat(coalesce(first::VARCHAR, 'None'), ' ', coalesce(last::VARCHAR, 'None')) AS full_name
-- --8<-- [end:concat_sql]
FROM 'source.parquet'
"""

RENAME_SQL = """
-- --8<-- [start:rename_sql]
SELECT old_name AS new_name, title AS artwork_title
-- --8<-- [end:rename_sql]
FROM 'source.parquet'
"""

IS_EMPTY_SQL = """
-- --8<-- [start:is_empty_sql]
SELECT
    email,
    email IS NULL AS missing_email,
    email IS NOT NULL AS has_email
-- --8<-- [end:is_empty_sql]
FROM 'source.parquet'
"""

EXPLODE_SQL = """
-- --8<-- [start:explode_sql]
SELECT id, unnest(tags) AS tags
-- --8<-- [end:explode_sql]
FROM 'source.parquet'
"""

REPLACE_ON_CONDITION_SQL = """
-- --8<-- [start:replace_on_condition_sql]
SELECT
    if(status = '', 'unknown', status) AS status,
    greatest(price, 0) AS price
-- --8<-- [end:replace_on_condition_sql]
FROM 'source.parquet'
"""

LOWER_STRING_SQL = """
-- --8<-- [start:lower_string_sql]
SELECT email.lower() AS email
-- --8<-- [end:lower_string_sql]
FROM 'source.parquet'
"""

KEEP_ONLY_SQL = """
-- --8<-- [start:keep_only_sql]
SELECT id, title
-- --8<-- [end:keep_only_sql]
FROM 'source.parquet'
"""

DROP_NULL_COLUMNS_SQL = """
-- --8<-- [start:drop_null_columns_sql]
-- Must enumerate non-null columns manually
SELECT id, title
-- --8<-- [end:drop_null_columns_sql]
FROM 'source.parquet'
"""

DROP_COLUMNS_SQL = """
-- --8<-- [start:drop_columns_sql]
SELECT * EXCLUDE (internal_id)
-- --8<-- [end:drop_columns_sql]
FROM 'source.parquet'
"""

COPY_FIELD_SQL = """
-- --8<-- [start:copy_field_sql]
SELECT *, original_title AS display_title
-- --8<-- [end:copy_field_sql]
FROM 'source.parquet'
"""

REPLACE_SQL = """
-- --8<-- [start:replace_sql]
SELECT CASE WHEN status = '' THEN 'unknown' ELSE status END AS status
-- --8<-- [end:replace_sql]
FROM 'source.parquet'
"""

# Merge needs FROM for JOIN syntax
MERGE_SQL = """
-- --8<-- [start:merge_sql]
SELECT s.id AS id_x, s.artist_id, a.artist_name, a.id AS id_y
FROM 'sales.parquet' s
LEFT JOIN 'artists.parquet' a ON s.artist_id = a.id
-- --8<-- [end:merge_sql]
"""

FILL_MISSING_TUPLES_SQL = """
-- --8<-- [start:fill_missing_tuples_sql]
SELECT
    coalesce(names, [NULL, NULL]) AS names,
    coalesce(roles, [NULL, NULL]) AS roles
-- --8<-- [end:fill_missing_tuples_sql]
FROM 'source.parquet'
"""

CONDITIONAL_BOOL_SQL = """
-- --8<-- [start:conditional_bool_sql]
SELECT *, (price > 1000 AND status = 'active') AS is_premium
-- --8<-- [end:conditional_bool_sql]
FROM 'source.parquet'
"""

CAPITALIZE_SQL = """
-- --8<-- [start:capitalize_sql]
SELECT upper(title[1]) || lower(title[2:]) AS title
-- --8<-- [end:capitalize_sql]
FROM 'source.parquet'
"""

APPEND_STRING_SQL = """
-- --8<-- [start:append_string_sql]
SELECT
    'ID-' || id AS id,
    filename || '.jpg' AS filename
-- --8<-- [end:append_string_sql]
FROM 'source.parquet'
"""

ADD_NUMBER_SQL = """
-- --8<-- [start:add_number_sql]
SELECT year, year + 1 AS next_year
-- --8<-- [end:add_number_sql]
FROM 'source.parquet'
"""

SUBTRACT_NUMBER_SQL = """
-- --8<-- [start:subtract_number_sql]
SELECT year, year - 1 AS prev_year
-- --8<-- [end:subtract_number_sql]
FROM 'source.parquet'
"""

SPLIT_STRING_SQL = """
-- --8<-- [start:split_string_sql]
SELECT tags.split(',') AS tags
-- --8<-- [end:split_string_sql]
FROM 'source.parquet'
"""

AS_TYPE_SQL = """
-- --8<-- [start:as_type_sql]
SELECT year::INTEGER AS year, price::DOUBLE AS price
-- --8<-- [end:as_type_sql]
FROM 'source.parquet'
"""

# Drop rows needs FROM for WHERE clause context
DROP_ROWS_SQL = """
-- --8<-- [start:drop_rows_sql]
SELECT *
FROM 'source.parquet'
WHERE status != 'deleted' AND price >= 0
-- --8<-- [end:drop_rows_sql]
"""

COLUMNS_TO_DICTS_SQL = """
-- --8<-- [start:columns_to_dicts_sql]
SELECT [{'w': width, 'h': height, 'd': depth}] AS dimensions
-- --8<-- [end:columns_to_dicts_sql]
FROM 'source.parquet'
"""


# ---------------------------------------------------------------------------
# DuckDB Execution Functions (for testing)
# ---------------------------------------------------------------------------


def _run_sql(conn: duckdb.DuckDBPyConnection, sql: str, parquet_path: str) -> pl.DataFrame:
    """Execute SQL with parquet path substitution."""
    return conn.sql(sql.replace("'source.parquet'", f"'{parquet_path}'")).pl()


def fill_empty_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, FILL_EMPTY_SQL, parquet_path)


def remove_nulls_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, REMOVE_NULLS_SQL, parquet_path)


def strip_string_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, STRIP_STRING_SQL, parquet_path)


def extract_first_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, EXTRACT_FIRST_SQL, parquet_path)


def extract_on_condition_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, EXTRACT_ON_CONDITION_SQL, parquet_path)


def contains_bool_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, CONTAINS_BOOL_SQL, parquet_path)


def string_constant_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, STRING_CONSTANT_SQL, parquet_path)


def append_on_condition_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, APPEND_ON_CONDITION_SQL, parquet_path)


def implode_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, IMPLODE_SQL, parquet_path)


def concat_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, CONCAT_SQL, parquet_path)


def rename_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, RENAME_SQL, parquet_path)


def is_empty_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, IS_EMPTY_SQL, parquet_path)


def explode_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, EXPLODE_SQL, parquet_path)


def replace_on_condition_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, REPLACE_ON_CONDITION_SQL, parquet_path)


def lower_string_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, LOWER_STRING_SQL, parquet_path)


def keep_only_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, KEEP_ONLY_SQL, parquet_path)


def drop_null_columns_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, DROP_NULL_COLUMNS_SQL, parquet_path)


def drop_columns_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, DROP_COLUMNS_SQL, parquet_path)


def copy_field_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, COPY_FIELD_SQL, parquet_path)


def replace_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, REPLACE_SQL, parquet_path)


def merge_duckdb(
    conn: duckdb.DuckDBPyConnection, sales_path: str, artists_path: str
) -> pl.DataFrame:
    sql = MERGE_SQL.replace("'sales.parquet'", f"'{sales_path}'").replace(
        "'artists.parquet'", f"'{artists_path}'"
    )
    return conn.sql(sql).pl()


def fill_missing_tuples_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, FILL_MISSING_TUPLES_SQL, parquet_path)


def conditional_bool_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, CONDITIONAL_BOOL_SQL, parquet_path)


def capitalize_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, CAPITALIZE_SQL, parquet_path)


def append_string_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, APPEND_STRING_SQL, parquet_path)


def add_number_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, ADD_NUMBER_SQL, parquet_path)


def subtract_number_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, SUBTRACT_NUMBER_SQL, parquet_path)


def split_string_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, SPLIT_STRING_SQL, parquet_path)


def as_type_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, AS_TYPE_SQL, parquet_path)


def drop_rows_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, DROP_ROWS_SQL, parquet_path)


def columns_to_dicts_duckdb(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> pl.DataFrame:
    return _run_sql(conn, COLUMNS_TO_DICTS_SQL, parquet_path)


# ---------------------------------------------------------------------------
# Test Runner
# ---------------------------------------------------------------------------


def run_test(
    name: str,
    data: dict[str, list],
    polars_fn: Callable[[pl.DataFrame], pl.DataFrame],
    duckdb_fn: Callable,  # Takes (conn, parquet_path) or (conn, path1, path2)
    honeysuckle_fn: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
    extra_data: dict[str, dict] | None = None,
    verbose: bool = False,
) -> ProcessorTestResult:
    """Run a single processor test."""
    try:
        # Prepare data
        df_polars = pl.DataFrame(data)
        df_pandas = pd.DataFrame(data)

        # Run Polars
        polars_result = polars_fn(df_polars)

        # Run DuckDB (from parquet files)
        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = Path(tmpdir) / "source.parquet"
            df_polars.write_parquet(parquet_path)

            conn = duckdb.connect()

            # Handle extra data (for MergeProcessor)
            if extra_data:
                extra_paths = {}
                for table_name, table_data in extra_data.items():
                    extra_path = Path(tmpdir) / f"{table_name}.parquet"
                    pl.DataFrame(table_data).write_parquet(extra_path)
                    extra_paths[table_name] = str(extra_path)
                # For merge test, pass both paths
                if "sales" in extra_paths and "artists" in extra_paths:
                    duckdb_result = duckdb_fn(conn, extra_paths["sales"], extra_paths["artists"])
                else:
                    duckdb_result = duckdb_fn(conn, str(parquet_path))
            else:
                duckdb_result = duckdb_fn(conn, str(parquet_path))

            conn.close()

        # Run Honeysuckle if available
        honeysuckle_result = None
        if HONEYSUCKLE_AVAILABLE and honeysuckle_fn:
            honeysuckle_result = honeysuckle_fn(df_pandas.copy())

        if verbose:
            print(f"\n{'=' * 60}")
            print(f"Test: {name}")
            print(f"{'=' * 60}")
            print(f"Input:\n{df_polars}")
            print(f"\nPolars output:\n{polars_result}")
            print(f"\nDuckDB output:\n{duckdb_result}")
            if honeysuckle_result is not None:
                print(f"\nHoneysuckle output:\n{honeysuckle_result}")

        return ProcessorTestResult(
            name=name,
            passed=True,
            polars_output=polars_result,
            duckdb_output=duckdb_result,
            honeysuckle_output=honeysuckle_result,
        )

    except Exception as e:
        return ProcessorTestResult(name=name, passed=False, error=str(e))


def get_all_tests() -> list[tuple[str, dict, Callable, Callable, Callable | None, dict | None]]:
    """Return all test definitions."""
    tests = []

    # 1. FillEmptyProcessor
    if HONEYSUCKLE_AVAILABLE:

        def fill_empty_hs(df):
            df = FillEmptyProcessor(column_name="title", column_fill="alt_title").process(df)
            df = FillEmptyProcessor(column_name="price", constant_fill=0).process(df)
            return df
    else:
        fill_empty_hs = None
    tests.append(
        (
            "FillEmptyProcessor",
            FILL_EMPTY_DATA,
            fill_empty_polars,
            fill_empty_duckdb,
            fill_empty_hs,
            None,
        )
    )

    # 2. RemoveMultivalueNullsProcessor
    if HONEYSUCKLE_AVAILABLE:

        def remove_nulls_hs(df):
            return RemoveMultivalueNullsProcessor(column="tags").process(df)
    else:
        remove_nulls_hs = None
    tests.append(
        (
            "RemoveMultivalueNullsProcessor",
            REMOVE_NULLS_DATA,
            remove_nulls_polars,
            remove_nulls_duckdb,
            remove_nulls_hs,
            None,
        )
    )

    # 3. StripStringProcessor
    if HONEYSUCKLE_AVAILABLE:

        def strip_string_hs(df):
            return StripStringProcessor(column_name="name", new_column="name_clean").process(df)
    else:
        strip_string_hs = None
    tests.append(
        (
            "StripStringProcessor",
            STRIP_STRING_DATA,
            strip_string_polars,
            strip_string_duckdb,
            strip_string_hs,
            None,
        )
    )

    # 4. ExtractFirstProcessor
    if HONEYSUCKLE_AVAILABLE:

        def extract_first_hs(df):
            return ExtractFirstProcessor(
                column_name="authors", result_column="primary_author"
            ).process(df)
    else:
        extract_first_hs = None
    tests.append(
        (
            "ExtractFirstProcessor",
            EXTRACT_FIRST_DATA,
            extract_first_polars,
            extract_first_duckdb,
            extract_first_hs,
            None,
        )
    )

    # 5. ExtractOnConditionProcessor
    if HONEYSUCKLE_AVAILABLE:

        def extract_on_condition_hs(df):
            return ExtractOnConditionProcessor(
                expression="is_on_sale == True",
                extract_columns=["price"],
                result_columns=["sale_price"],
            ).process(df)
    else:
        extract_on_condition_hs = None
    tests.append(
        (
            "ExtractOnConditionProcessor",
            EXTRACT_ON_CONDITION_DATA,
            extract_on_condition_polars,
            extract_on_condition_duckdb,
            extract_on_condition_hs,
            None,
        )
    )

    # 6. ContainsBoolProcessor
    if HONEYSUCKLE_AVAILABLE:

        def contains_bool_hs(df):
            return ContainsBoolProcessor(
                column_name="description", result_column="has_keyword", pattern="important"
            ).process(df)
    else:
        contains_bool_hs = None
    tests.append(
        (
            "ContainsBoolProcessor",
            CONTAINS_BOOL_DATA,
            contains_bool_polars,
            contains_bool_duckdb,
            contains_bool_hs,
            None,
        )
    )

    # 7. StringConstantDataframeProcessor
    if HONEYSUCKLE_AVAILABLE:

        def string_constant_hs(df):
            return StringConstantDataframeProcessor(
                target_field="source", value="huntington"
            ).process(df)
    else:
        string_constant_hs = None
    tests.append(
        (
            "StringConstantDataframeProcessor",
            STRING_CONSTANT_DATA,
            string_constant_polars,
            string_constant_duckdb,
            string_constant_hs,
            None,
        )
    )

    # 8. AppendOnConditionProcessor
    if HONEYSUCKLE_AVAILABLE:

        def append_on_condition_hs(df):
            return AppendOnConditionProcessor(
                initial_column="tags",
                append_columns=["category"],
                condition="is_featured == True",
            ).process(df)
    else:
        append_on_condition_hs = None
    tests.append(
        (
            "AppendOnConditionProcessor",
            APPEND_ON_CONDITION_DATA,
            append_on_condition_polars,
            append_on_condition_duckdb,
            append_on_condition_hs,
            None,
        )
    )

    # 9. ImplodeProcessor
    if HONEYSUCKLE_AVAILABLE:

        def implode_hs(df):
            return ImplodeProcessor(column_names=["tag"], index_column="artwork_id").process(df)
    else:
        implode_hs = None
    tests.append(
        ("ImplodeProcessor", IMPLODE_DATA, implode_polars, implode_duckdb, implode_hs, None)
    )

    # 10. ConcatProcessor
    if HONEYSUCKLE_AVAILABLE:

        def concat_hs(df):
            return ConcatProcessor(
                new_field="full_name", join_fields=["first", "last"], join_on=" "
            ).process(df)
    else:
        concat_hs = None
    tests.append(("ConcatProcessor", CONCAT_DATA, concat_polars, concat_duckdb, concat_hs, None))

    # 11. RenameProcessor
    if HONEYSUCKLE_AVAILABLE:

        def rename_hs(df):
            return RenameProcessor(
                mapping={"old_name": "new_name", "title": "artwork_title"}
            ).process(df)
    else:
        rename_hs = None
    tests.append(("RenameProcessor", RENAME_DATA, rename_polars, rename_duckdb, rename_hs, None))

    # 12. IsEmptyProcessor
    if HONEYSUCKLE_AVAILABLE:

        def is_empty_hs(df):
            df = IsEmptyProcessor(column_name="email", result_column="missing_email").process(df)
            df = IsEmptyProcessor(
                column_name="email", result_column="has_email", invert=True
            ).process(df)
            return df
    else:
        is_empty_hs = None
    tests.append(
        ("IsEmptyProcessor", IS_EMPTY_DATA, is_empty_polars, is_empty_duckdb, is_empty_hs, None)
    )

    # 13. ExplodeColumnsProcessor
    if HONEYSUCKLE_AVAILABLE:

        def explode_hs(df):
            return ExplodeColumnsProcessor(column_names=["tags"]).process(df)
    else:
        explode_hs = None
    tests.append(
        ("ExplodeColumnsProcessor", EXPLODE_DATA, explode_polars, explode_duckdb, explode_hs, None)
    )

    # 14. ReplaceOnConditionProcessor
    if HONEYSUCKLE_AVAILABLE:

        def replace_on_condition_hs(df):
            df = ReplaceOnConditionProcessor(
                target_field="status",
                conditional_operator="==",
                conditional_value="",
                replacement="unknown",
            ).process(df)
            df = ReplaceOnConditionProcessor(
                target_field="price",
                conditional_operator="<",
                conditional_value=0,
                replacement=0,
            ).process(df)
            return df
    else:
        replace_on_condition_hs = None
    tests.append(
        (
            "ReplaceOnConditionProcessor",
            REPLACE_ON_CONDITION_DATA,
            replace_on_condition_polars,
            replace_on_condition_duckdb,
            replace_on_condition_hs,
            None,
        )
    )

    # 15. LowerStringProcessor
    if HONEYSUCKLE_AVAILABLE:

        def lower_string_hs(df):
            return LowerStringProcessor(column_name="email").process(df)
    else:
        lower_string_hs = None
    tests.append(
        (
            "LowerStringProcessor",
            LOWER_STRING_DATA,
            lower_string_polars,
            lower_string_duckdb,
            lower_string_hs,
            None,
        )
    )

    # 16. KeepOnlyProcessor
    if HONEYSUCKLE_AVAILABLE:

        def keep_only_hs(df):
            return KeepOnlyProcessor(column_names=["id", "title"]).process(df)
    else:
        keep_only_hs = None
    tests.append(
        (
            "KeepOnlyProcessor",
            KEEP_ONLY_DATA,
            keep_only_polars,
            keep_only_duckdb,
            keep_only_hs,
            None,
        )
    )

    # 17. DropNullColumnsProcessor
    if HONEYSUCKLE_AVAILABLE:

        def drop_null_columns_hs(df):
            return DropNullColumnsProcessor().process(df)
    else:
        drop_null_columns_hs = None
    tests.append(
        (
            "DropNullColumnsProcessor",
            DROP_NULL_COLUMNS_DATA,
            drop_null_columns_polars,
            drop_null_columns_duckdb,
            drop_null_columns_hs,
            None,
        )
    )

    # 18. DropColumnsProcessor
    if HONEYSUCKLE_AVAILABLE:

        def drop_columns_hs(df):
            return DropColumnsProcessor(column_names=["internal_id"]).process(df)
    else:
        drop_columns_hs = None
    tests.append(
        (
            "DropColumnsProcessor",
            DROP_COLUMNS_DATA,
            drop_columns_polars,
            drop_columns_duckdb,
            drop_columns_hs,
            None,
        )
    )

    # 19. CopyFieldDataframeProcessor
    if HONEYSUCKLE_AVAILABLE:

        def copy_field_hs(df):
            return CopyFieldDataframeProcessor(
                target_field="original_title", new_field="display_title"
            ).process(df)
    else:
        copy_field_hs = None
    tests.append(
        (
            "CopyFieldDataframeProcessor",
            COPY_FIELD_DATA,
            copy_field_polars,
            copy_field_duckdb,
            copy_field_hs,
            None,
        )
    )

    # 20. ReplaceProcessor
    if HONEYSUCKLE_AVAILABLE:

        def replace_hs(df):
            return ReplaceProcessor(
                target_field="status", to_replace="", replacement="unknown"
            ).process(df)
    else:
        replace_hs = None
    tests.append(
        ("ReplaceProcessor", REPLACE_DATA, replace_polars, replace_duckdb, replace_hs, None)
    )

    # 21. MergeProcessor - requires two dataframes
    def merge_polars_wrapper(df):
        artists = pl.DataFrame(MERGE_ARTISTS_DATA)
        return merge_polars(df, artists)

    if HONEYSUCKLE_AVAILABLE:

        def merge_hs(df):
            # Workaround: Honeysuckle MergeProcessor has a bug with unnamed DataFrame indices
            # (set_index(None) fails), so we name the index before passing
            df.index.name = "_index"
            artists_df = pd.DataFrame(MERGE_ARTISTS_DATA)
            return MergeProcessor(
                merge_dataframe=artists_df,
                merge_columns=["artist_name"],
                merge_dataframe_left="artist_id",
                merge_dataframe_right="id",
            ).process(df)
    else:
        merge_hs = None
    tests.append(
        (
            "MergeProcessor",
            MERGE_SALES_DATA,
            merge_polars_wrapper,
            merge_duckdb,
            merge_hs,
            {"sales": MERGE_SALES_DATA, "artists": MERGE_ARTISTS_DATA},
        )
    )

    # 22. FillMissingTuplesProcessor
    if HONEYSUCKLE_AVAILABLE:

        def fill_missing_tuples_hs(df):
            return FillMissingTuplesProcessor(columns=["names", "roles"]).process(df)
    else:
        fill_missing_tuples_hs = None
    tests.append(
        (
            "FillMissingTuplesProcessor",
            FILL_MISSING_TUPLES_DATA,
            fill_missing_tuples_polars,
            fill_missing_tuples_duckdb,
            fill_missing_tuples_hs,
            None,
        )
    )

    # 23. ConditionalBoolProcessor
    if HONEYSUCKLE_AVAILABLE:

        def conditional_bool_hs(df):
            return ConditionalBoolProcessor(
                expression="price > 1000 and status == 'active'", result_column="is_premium"
            ).process(df)
    else:
        conditional_bool_hs = None
    tests.append(
        (
            "ConditionalBoolProcessor",
            CONDITIONAL_BOOL_DATA,
            conditional_bool_polars,
            conditional_bool_duckdb,
            conditional_bool_hs,
            None,
        )
    )

    # 24. CapitalizeProcessor
    if HONEYSUCKLE_AVAILABLE:

        def capitalize_hs(df):
            return CapitalizeProcessor(column_name="title").process(df)
    else:
        capitalize_hs = None
    tests.append(
        (
            "CapitalizeProcessor",
            CAPITALIZE_DATA,
            capitalize_polars,
            capitalize_duckdb,
            capitalize_hs,
            None,
        )
    )

    # 25. AppendStringProcessor
    if HONEYSUCKLE_AVAILABLE:

        def append_string_hs(df):
            df = AppendStringProcessor(column="id", value="ID-", prefix=True).process(df)
            df = AppendStringProcessor(column="filename", value=".jpg").process(df)
            return df
    else:
        append_string_hs = None
    tests.append(
        (
            "AppendStringProcessor",
            APPEND_STRING_DATA,
            append_string_polars,
            append_string_duckdb,
            append_string_hs,
            None,
        )
    )

    # 26. AddNumberProcessor
    if HONEYSUCKLE_AVAILABLE:

        def add_number_hs(df):
            return AddNumberProcessor(column_name="year", new_column="next_year", value=1).process(
                df
            )
    else:
        add_number_hs = None
    tests.append(
        (
            "AddNumberProcessor",
            ADD_NUMBER_DATA,
            add_number_polars,
            add_number_duckdb,
            add_number_hs,
            None,
        )
    )

    # 27. SubtractNumberProcessor
    if HONEYSUCKLE_AVAILABLE:

        def subtract_number_hs(df):
            return SubtractNumberProcessor(
                column_name="year", new_column="prev_year", value=1
            ).process(df)
    else:
        subtract_number_hs = None
    tests.append(
        (
            "SubtractNumberProcessor",
            SUBTRACT_NUMBER_DATA,
            subtract_number_polars,
            subtract_number_duckdb,
            subtract_number_hs,
            None,
        )
    )

    # 28. SplitStringProcessor
    if HONEYSUCKLE_AVAILABLE:

        def split_string_hs(df):
            return SplitStringProcessor(column_name="tags", delimiter=",").process(df)
    else:
        split_string_hs = None
    tests.append(
        (
            "SplitStringProcessor",
            SPLIT_STRING_DATA,
            split_string_polars,
            split_string_duckdb,
            split_string_hs,
            None,
        )
    )

    # 29. AsTypeProcessor
    if HONEYSUCKLE_AVAILABLE:

        def as_type_hs(df):
            df = AsTypeProcessor(column_name="year", new_type="int").process(df)
            df = AsTypeProcessor(column_name="price", new_type="float").process(df)
            return df
    else:
        as_type_hs = None
    tests.append(
        ("AsTypeProcessor", AS_TYPE_DATA, as_type_polars, as_type_duckdb, as_type_hs, None)
    )

    # 30. DropRowsOnConditionProcessor
    if HONEYSUCKLE_AVAILABLE:

        def drop_rows_hs(df):
            df = DropRowsOnConditionProcessor(
                target_field="status",
                conditional_operator="==",
                conditional_value="deleted",
            ).process(df)
            df = DropRowsOnConditionProcessor(
                target_field="price",
                conditional_operator="<",
                conditional_value=0,
            ).process(df)
            return df
    else:
        drop_rows_hs = None
    tests.append(
        (
            "DropRowsOnConditionProcessor",
            DROP_ROWS_DATA,
            drop_rows_polars,
            drop_rows_duckdb,
            drop_rows_hs,
            None,
        )
    )

    # 31. ColumnsToDictsProcessor
    if HONEYSUCKLE_AVAILABLE:

        def columns_to_dicts_hs(df):
            return ColumnsToDictsProcessor(
                new_field="dimensions",
                column_names=["width", "height", "depth"],
                new_column_names=["w", "h", "d"],
            ).process(df)
    else:
        columns_to_dicts_hs = None
    tests.append(
        (
            "ColumnsToDictsProcessor",
            COLUMNS_TO_DICTS_DATA,
            columns_to_dicts_polars,
            columns_to_dicts_duckdb,
            columns_to_dicts_hs,
            None,
        )
    )

    return tests


def main():
    parser = argparse.ArgumentParser(description="Test processor equivalents")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed output")
    parser.add_argument("--processor", "-p", type=str, help="Run only specific processor test")
    args = parser.parse_args()

    print("=" * 70)
    print("Processor Equivalents Test Suite")
    print("=" * 70)
    print(f"Honeysuckle available: {HONEYSUCKLE_AVAILABLE}")
    if HONEYSUCKLE_AVAILABLE:
        print(f"Honeysuckle path: {HONEYSUCKLE_PATH}")
    print()

    tests = get_all_tests()

    if args.processor:
        tests = [(name, *rest) for name, *rest in tests if args.processor.lower() in name.lower()]
        if not tests:
            print(f"No tests found matching '{args.processor}'")
            return

    results = []
    for name, data, polars_fn, duckdb_fn, honeysuckle_fn, extra_data in tests:
        result = run_test(
            name, data, polars_fn, duckdb_fn, honeysuckle_fn, extra_data, verbose=args.verbose
        )
        results.append(result)

        status = "✓" if result.passed else "✗"
        hs_status = "HS" if honeysuckle_fn and HONEYSUCKLE_AVAILABLE else "--"
        print(f"  {status} {name:<40} [Polars, DuckDB, {hs_status}]")
        if not result.passed:
            print(f"      Error: {result.error}")

    # Summary
    passed = sum(1 for r in results if r.passed)
    total = len(results)
    print()
    print("-" * 70)
    print(f"Results: {passed}/{total} tests passed")

    if passed < total:
        sys.exit(1)


if __name__ == "__main__":
    main()
