"""Helper functions and decorators to reduce boilerplate in ETL pipelines.

This module provides:
- Decorators for automatic error handling
- Helper functions for common operations
- Clear patterns for harvest/transform/output assets
"""

import time
from functools import wraps
from typing import Callable, TypeVar

import dagster as dg
import polars as pl

from .config import CONFIG
from .exceptions import DataValidationError
from .utils import read_raw_table_lazy

# Type variable for generic decorators
F = TypeVar("F", bound=Callable)


def with_timing(func: F) -> F:
    """Decorator to add timing metadata to asset execution.

    Automatically logs execution time and adds it to metadata.

    Example:
        @dg.asset
        @with_timing
        def my_asset(context):
            # Processing happens here
            return result
    """

    @wraps(func)
    def wrapper(context: dg.AssetExecutionContext, *args, **kwargs):
        start_time = time.perf_counter()
        result = func(context, *args, **kwargs)
        elapsed_ms = (time.perf_counter() - start_time) * 1000

        # Add timing to metadata if not already present
        if hasattr(context, "add_output_metadata"):
            # Get existing metadata if any
            existing_metadata = getattr(context, "_metadata_entries", {})
            if "processing_time_ms" not in existing_metadata:
                context.add_output_metadata({"processing_time_ms": round(elapsed_ms, 2)})

        context.log.info(f"[{func.__name__}] Completed in {elapsed_ms:.1f}ms")
        return result

    return wrapper  # type: ignore


def read_harvest_tables(
    *table_specs: tuple[str, list[str]],
    asset_name: str,
) -> dict[str, pl.LazyFrame]:
    """Read multiple harvest tables with validation in one call.

    This helper reduces boilerplate when reading multiple related tables.

    Args:
        *table_specs: Tuples of (table_name, required_columns)
        asset_name: Name of calling asset for error context

    Returns:
        Dictionary mapping table names to LazyFrames

    Example:
        tables = read_harvest_tables(
            ("sales_raw", ["sale_id", "sale_price_usd"]),
            ("artworks_raw", ["artwork_id", "title"]),
            asset_name="sales_transform",
        )
        sales = tables["sales_raw"]
        artworks = tables["artworks_raw"]
    """
    result = {}
    for table_name, required_columns in table_specs:
        result[table_name] = read_raw_table_lazy(
            CONFIG.duckdb_path,
            table_name,
            required_columns=required_columns,
            asset_name=asset_name,
        )
    return result


def add_standard_metadata(
    context: dg.AssetExecutionContext,
    df: pl.DataFrame,
    **extra_metadata,
) -> None:
    """Add standard metadata for a DataFrame result.

    Automatically includes:
    - Record count
    - Column list
    - Preview (first 5 rows as markdown table)
    - Any extra metadata provided

    Args:
        context: Asset execution context
        df: Result DataFrame
        **extra_metadata: Additional metadata to include

    Example:
        add_standard_metadata(
            context,
            result,
            unique_artworks=result["artwork_id"].n_unique(),
            total_value=float(result["sale_price_usd"].sum()),
        )
    """
    metadata = {
        "record_count": len(df),
        "columns": df.columns,
        "preview": dg.MetadataValue.md(df.head(5).to_pandas().to_markdown(index=False)),
        **extra_metadata,
    }
    context.add_output_metadata(metadata)


# Asset group constants for consistency
class AssetGroups:
    """Standard asset group names."""

    HARVEST = "harvest"
    TRANSFORM = "transform"
    TRANSFORM_POLARS = "transform_polars"
    TRANSFORM_POLARS_OPS = "transform_polars_ops"
    TRANSFORM_DUCKDB = "transform_duckdb"
    TRANSFORM_POLARS_FS = "transform_polars_fs"
    OUTPUT = "output"
    OUTPUT_POLARS = "output_polars"
    OUTPUT_POLARS_OPS = "output_polars_ops"
    OUTPUT_DUCKDB = "output_duckdb"
    OUTPUT_POLARS_FS = "output_polars_fs"


# Standard harvest dependencies
STANDARD_HARVEST_DEPS = [
    dg.AssetKey("dlt_harvest_sales_raw"),
    dg.AssetKey("dlt_harvest_artworks_raw"),
    dg.AssetKey("dlt_harvest_artists_raw"),
    dg.AssetKey("dlt_harvest_media"),
]


def transform_asset(
    group_name: str = AssetGroups.TRANSFORM_POLARS,
    harvest_deps: list[dg.AssetKey] | None = None,
):
    """Decorator for transform assets with automatic error handling.

    Provides:
    - Automatic group_name
    - Standard kinds (polars)
    - Harvest dependencies
    - Error context in exceptions

    Example:
        @transform_asset(group_name="transform_polars")
        def sales_transform(context: dg.AssetExecutionContext) -> pl.DataFrame:
            tables = read_harvest_tables(
                ("sales_raw", ["sale_id", "sale_price_usd"]),
                asset_name="sales_transform",
            )
            # Transform logic...
            return result
    """

    def decorator(func: Callable) -> Callable:
        # Apply dagster asset decorator with standard settings
        @dg.asset(
            kinds={"polars"},
            group_name=group_name,
            deps=harvest_deps or STANDARD_HARVEST_DEPS,
        )
        @wraps(func)
        def wrapper(context: dg.AssetExecutionContext, *args, **kwargs) -> pl.DataFrame:
            try:
                start_time = time.perf_counter()
                result = func(context, *args, **kwargs)
                elapsed_ms = (time.perf_counter() - start_time) * 1000

                # Auto-add timing if not already added
                if hasattr(context, "_output_metadata"):
                    metadata = context._output_metadata  # type: ignore
                    if "processing_time_ms" not in metadata:
                        context.add_output_metadata(
                            {"processing_time_ms": round(elapsed_ms, 2)}
                        )

                context.log.info(
                    f"[{func.__name__}] Completed with {len(result):,} records in {elapsed_ms:.1f}ms"
                )
                return result
            except DataValidationError:
                # Re-raise validation errors with context already set
                raise
            except Exception as e:
                # Wrap other exceptions with asset context
                context.log.error(f"[{func.__name__}] Failed with error: {e}")
                raise DataValidationError(func.__name__, str(e)) from e

        return wrapper

    return decorator


def output_asset(
    group_name: str = AssetGroups.OUTPUT_POLARS,
    freshness_hours: int = 24,
):
    """Decorator for output assets.

    Provides:
    - Automatic group_name
    - Standard kinds (polars, json)
    - Freshness policy
    - Error context

    Example:
        @output_asset(group_name="output_polars")
        def sales_output(
            context: dg.AssetExecutionContext,
            sales_transform: pl.DataFrame,
        ) -> pl.DataFrame:
            # Filter and output...
            return result
    """

    def decorator(func: Callable) -> Callable:
        from datetime import timedelta

        @dg.asset(
            kinds={"polars", "json"},
            group_name=group_name,
            freshness_policy=dg.FreshnessPolicy.time_window(
                fail_window=timedelta(hours=freshness_hours)
            ),
        )
        @wraps(func)
        def wrapper(context: dg.AssetExecutionContext, *args, **kwargs) -> pl.DataFrame:
            try:
                start_time = time.perf_counter()
                result = func(context, *args, **kwargs)
                elapsed_ms = (time.perf_counter() - start_time) * 1000

                context.log.info(
                    f"[{func.__name__}] Output {len(result):,} records in {elapsed_ms:.1f}ms"
                )
                return result
            except Exception as e:
                context.log.error(f"[{func.__name__}] Failed with error: {e}")
                raise DataValidationError(func.__name__, str(e)) from e

        return wrapper

    return decorator
