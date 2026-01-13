"""Pandera schemas for data validation.

These schemas define the expected structure and constraints for transformed data.
Used by blocking asset checks to prevent bad data from flowing downstream.

Features:
- Lazy validation: Collect ALL errors before failing (vs. fail-fast)
- Row filtering: Optionally remove invalid rows instead of failing
- Rich error reporting: Full error details for debugging

Note: Uses pandera.polars for validating Polars DataFrames.

Usage:
    # Fail-fast validation (default)
    SalesTransformSchema.validate(df)

    # Lazy validation - collect all errors
    df, errors = validate_with_errors(df, SalesTransformSchema)

    # Filter out bad rows instead of failing
    clean_df, removed_count = filter_invalid_rows(df, SalesTransformSchema)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar

import pandera.polars as pa

if TYPE_CHECKING:
    import polars as pl


T = TypeVar("T", bound=pa.DataFrameModel)


@dataclass
class ValidationResult:
    """Result of lazy validation with error details.

    Attributes:
        valid: Whether all rows passed validation
        dataframe: Original DataFrame
        error_count: Number of validation errors
        errors: List of error details (column, check, failure_case)
        invalid_indices: Set of row indices that failed validation
    """

    valid: bool
    dataframe: pl.DataFrame
    error_count: int
    errors: list[dict]
    invalid_indices: set[int]


def validate_lazy(
    df: pl.DataFrame,
    schema: type[T],
) -> ValidationResult:
    """Validate DataFrame with lazy=True to collect ALL errors.

    Unlike standard validation that fails on first error, lazy validation
    continues checking all rows and columns, collecting every error.

    Args:
        df: Polars DataFrame to validate
        schema: Pandera DataFrameModel class

    Returns:
        ValidationResult with error details and invalid row indices

    Example:
        result = validate_lazy(df, SalesTransformSchema)
        if not result.valid:
            print(f"Found {result.error_count} errors:")
            for err in result.errors:
                print(f"  - Column '{err['column']}': {err['check']}")
    """
    try:
        schema.validate(df, lazy=True)
        return ValidationResult(
            valid=True,
            dataframe=df,
            error_count=0,
            errors=[],
            invalid_indices=set(),
        )
    except pa.errors.SchemaErrors as e:
        # Parse error details from SchemaErrors
        errors = []
        invalid_indices: set[int] = set()

        for failure in e.failure_cases.to_dicts():
            error_detail = {
                "column": failure.get("column"),
                "check": failure.get("check"),
                "failure_case": failure.get("failure_case"),
            }
            errors.append(error_detail)

            # Track row index if available
            idx = failure.get("index")
            if idx is not None:
                invalid_indices.add(idx)

        return ValidationResult(
            valid=False,
            dataframe=df,
            error_count=len(errors),
            errors=errors,
            invalid_indices=invalid_indices,
        )


def filter_invalid_rows(
    df: pl.DataFrame,
    schema: type[T],
    log_removed: bool = True,
) -> tuple[pl.DataFrame, int]:
    """Validate DataFrame and filter out invalid rows instead of failing.

    Performs lazy validation to identify all invalid rows, then returns
    a clean DataFrame with those rows removed.

    Args:
        df: Polars DataFrame to validate
        schema: Pandera DataFrameModel class
        log_removed: Whether to log removed row details (for debugging)

    Returns:
        Tuple of (clean_df, removed_count)

    Example:
        clean_df, removed = filter_invalid_rows(df, SalesTransformSchema)
        print(f"Removed {removed} invalid rows, {len(clean_df)} remaining")
    """
    import polars as pl

    result = validate_lazy(df, schema)

    if result.valid:
        return df, 0

    # Filter out invalid rows by index
    if result.invalid_indices:
        # Add row index, filter, then drop
        clean_df = (
            df.with_row_index("__row_idx__")
            .filter(~pl.col("__row_idx__").is_in(list(result.invalid_indices)))
            .drop("__row_idx__")
        )
        removed_count = len(result.invalid_indices)
    else:
        # No row indices available - can't filter, return original
        return df, 0

    return clean_df, removed_count


def validate_with_report(
    df: pl.DataFrame,
    schema: type[T],
    asset_name: str | None = None,
) -> tuple[bool, dict]:
    """Validate DataFrame and return a structured report for metadata.

    Designed for use with Dagster asset checks - returns metadata dict
    suitable for add_output_metadata().

    Args:
        df: Polars DataFrame to validate
        schema: Pandera DataFrameModel class
        asset_name: Optional asset name for reporting

    Returns:
        Tuple of (passed, metadata_dict)

    Example:
        passed, metadata = validate_with_report(df, SalesTransformSchema)
        context.add_output_metadata(metadata)
    """
    result = validate_lazy(df, schema)

    metadata = {
        "schema": schema.__name__,
        "record_count": len(df),
        "valid": result.valid,
        "error_count": result.error_count,
    }

    if asset_name:
        metadata["asset"] = asset_name

    if not result.valid:
        # Summarize errors by column
        error_summary: dict[str, list[str]] = {}
        for err in result.errors:
            col = err.get("column", "unknown")
            if col not in error_summary:
                error_summary[col] = []
            error_summary[col].append(err.get("check", "unknown"))

        metadata["error_summary"] = error_summary
        metadata["invalid_row_count"] = len(result.invalid_indices)

    return result.valid, metadata


class SalesTransformSchema(pa.DataFrameModel):
    """Schema for sales_transform output."""

    sale_id: int
    artwork_id: int
    sale_date: str
    sale_price_usd: int = pa.Field(gt=0)
    buyer_country: str
    title: str
    artist_id: int
    artwork_year: float = pa.Field(nullable=True)
    medium: str
    list_price_usd: int = pa.Field(gt=0)
    artist_name: str = pa.Field(nullable=False)
    nationality: str
    price_diff: int
    pct_change: float = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = False  # Allow extra columns


class ArtworksTransformSchema(pa.DataFrameModel):
    """Schema for artworks_transform output."""

    artwork_id: int
    title: str
    year: float = pa.Field(nullable=True)
    medium: str
    list_price_usd: int = pa.Field(gt=0)
    artist_name: str = pa.Field(nullable=False)
    nationality: str
    sale_count: int = pa.Field(ge=0)
    total_sales_value: float = pa.Field(ge=0)
    avg_sale_price: float = pa.Field(nullable=True)
    first_sale_date: str = pa.Field(nullable=True)
    last_sale_date: str = pa.Field(nullable=True)
    has_sold: bool
    price_tier: str = pa.Field(isin=["budget", "mid", "premium"])
    sales_rank: int = pa.Field(ge=1)
    primary_image: str = pa.Field(nullable=True)
    primary_image_alt: str = pa.Field(nullable=True)
    media_count: int = pa.Field(ge=0)

    class Config:
        coerce = True
        strict = False  # Allow extra columns
