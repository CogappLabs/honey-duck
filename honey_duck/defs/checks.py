"""Asset checks for the honey-duck pipeline.

Includes:
- Pandera schema validation (blocking) for transform assets
- Data quality checks for output assets

Note: Freshness is handled via FreshnessPolicy on assets (not checks).
"""

import dagster as dg
import pandas as pd
import pandera.pandas as pa

from .assets import (
    artworks_output,
    artworks_transform,
    sales_output,
    sales_transform,
)
from .constants import MIN_SALE_VALUE_USD, PRICE_TIERS
from .schemas import ArtworksTransformSchema, SalesTransformSchema


# -----------------------------------------------------------------------------
# Blocking Pandera Checks - Prevent downstream if schema validation fails
# -----------------------------------------------------------------------------


@dg.asset_check(asset=sales_transform, blocking=True)
def check_sales_transform_schema(sales_transform: pd.DataFrame) -> dg.AssetCheckResult:
    """Validate sales_transform against Pandera schema.

    Blocking: If this fails, sales_output will not materialize.
    """
    try:
        SalesTransformSchema.validate(sales_transform)
        return dg.AssetCheckResult(
            passed=True,
            metadata={
                "record_count": dg.MetadataValue.int(len(sales_transform)),
                "schema": dg.MetadataValue.text("SalesTransformSchema"),
            },
        )
    except pa.errors.SchemaError as e:
        return dg.AssetCheckResult(
            passed=False,
            metadata={
                "error": dg.MetadataValue.text(str(e)),
                "schema": dg.MetadataValue.text("SalesTransformSchema"),
            },
        )


@dg.asset_check(asset=artworks_transform, blocking=True)
def check_artworks_transform_schema(
    artworks_transform: pd.DataFrame,
) -> dg.AssetCheckResult:
    """Validate artworks_transform against Pandera schema.

    Blocking: If this fails, artworks_output will not materialize.
    """
    try:
        ArtworksTransformSchema.validate(artworks_transform)
        return dg.AssetCheckResult(
            passed=True,
            metadata={
                "record_count": dg.MetadataValue.int(len(artworks_transform)),
                "schema": dg.MetadataValue.text("ArtworksTransformSchema"),
            },
        )
    except pa.errors.SchemaError as e:
        return dg.AssetCheckResult(
            passed=False,
            metadata={
                "error": dg.MetadataValue.text(str(e)),
                "schema": dg.MetadataValue.text("ArtworksTransformSchema"),
            },
        )


# -----------------------------------------------------------------------------
# Output Data Quality Checks - Non-blocking validation of final outputs
# -----------------------------------------------------------------------------


@dg.asset_check(asset=sales_output)
def check_sales_above_threshold(sales_output: pd.DataFrame) -> dg.AssetCheckResult:
    """Check that all sales in output meet the minimum threshold."""
    below_threshold = (sales_output["sale_price_usd"] < MIN_SALE_VALUE_USD).sum()
    passed = bool(below_threshold == 0)

    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "threshold": dg.MetadataValue.text(f"${MIN_SALE_VALUE_USD:,}"),
            "min_sale_price": dg.MetadataValue.float(
                float(sales_output["sale_price_usd"].min())
            ),
            "below_threshold_count": dg.MetadataValue.int(int(below_threshold)),
        },
    )


@dg.asset_check(asset=artworks_output)
def check_valid_price_tiers(artworks_output: pd.DataFrame) -> dg.AssetCheckResult:
    """Check that all artworks have a valid price tier."""
    invalid_tiers = ~artworks_output["price_tier"].isin(PRICE_TIERS)
    invalid_count = invalid_tiers.sum()
    passed = bool(invalid_count == 0)

    # Convert to JSON-serializable dict (pandas to_dict handles numpy types)
    tier_dist = artworks_output["price_tier"].value_counts().to_dict()

    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "invalid_tier_count": dg.MetadataValue.int(int(invalid_count)),
            "tier_distribution": dg.MetadataValue.json(tier_dist),
        },
    )
