"""Asset checks for the honey-duck pipeline.

Simple data quality checks to validate asset outputs.
"""

import dagster as dg
import pandas as pd


@dg.asset_check(asset="sales_enriched")
def check_no_null_artists(sales_enriched: pd.DataFrame) -> dg.AssetCheckResult:
    """Check that all sales have an artist name."""
    null_count = sales_enriched["artist_name"].isna().sum()
    passed = bool(null_count == 0)

    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "null_artist_count": dg.MetadataValue.int(int(null_count)),
            "total_records": dg.MetadataValue.int(len(sales_enriched)),
        },
    )


@dg.asset_check(asset="sales_output")
def check_sales_above_threshold(sales_output: pd.DataFrame) -> dg.AssetCheckResult:
    """Check that all sales in output meet the $30M threshold."""
    min_threshold = 30_000_000
    below_threshold = (sales_output["sale_price_usd"] < min_threshold).sum()
    passed = bool(below_threshold == 0)

    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "min_sale_price": dg.MetadataValue.float(float(sales_output["sale_price_usd"].min())),
            "below_threshold_count": dg.MetadataValue.int(int(below_threshold)),
        },
    )


@dg.asset_check(asset="artworks_output")
def check_valid_price_tiers(artworks_output: pd.DataFrame) -> dg.AssetCheckResult:
    """Check that all artworks have a valid price tier."""
    valid_tiers = {"budget", "mid", "premium"}
    invalid_tiers = ~artworks_output["price_tier"].isin(valid_tiers)
    invalid_count = invalid_tiers.sum()
    passed = bool(invalid_count == 0)

    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "invalid_tier_count": dg.MetadataValue.int(int(invalid_count)),
            "tier_distribution": dg.MetadataValue.json(
                artworks_output["price_tier"].value_counts().to_dict()
            ),
        },
    )
