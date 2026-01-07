"""Business rules and configuration constants for the honey-duck pipeline.

These constants define the business logic thresholds and rules used across
the pipeline. Centralizing them here makes it easy to:
- Understand what business rules exist
- Modify thresholds without hunting through code
- Test with different configurations
"""

# -----------------------------------------------------------------------------
# Sales Filtering
# -----------------------------------------------------------------------------

# Minimum sale value to include in high-value sales output
# Sales below this threshold are filtered out in sales_output
MIN_SALE_VALUE_USD = 30_000_000

# -----------------------------------------------------------------------------
# Price Tiers
# -----------------------------------------------------------------------------

# Artworks are categorized into price tiers for analysis:
#   - budget:  < $500,000
#   - mid:     $500,000 - $3,000,000
#   - premium: >= $3,000,000

PRICE_TIER_BUDGET_MAX_USD = 500_000
PRICE_TIER_MID_MAX_USD = 3_000_000

# Valid tier names (for validation)
PRICE_TIERS = ("budget", "mid", "premium")
