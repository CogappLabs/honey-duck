"""Helper utilities for honey-duck pipeline assets.

This module provides common dependencies used across asset modules.
Import STANDARD_HARVEST_DEPS for consistent dependency declarations.
"""

import dagster as dg

# Standard harvest dependencies - all transform assets depend on these
# Import this constant in asset modules instead of declaring HARVEST_DEPS locally
STANDARD_HARVEST_DEPS = [
    dg.AssetKey("dlt_harvest_sales_raw"),
    dg.AssetKey("dlt_harvest_artworks_raw"),
    dg.AssetKey("dlt_harvest_artists_raw"),
    dg.AssetKey("dlt_harvest_media"),
]
