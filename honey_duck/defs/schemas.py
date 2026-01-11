"""Pandera schemas for data validation.

These schemas define the expected structure and constraints for transformed data.
Used by blocking asset checks to prevent bad data from flowing downstream.

Note: Uses pandera.polars for validating Polars DataFrames.
"""

import pandera.polars as pa
from pandera.polars import Column


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
