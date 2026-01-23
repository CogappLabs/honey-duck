"""Example: XML Harvesting

Demonstrates XML → Polars DataFrame → Parquet using the plant catalog.

Run with:
    uv run python examples/xml_harvesting_example.py
"""

import dagster as dg
import polars as pl
from pathlib import Path

from cogapp_libs.dagster import parse_xml_file


# =============================================================================
# Simple Example: Plant Catalog
# =============================================================================


@dg.asset(group_name="xml_harvest", kinds={"xml"})
def plant_catalog(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Harvest plant catalog from XML with nested elements as JSON."""
    records = parse_xml_file(
        file_path=Path("examples/data/plant_catalog.xml"),
        record_path="./PLANT",  # Direct children only
        fields={
            "plant_id": "./@id",
            "common_name": "./COMMON/text()",
            "botanical_name": "./BOTANICAL/text()",
            "zone": "./ZONE/text()",
            "light": "./LIGHT/text()",
            "price": "./PRICE/text()",
            "availability": "./AVAILABILITY/text()",
        },
        nested_fields={
            "images": "./IMAGES/IMAGE",  # Each IMAGE → JSON with type attr
            "companions": "./COMPANIONS/PLANT",  # Each companion plant → JSON
        },
    )

    df = pl.DataFrame(records)
    context.add_output_metadata(
        {
            "row_count": len(df),
            "columns": df.columns,
        }
    )
    return df


# =============================================================================
# Dagster Definitions
# =============================================================================

defs = dg.Definitions(
    assets=[plant_catalog],
)


# =============================================================================
# Standalone Usage
# =============================================================================

if __name__ == "__main__":
    records = parse_xml_file(
        file_path=Path("examples/data/plant_catalog.xml"),
        record_path="./PLANT",  # Direct children only
        fields={
            "plant_id": "./@id",
            "common_name": "./COMMON/text()",
            "botanical_name": "./BOTANICAL/text()",
            "zone": "./ZONE/text()",
            "light": "./LIGHT/text()",
            "price": "./PRICE/text()",
        },
        nested_fields={
            "images": "./IMAGES/IMAGE",
            "companions": "./COMPANIONS/PLANT",
        },
    )

    df = pl.DataFrame(records)
    print(f"Loaded {len(df)} plants\n")
    print(df)
    print("\nSample images JSON:")
    print(records[0]["images"])
    print("\nSample companions JSON:")
    print(records[0]["companions"])
