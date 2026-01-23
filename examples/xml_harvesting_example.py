"""Example: XML Harvesting

Demonstrates XML → Polars DataFrame → Parquet using the plant catalog.

Run with:
    uv run python examples/xml_harvesting_example.py
"""

import dagster as dg
import polars as pl
from pathlib import Path

from cogapp_libs.dagster import (
    add_dataframe_metadata,
    parse_xml_file,
    table_preview_to_metadata,
    track_timing,
)


# =============================================================================
# Simple Example: Plant Catalog
# =============================================================================


@dg.asset(group_name="xml_harvest", kinds={"xml"})
def plant_catalog(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Harvest plant catalog from XML with nested elements as JSON."""
    with track_timing(context, "XML parsing"):
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

    # Rich metadata: record count, columns, markdown table preview
    add_dataframe_metadata(
        context,
        df,
        # Custom extras
        unique_zones=df["zone"].n_unique(),
        light_conditions=df["light"].unique().to_list(),
    )

    # Additional preview showing just names and prices
    context.add_output_metadata(
        table_preview_to_metadata(
            df.select("common_name", "botanical_name", "price"),
            title="price_list",
            header="Plant Price List",
        )
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
