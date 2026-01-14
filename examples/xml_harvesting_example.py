"""Example: XML Harvesting with DLT

This example demonstrates how to harvest XML data using the XML DLT sources.

Covers:
- Local XML files
- HTTP/RSS feeds
- Large file streaming
- Namespace handling
- Dagster integration
"""

import dagster as dg
import dlt
from dagster_dlt import DagsterDltResource, dlt_assets

from cogapp_deps.dagster import (
    create_parquet_pipeline,
    xml_file_source,
    xml_http_source,
    xml_multi_source,
    xml_streaming_source,
)


# Example 1: Simple XML File
# ==========================


@dlt.source(name="artwork_catalog")
def artwork_xml_source():
    """Harvest artwork catalog from XML file."""
    return xml_file_source(
        file_path="examples/data/artworks.xml",
        record_path="//artwork",
        fields={
            "artwork_id": "./@id",  # Attribute
            "title": "./title/text()",  # Element text
            "artist": "./artist/text()",
            "year": "./year/text()",
            "medium": "./medium/text()",
            "price_usd": "./price/text()",
        },
        resource_name="artworks_xml",
    )


@dlt_assets(
    dlt_source=artwork_xml_source(),
    dlt_pipeline=create_parquet_pipeline(
        pipeline_name="xml_harvest",
        destination_dir="data/output/xml_harvest",
        dataset_name="raw",
    ),
    name="artwork_catalog_xml",
    group_name="xml_harvest",
)
def artwork_xml_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    """Harvest artwork catalog from XML."""
    yield from dlt.run(context=context, write_disposition="replace")


# Example 2: RSS Feed
# ===================


@dlt.source(name="rss_feed")
def rss_feed_source():
    """Harvest RSS feed as XML."""
    return xml_http_source(
        url="https://example.com/rss.xml",
        record_path="//item",
        fields={
            "title": "./title/text()",
            "link": "./link/text()",
            "description": "./description/text()",
            "pub_date": "./pubDate/text()",
            "guid": "./guid/text()",
        },
    )


# Example 3: Large File Streaming
# ================================


@dlt.source(name="large_catalog")
def large_xml_streaming():
    """Stream large XML file (>1GB) efficiently."""
    return xml_streaming_source(
        file_path="examples/data/large_catalog.xml",
        record_tag="record",  # Tag name, not XPath
        fields={
            "id": "./id/text()",
            "name": "./name/text()",
            "category": "./category/text()",
            "value": "./value/text()",
        },
    )


# Example 4: XML with Namespaces
# ===============================


@dlt.source(name="namespaced_xml")
def namespaced_xml_source():
    """Handle XML with namespaces (e.g., SOAP, Atom feeds)."""
    return xml_file_source(
        file_path="examples/data/atom_feed.xml",
        record_path="//atom:entry",
        fields={
            "id": "./atom:id/text()",
            "title": "./atom:title/text()",
            "updated": "./atom:updated/text()",
            "author": "./atom:author/atom:name/text()",
            "content": "./atom:content/text()",
        },
        namespaces={
            "atom": "http://www.w3.org/2005/Atom",
        },
        resource_name="atom_entries",
    )


# Example 5: Multiple XML Sources
# ================================


@dlt.source(name="multi_xml_harvest")
def multi_xml_harvest():
    """Combine multiple XML sources in one pipeline."""
    return xml_multi_source(
        local_files=[
            {
                "file_path": "examples/data/artworks.xml",
                "record_path": "//artwork",
                "fields": {
                    "id": "./@id",
                    "title": "./title/text()",
                    "artist": "./artist/text()",
                },
                "resource_name": "artworks",
            },
            {
                "file_path": "examples/data/artists.xml",
                "record_path": "//artist",
                "fields": {
                    "id": "./@id",
                    "name": "./name/text()",
                    "nationality": "./nationality/text()",
                },
                "resource_name": "artists",
            },
        ],
        http_endpoints=[
            {
                "url": "https://api.example.com/exhibitions.xml",
                "record_path": "//exhibition",
                "fields": {
                    "id": "./@id",
                    "title": "./title/text()",
                    "start_date": "./start-date/text()",
                },
                "resource_name": "exhibitions",
            },
        ],
    )


@dlt_assets(
    dlt_source=multi_xml_harvest(),
    dlt_pipeline=create_parquet_pipeline(
        pipeline_name="multi_xml_harvest",
        destination_dir="data/output/xml_harvest",
        dataset_name="raw",
    ),
    name="multi_xml_harvest_assets",
    group_name="xml_harvest",
)
def multi_xml_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    """Harvest from multiple XML sources."""
    yield from dlt.run(context=context, write_disposition="replace")


# Example 6: SOAP API Response
# =============================


@dlt.source(name="soap_api")
def soap_xml_source():
    """Parse SOAP API response."""
    return xml_http_source(
        url="https://api.example.com/soap/endpoint",
        record_path="//soap:Body//tns:Record",
        fields={
            "id": "./tns:ID/text()",
            "name": "./tns:Name/text()",
            "status": "./tns:Status/text()",
        },
        namespaces={
            "soap": "http://schemas.xmlsoap.org/soap/envelope/",
            "tns": "http://example.com/schema",
        },
        headers={
            "Content-Type": "text/xml",
            "SOAPAction": "GetRecords",
        },
    )


# Example 7: Nested XML Structures
# =================================


@dlt.source(name="nested_xml")
def nested_xml_source():
    """Handle nested XML with multiple levels."""
    return xml_file_source(
        file_path="examples/data/catalog_nested.xml",
        record_path="//product",
        fields={
            "product_id": "./@id",
            "name": "./name/text()",
            "price": "./pricing/retail/text()",
            "category": "./category/@name",
            # For nested lists, XPath returns multiple results
            "tags": "./tags/tag/text()",  # Returns list
            "variants": "./variants/variant/@sku",  # Returns list of SKUs
        },
        resource_name="products",
    )


# ============================================================================
# Complete Dagster Definitions
# ============================================================================

defs = dg.Definitions(
    assets=[
        artwork_xml_assets,
        multi_xml_assets,
    ],
    resources={
        "dlt": DagsterDltResource(),
    },
)


# ============================================================================
# Usage Examples
# ============================================================================

if __name__ == "__main__":
    # Test XML parsing standalone
    import dlt

    # Example: Parse local XML
    pipeline = dlt.pipeline(
        pipeline_name="xml_test",
        destination="duckdb",
        dataset_name="xml_data",
    )

    source = artwork_xml_source()
    load_info = pipeline.run(source)

    print(f"Loaded {load_info.load_packages[0].state} records")

    # Example: Stream large file
    streaming_pipeline = dlt.pipeline(
        pipeline_name="xml_streaming_test",
        destination="duckdb",
        dataset_name="xml_data",
    )

    streaming_source = large_xml_streaming()
    load_info = streaming_pipeline.run(streaming_source)

    print("Streamed records from large file")
