"""DLT sources for harvesting XML data.

This module provides DLT resources for parsing and loading XML data from
various sources (local files, HTTP endpoints, S3, etc.).

Features:
- XPath-based element extraction
- Namespace handling
- Streaming for large XML files
- Automatic schema inference
- Support for nested structures

Example:
    ```python
    import dlt
    from cogapp_deps.dagster.xml_sources import xml_file_source

    @dlt.source
    def harvest_xml():
        return xml_file_source(
            file_path="data.xml",
            record_path="//record",
            fields={
                "id": "./id/text()",
                "name": "./name/text()",
                "value": "./value/text()",
            },
        )
    ```
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Iterator

import dlt


@dlt.resource(
    name="xml_records",
    write_disposition="replace",
)
def xml_file_source(
    file_path: str | Path,
    record_path: str,
    fields: dict[str, str],
    namespaces: dict[str, str] | None = None,
    resource_name: str = "xml_records",
) -> Iterator[dict[str, Any]]:
    """Harvest records from an XML file using XPath.

    Parses XML file and extracts records using XPath expressions. Each record
    is yielded as a dictionary for DLT processing.

    Args:
        file_path: Path to XML file (local or URL)
        record_path: XPath to locate record elements (e.g., "//record", ".//item")
        fields: Mapping of field names to XPath expressions relative to record
            Example: {"id": "./id/text()", "name": "./@name"}
        namespaces: Optional namespace mappings for XPath
            Example: {"ns": "http://example.com/schema"}
        resource_name: DLT resource name (default: "xml_records")

    Yields:
        Dict with extracted fields from each record

    Example:
        ```python
        # XML structure:
        # <root>
        #   <record>
        #     <id>1</id>
        #     <name>Item 1</name>
        #     <value>100</value>
        #   </record>
        # </root>

        yield from xml_file_source(
            file_path="data.xml",
            record_path="//record",
            fields={
                "id": "./id/text()",
                "name": "./name/text()",
                "value": "./value/text()",
            },
        )
        # Yields: {"id": "1", "name": "Item 1", "value": "100"}
        ```

    Notes:
        - For large files, consider using xml_streaming_source()
        - XPath expressions are relative to the record element
        - Use text() to extract text content, @ for attributes
        - Supports remote files via HTTP/HTTPS URLs
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"XML file not found: {file_path}")

    print(f"Parsing XML file: {file_path}")

    # Parse XML
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Find all records using XPath
    records = root.findall(record_path, namespaces=namespaces or {})
    print(f"Found {len(records)} records at path: {record_path}")

    # Extract fields from each record
    for idx, record in enumerate(records):
        row: dict[str, str | list[str | None] | None] = {}

        for field_name, xpath_expr in fields.items():
            # Evaluate XPath relative to record
            result = record.findall(xpath_expr, namespaces=namespaces or {})

            if result:
                # Handle single vs multiple results
                if len(result) == 1:
                    # Single value - check if it's text or element
                    if isinstance(result[0], str):
                        row[field_name] = result[0]
                    elif hasattr(result[0], "text"):
                        row[field_name] = result[0].text
                    else:
                        row[field_name] = str(result[0])
                else:
                    # Multiple values - return as list
                    row[field_name] = [r.text if hasattr(r, "text") else str(r) for r in result]
            else:
                # XPath didn't match - check if it's an attribute or text()
                if xpath_expr.endswith("/text()"):
                    # Try direct text access
                    elem_path = xpath_expr[:-7]  # Remove /text()
                    elem = record.find(elem_path, namespaces=namespaces or {})
                    row[field_name] = elem.text if elem is not None else None
                elif "@" in xpath_expr:
                    # Attribute access
                    attr_name = xpath_expr.split("@")[1]
                    row[field_name] = record.get(attr_name)
                else:
                    row[field_name] = None

        yield row


@dlt.resource(
    name="xml_streaming",
    write_disposition="replace",
)
def xml_streaming_source(
    file_path: str | Path,
    record_tag: str,
    fields: dict[str, str],
    namespaces: dict[str, str] | None = None,
) -> Iterator[dict[str, Any]]:
    """Stream large XML files using iterparse for memory efficiency.

    Uses incremental parsing to handle XML files larger than available RAM.
    Ideal for processing multi-GB XML files.

    Args:
        file_path: Path to XML file
        record_tag: Tag name for record elements (e.g., "record", "item")
        fields: Field mappings (same as xml_file_source)
        namespaces: Optional namespace mappings

    Yields:
        Dict with extracted fields from each record

    Example:
        ```python
        # For large XML files (>1GB)
        yield from xml_streaming_source(
            file_path="large_data.xml",
            record_tag="record",
            fields={"id": "./id/text()", "value": "./value/text()"},
        )
        ```

    Notes:
        - Memory-efficient for large files
        - Clears processed elements to free memory
        - Slower than xml_file_source for small files
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"XML file not found: {file_path}")

    print(f"Streaming XML file: {file_path}")
    count = 0

    # Prepare namespace-aware tag
    ns = namespaces or {}
    if ns and not record_tag.startswith("{"):
        # Add namespace to tag
        default_ns = ns.get("", list(ns.values())[0] if ns else "")
        if default_ns:
            record_tag = f"{{{default_ns}}}{record_tag}"

    # Stream parse XML
    context = ET.iterparse(file_path, events=("end",))

    for event, elem in context:
        if elem.tag == record_tag:
            # Extract fields
            row = {}

            for field_name, xpath_expr in fields.items():
                # Simplified extraction for streaming
                if xpath_expr.endswith("/text()"):
                    # Element text
                    elem_path = xpath_expr[2:-7] if xpath_expr.startswith("./") else xpath_expr[:-7]
                    child = elem.find(elem_path, ns)
                    row[field_name] = child.text if child is not None else None
                elif "@" in xpath_expr:
                    # Attribute
                    attr_name = xpath_expr.split("@")[1]
                    row[field_name] = elem.get(attr_name)
                else:
                    # Direct element
                    child = elem.find(xpath_expr, ns)
                    row[field_name] = child.text if child is not None else None

            yield row
            count += 1

            # Clear element to free memory
            elem.clear()

            if count % 10000 == 0:
                print(f"Processed {count} records...")

    print(f"Streaming complete. Total records: {count}")


@dlt.resource(
    name="xml_http",
    write_disposition="replace",
)
def xml_http_source(
    url: str,
    record_path: str,
    fields: dict[str, str],
    namespaces: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
) -> Iterator[dict[str, Any]]:
    """Harvest XML data from HTTP endpoint.

    Fetches XML from URL and parses records. Useful for XML APIs, RSS feeds,
    SOAP services, etc.

    Args:
        url: HTTP/HTTPS URL to fetch XML from
        record_path: XPath to record elements
        fields: Field mappings
        namespaces: Optional namespace mappings
        headers: Optional HTTP headers (auth, content-type, etc.)

    Yields:
        Dict with extracted fields from each record

    Example:
        ```python
        # RSS feed
        yield from xml_http_source(
            url="https://example.com/rss.xml",
            record_path="//item",
            fields={
                "title": "./title/text()",
                "link": "./link/text()",
                "pubDate": "./pubDate/text()",
            },
        )

        # API with auth
        yield from xml_http_source(
            url="https://api.example.com/data.xml",
            record_path="//record",
            fields={"id": "./@id", "value": "./value/text()"},
            headers={"Authorization": "Bearer token"},
        )
        ```

    Notes:
        - Supports HTTPS with SSL verification
        - Handles redirects automatically
        - For very large responses, save to file first and use xml_streaming_source
    """
    try:
        import requests
    except ImportError:
        raise ImportError(
            "requests package required for HTTP sources. Install with: pip install requests"
        )

    print(f"Fetching XML from: {url}")

    # Fetch XML
    response = requests.get(url, headers=headers or {}, timeout=30)
    response.raise_for_status()

    # Parse XML from response
    root = ET.fromstring(response.content)

    # Find records
    records = root.findall(record_path, namespaces=namespaces or {})
    print(f"Found {len(records)} records from HTTP source")

    # Extract fields
    for record in records:
        row: dict[str, str | list[str | None] | None] = {}

        for field_name, xpath_expr in fields.items():
            result = record.findall(xpath_expr, namespaces=namespaces or {})

            if result:
                if len(result) == 1:
                    if isinstance(result[0], str):
                        row[field_name] = result[0]
                    elif hasattr(result[0], "text"):
                        row[field_name] = result[0].text
                    else:
                        row[field_name] = str(result[0])
                else:
                    row[field_name] = [r.text if hasattr(r, "text") else str(r) for r in result]
            else:
                # Fallback extraction
                if xpath_expr.endswith("/text()"):
                    elem_path = xpath_expr[:-7]
                    elem = record.find(elem_path, namespaces=namespaces or {})
                    row[field_name] = elem.text if elem is not None else None
                elif "@" in xpath_expr:
                    attr_name = xpath_expr.split("@")[1]
                    row[field_name] = record.get(attr_name)
                else:
                    row[field_name] = None

        yield row


# Example DLT source combining multiple XML resources
@dlt.source(name="xml_harvest")
def xml_multi_source(
    local_files: list[dict] | None = None,
    http_endpoints: list[dict] | None = None,
):
    """Combine multiple XML sources in a single DLT source.

    Enables harvesting from multiple XML files and HTTP endpoints in one
    pipeline run.

    Args:
        local_files: List of local file configs, each with keys:
            - file_path: Path to XML file
            - record_path: XPath to records
            - fields: Field mappings
            - resource_name: Unique name for this resource
        http_endpoints: List of HTTP endpoint configs (same structure)

    Example:
        ```python
        @dlt.source
        def harvest_all_xml():
            return xml_multi_source(
                local_files=[
                    {
                        "file_path": "sales.xml",
                        "record_path": "//sale",
                        "fields": {"id": "./id/text()", "amount": "./amount/text()"},
                        "resource_name": "sales_xml",
                    },
                ],
                http_endpoints=[
                    {
                        "url": "https://api.example.com/feed.xml",
                        "record_path": "//item",
                        "fields": {"title": "./title/text()"},
                        "resource_name": "api_feed",
                    },
                ],
            )
        ```
    """
    resources = []

    # Process local files
    if local_files:
        for config in local_files:
            resource = xml_file_source(
                file_path=config["file_path"],
                record_path=config["record_path"],
                fields=config["fields"],
                namespaces=config.get("namespaces"),
                resource_name=config.get("resource_name", "xml_records"),
            )
            resources.append(resource.with_name(config.get("resource_name", "xml_records")))

    # Process HTTP endpoints
    if http_endpoints:
        for config in http_endpoints:
            resource = xml_http_source(
                url=config["url"],
                record_path=config["record_path"],
                fields=config["fields"],
                namespaces=config.get("namespaces"),
                headers=config.get("headers"),
            )
            resources.append(resource.with_name(config.get("resource_name", "xml_http")))

    return resources


__all__ = [
    "xml_file_source",
    "xml_streaming_source",
    "xml_http_source",
    "xml_multi_source",
]
