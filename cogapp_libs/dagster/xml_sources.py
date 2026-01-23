"""XML parsing utilities for harvesting XML data into DataFrames.

Simple, dlt-free XML parsing with XPath field mappings. Nested elements
are serialized as JSON strings for storage in Parquet.

Example:
    ```python
    from cogapp_libs.dagster.xml_sources import parse_xml_file

    records = parse_xml_file(
        file_path="data.xml",
        record_path="//record",
        fields={
            "id": "./@id",
            "name": "./name/text()",
        },
    )
    df = pl.DataFrame(records)
    ```
"""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Iterator


def parse_xml_file(
    file_path: str | Path,
    record_path: str,
    fields: dict[str, str],
    nested_fields: dict[str, str] | None = None,
    namespaces: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Parse XML file using XPath field mappings.

    Args:
        file_path: Path to XML file
        record_path: XPath to locate record elements (e.g., "//record", ".//item")
        fields: Mapping of field names to XPath expressions relative to record.
            Example: {"id": "./@id", "name": "./name/text()"}
        nested_fields: Mapping of field names to XPath for nested elements.
            These are serialized as JSON strings.
            Example: {"tags": "./tags/tag", "media": "./media/item"}
        namespaces: Optional namespace mappings for XPath.
            Example: {"ns": "http://example.com/schema"}

    Returns:
        List of dictionaries, one per record.

    Example:
        ```python
        # XML structure:
        # <catalog>
        #   <artwork id="1">
        #     <title>Starry Night</title>
        #     <tags><tag>landscape</tag><tag>night</tag></tags>
        #   </artwork>
        # </catalog>

        records = parse_xml_file(
            file_path="catalog.xml",
            record_path="//artwork",
            fields={
                "artwork_id": "./@id",
                "title": "./title/text()",
            },
            nested_fields={
                "tags": "./tags/tag",
            },
        )
        # Returns: [{"artwork_id": "1", "title": "Starry Night", "tags": '[...]'}]
        ```
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"XML file not found: {file_path}")

    tree = ET.parse(file_path)
    root = tree.getroot()
    ns = namespaces or {}
    nested = nested_fields or {}

    # Convert absolute XPath to relative for element.findall()
    # ElementTree's findall on an element doesn't support "//" at start
    search_path = record_path
    if search_path.startswith("//"):
        search_path = "." + search_path

    records = []
    for elem in root.findall(search_path, ns):
        row = _extract_fields(elem, fields, ns)
        row.update(_extract_nested_fields(elem, nested, ns))
        records.append(row)

    return records


def parse_xml_string(
    xml_content: str | bytes,
    record_path: str,
    fields: dict[str, str],
    nested_fields: dict[str, str] | None = None,
    namespaces: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Parse XML string using XPath field mappings.

    Same as parse_xml_file but accepts XML content directly.

    Args:
        xml_content: XML content as string or bytes
        record_path: XPath to locate record elements
        fields: Mapping of field names to XPath expressions
        nested_fields: Mapping of field names to XPath for nested elements
        namespaces: Optional namespace mappings

    Returns:
        List of dictionaries, one per record.
    """
    root = ET.fromstring(xml_content)
    ns = namespaces or {}
    nested = nested_fields or {}

    # Convert absolute XPath to relative for element.findall()
    # ElementTree's findall on an element doesn't support "//" at start
    search_path = record_path
    if search_path.startswith("//"):
        search_path = "." + search_path

    records = []
    for elem in root.findall(search_path, ns):
        row = _extract_fields(elem, fields, ns)
        row.update(_extract_nested_fields(elem, nested, ns))
        records.append(row)

    return records


def parse_xml_streaming(
    file_path: str | Path,
    record_tag: str,
    fields: dict[str, str],
    nested_fields: dict[str, str] | None = None,
    namespaces: dict[str, str] | None = None,
) -> Iterator[dict[str, Any]]:
    """Stream large XML files using iterparse for memory efficiency.

    Uses incremental parsing to handle XML files larger than available RAM.

    Args:
        file_path: Path to XML file
        record_tag: Tag name for record elements (e.g., "record", "item").
            Note: This is a tag name, NOT an XPath expression.
        fields: Field mappings (XPath expressions relative to record element)
        nested_fields: Nested field mappings (serialized as JSON)
        namespaces: Optional namespace mappings

    Yields:
        Dictionary for each record.

    Example:
        ```python
        # For large XML files (>500MB)
        for record in parse_xml_streaming(
            file_path="large_catalog.xml",
            record_tag="artwork",
            fields={"id": "./@id", "title": "./title/text()"},
        ):
            process(record)
        ```
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"XML file not found: {file_path}")

    ns = namespaces or {}
    nested = nested_fields or {}

    # Handle namespaced tags
    tag_to_match = record_tag
    if ns and not record_tag.startswith("{"):
        default_ns = ns.get("", next(iter(ns.values()), ""))
        if default_ns:
            tag_to_match = f"{{{default_ns}}}{record_tag}"

    context = ET.iterparse(file_path, events=("end",))

    for _, elem in context:
        if elem.tag == tag_to_match or elem.tag == record_tag:
            row = _extract_fields(elem, fields, ns)
            row.update(_extract_nested_fields(elem, nested, ns))
            yield row

            # Clear element to free memory
            elem.clear()


def parse_xml_http(
    url: str,
    record_path: str,
    fields: dict[str, str],
    nested_fields: dict[str, str] | None = None,
    namespaces: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 30,
) -> list[dict[str, Any]]:
    """Fetch and parse XML from HTTP endpoint.

    Args:
        url: HTTP/HTTPS URL to fetch XML from
        record_path: XPath to record elements
        fields: Field mappings
        nested_fields: Nested field mappings (serialized as JSON)
        namespaces: Optional namespace mappings
        headers: Optional HTTP headers (auth, content-type, etc.)
        timeout: Request timeout in seconds

    Returns:
        List of dictionaries, one per record.

    Example:
        ```python
        # RSS feed
        records = parse_xml_http(
            url="https://example.com/feed.rss",
            record_path="//item",
            fields={
                "title": "./title/text()",
                "link": "./link/text()",
                "pub_date": "./pubDate/text()",
            },
        )

        # API with auth
        records = parse_xml_http(
            url="https://api.example.com/data.xml",
            record_path="//record",
            fields={"id": "./@id", "value": "./value/text()"},
            headers={"Authorization": "Bearer token"},
        )
        ```
    """
    try:
        import requests
    except ImportError:
        raise ImportError(
            "requests package required for HTTP sources. Install with: pip install requests"
        )

    response = requests.get(url, headers=headers or {}, timeout=timeout)
    response.raise_for_status()

    return parse_xml_string(
        xml_content=response.content,
        record_path=record_path,
        fields=fields,
        nested_fields=nested_fields,
        namespaces=namespaces,
    )


def parse_xml_s3(
    bucket: str,
    key: str,
    record_path: str,
    fields: dict[str, str],
    nested_fields: dict[str, str] | None = None,
    namespaces: dict[str, str] | None = None,
    s3_client: Any | None = None,
    endpoint_url: str | None = None,
) -> list[dict[str, Any]]:
    """Download and parse XML from S3.

    Args:
        bucket: S3 bucket name
        key: S3 object key (path to XML file)
        record_path: XPath to record elements
        fields: Field mappings
        nested_fields: Nested field mappings (serialized as JSON)
        namespaces: Optional namespace mappings
        s3_client: Optional pre-configured boto3 S3 client
        endpoint_url: Optional S3 endpoint (for LocalStack/MinIO)

    Returns:
        List of dictionaries, one per record.

    Example:
        ```python
        records = parse_xml_s3(
            bucket="my-bucket",
            key="data/catalog.xml",
            record_path="//artwork",
            fields={"id": "./@id", "title": "./title/text()"},
        )

        # With LocalStack
        records = parse_xml_s3(
            bucket="test-bucket",
            key="catalog.xml",
            record_path="//artwork",
            fields={...},
            endpoint_url="http://localhost:4566",
        )
        ```
    """
    try:
        import boto3
    except ImportError:
        raise ImportError("boto3 package required for S3 sources. Install with: pip install boto3")

    if s3_client is None:
        config = {"endpoint_url": endpoint_url} if endpoint_url else {}
        s3_client = boto3.client("s3", **config)

    response = s3_client.get_object(Bucket=bucket, Key=key)
    xml_content = response["Body"].read()

    return parse_xml_string(
        xml_content=xml_content,
        record_path=record_path,
        fields=fields,
        nested_fields=nested_fields,
        namespaces=namespaces,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _extract_fields(
    elem: ET.Element,
    fields: dict[str, str],
    namespaces: dict[str, str],
) -> dict[str, Any]:
    """Extract simple fields from an element using XPath."""
    row: dict[str, Any] = {}

    for field_name, xpath_expr in fields.items():
        value = _extract_value(elem, xpath_expr, namespaces)
        row[field_name] = value

    return row


def _extract_value(
    elem: ET.Element,
    xpath_expr: str,
    namespaces: dict[str, str],
) -> str | list[str] | None:
    """Extract a single value or list of values from an element."""
    # Handle text() extraction
    if xpath_expr.endswith("/text()"):
        elem_path = xpath_expr[:-7]  # Remove /text()
        if elem_path.startswith("./"):
            elem_path = elem_path[2:]

        if not elem_path:
            return elem.text

        child = elem.find(elem_path, namespaces)
        return child.text if child is not None else None

    # Handle attribute extraction
    if "@" in xpath_expr:
        if xpath_expr.startswith("./@"):
            # Direct attribute on current element
            attr_name = xpath_expr[3:]
            return elem.get(attr_name)
        else:
            # Attribute on child element
            parts = xpath_expr.rsplit("/@", 1)
            elem_path = parts[0]
            if elem_path.startswith("./"):
                elem_path = elem_path[2:]
            attr_name = parts[1]

            child = elem.find(elem_path, namespaces) if elem_path else elem
            return child.get(attr_name) if child is not None else None

    # Direct element - try to find and get text
    elem_path = xpath_expr[2:] if xpath_expr.startswith("./") else xpath_expr
    if not elem_path:
        return elem.text

    # Check for multiple matches
    matches = elem.findall(elem_path, namespaces)
    if len(matches) == 0:
        return None
    elif len(matches) == 1:
        return matches[0].text
    else:
        # Multiple matches - return as list
        return [m.text for m in matches if m.text]


def _extract_nested_fields(
    elem: ET.Element,
    nested_fields: dict[str, str],
    namespaces: dict[str, str],
) -> dict[str, str | None]:
    """Extract nested elements and serialize as JSON strings."""
    row: dict[str, str | None] = {}

    for field_name, xpath_expr in nested_fields.items():
        elem_path = xpath_expr[2:] if xpath_expr.startswith("./") else xpath_expr
        nested_elems = elem.findall(elem_path, namespaces)

        if not nested_elems:
            row[field_name] = None
            continue

        nested_data = []
        for nested in nested_elems:
            # Extract all child elements as dict
            item: dict[str, Any] = {}

            # Add attributes
            item.update(nested.attrib)

            # Add child element text
            for child in nested:
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                item[tag] = child.text

            # Add direct text content if present
            if nested.text and nested.text.strip():
                item["value"] = nested.text.strip()

            nested_data.append(item)

        row[field_name] = json.dumps(nested_data)

    return row


__all__ = [
    "parse_xml_file",
    "parse_xml_string",
    "parse_xml_streaming",
    "parse_xml_http",
    "parse_xml_s3",
]
