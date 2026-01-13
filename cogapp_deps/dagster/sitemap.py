"""Sitemap generation utilities for Dagster pipelines.

Provides helpers for generating XML sitemaps from pipeline data for SEO and
search engine discoverability. Supports standard sitemaps, sitemap indices,
image sitemaps, and video sitemaps.

Example:
    ```python
    from cogapp_deps.dagster import create_sitemap_asset

    # Create sitemap from asset data
    sitemap = create_sitemap_asset(
        name="sitemap_artworks",
        source_asset="artworks_catalog",
        url_column="artwork_url",
        lastmod_column="updated_at",
        base_url="https://example.com",
    )
    ```
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

import dagster as dg

if TYPE_CHECKING:
    import polars as pl


# Sitemap constants
SITEMAP_NS = "http://www.sitemaps.org/schemas/sitemap/0.9"
IMAGE_NS = "http://www.google.com/schemas/sitemap-image/1.1"
VIDEO_NS = "http://www.google.com/schemas/sitemap-video/1.1"
NEWS_NS = "http://www.google.com/schemas/sitemap-news/0.9"

MAX_URLS_PER_SITEMAP = 50_000  # Google limit
MAX_SITEMAP_SIZE_BYTES = 50_000_000  # 50MB limit


def generate_sitemap_xml(
    urls: list[dict],
    output_path: Path | str,
    pretty: bool = True,
) -> int:
    """Generate standard sitemap.xml file.

    Args:
        urls: List of URL dictionaries with keys:
            - loc (required): URL of the page
            - lastmod (optional): Last modification date (ISO 8601)
            - changefreq (optional): How frequently the page changes
            - priority (optional): Priority of this URL (0.0-1.0)
        output_path: Path to write sitemap.xml
        pretty: Whether to pretty-print XML (default: True)

    Returns:
        Number of URLs written

    Example:
        ```python
        urls = [
            {
                "loc": "https://example.com/artwork/1",
                "lastmod": "2024-01-15",
                "changefreq": "monthly",
                "priority": "0.8",
            },
        ]
        generate_sitemap_xml(urls, "sitemap.xml")
        # Returns: 1
        ```
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Create XML structure
    urlset = Element("urlset")
    urlset.set("xmlns", SITEMAP_NS)

    for url_data in urls:
        url_elem = SubElement(urlset, "url")

        # Required: loc
        loc = SubElement(url_elem, "loc")
        loc.text = url_data["loc"]

        # Optional: lastmod
        if "lastmod" in url_data and url_data["lastmod"]:
            lastmod = SubElement(url_elem, "lastmod")
            lastmod.text = str(url_data["lastmod"])

        # Optional: changefreq
        if "changefreq" in url_data and url_data["changefreq"]:
            changefreq = SubElement(url_elem, "changefreq")
            changefreq.text = url_data["changefreq"]

        # Optional: priority
        if "priority" in url_data and url_data["priority"]:
            priority = SubElement(url_elem, "priority")
            priority.text = str(url_data["priority"])

    # Write to file
    if pretty:
        xml_str = minidom.parseString(tostring(urlset)).toprettyxml(indent="  ")
        # Remove extra blank lines
        xml_str = "\n".join([line for line in xml_str.split("\n") if line.strip()])
    else:
        xml_str = tostring(urlset, encoding="unicode")

    output_path.write_text(xml_str, encoding="utf-8")

    return len(urls)


def generate_image_sitemap_xml(
    urls: list[dict],
    output_path: Path | str,
    pretty: bool = True,
) -> int:
    """Generate image sitemap with image:image tags.

    Args:
        urls: List of URL dictionaries with keys:
            - loc (required): URL of the page
            - images (required): List of image dicts with:
                - loc (required): Image URL
                - title (optional): Image title
                - caption (optional): Image caption
                - geo_location (optional): Geographic location
                - license (optional): License URL
        output_path: Path to write sitemap.xml
        pretty: Whether to pretty-print XML

    Returns:
        Number of URLs written

    Example:
        ```python
        urls = [
            {
                "loc": "https://example.com/artwork/1",
                "images": [
                    {
                        "loc": "https://example.com/images/artwork1.jpg",
                        "title": "Artwork Title",
                        "caption": "Beautiful painting",
                    }
                ],
            },
        ]
        generate_image_sitemap_xml(urls, "sitemap_images.xml")
        # Returns: 1
        ```
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Create XML structure
    urlset = Element("urlset")
    urlset.set("xmlns", SITEMAP_NS)
    urlset.set("xmlns:image", IMAGE_NS)

    for url_data in urls:
        url_elem = SubElement(urlset, "url")

        # Page URL
        loc = SubElement(url_elem, "loc")
        loc.text = url_data["loc"]

        # Image tags
        for image_data in url_data.get("images", []):
            image_elem = SubElement(url_elem, "{%s}image" % IMAGE_NS)

            # Required: image:loc
            image_loc = SubElement(image_elem, "{%s}loc" % IMAGE_NS)
            image_loc.text = image_data["loc"]

            # Optional: image:title
            if "title" in image_data:
                image_title = SubElement(image_elem, "{%s}title" % IMAGE_NS)
                image_title.text = image_data["title"]

            # Optional: image:caption
            if "caption" in image_data:
                image_caption = SubElement(image_elem, "{%s}caption" % IMAGE_NS)
                image_caption.text = image_data["caption"]

            # Optional: image:geo_location
            if "geo_location" in image_data:
                geo = SubElement(image_elem, "{%s}geo_location" % IMAGE_NS)
                geo.text = image_data["geo_location"]

            # Optional: image:license
            if "license" in image_data:
                license_elem = SubElement(image_elem, "{%s}license" % IMAGE_NS)
                license_elem.text = image_data["license"]

    # Write to file
    if pretty:
        xml_str = minidom.parseString(tostring(urlset)).toprettyxml(indent="  ")
        xml_str = "\n".join([line for line in xml_str.split("\n") if line.strip()])
    else:
        xml_str = tostring(urlset, encoding="unicode")

    output_path.write_text(xml_str, encoding="utf-8")

    return len(urls)


def generate_sitemap_index_xml(
    sitemaps: list[dict],
    output_path: Path | str,
    pretty: bool = True,
) -> int:
    """Generate sitemap index file (for multiple sitemaps).

    Args:
        sitemaps: List of sitemap dictionaries with keys:
            - loc (required): URL of the sitemap
            - lastmod (optional): Last modification date
        output_path: Path to write sitemap_index.xml
        pretty: Whether to pretty-print XML

    Returns:
        Number of sitemaps in index

    Example:
        ```python
        sitemaps = [
            {
                "loc": "https://example.com/sitemap_artworks.xml",
                "lastmod": "2024-01-15",
            },
            {
                "loc": "https://example.com/sitemap_images.xml",
                "lastmod": "2024-01-15",
            },
        ]
        generate_sitemap_index_xml(sitemaps, "sitemap_index.xml")
        # Returns: 2
        ```
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Create XML structure
    sitemapindex = Element("sitemapindex")
    sitemapindex.set("xmlns", SITEMAP_NS)

    for sitemap_data in sitemaps:
        sitemap_elem = SubElement(sitemapindex, "sitemap")

        # Required: loc
        loc = SubElement(sitemap_elem, "loc")
        loc.text = sitemap_data["loc"]

        # Optional: lastmod
        if "lastmod" in sitemap_data and sitemap_data["lastmod"]:
            lastmod = SubElement(sitemap_elem, "lastmod")
            lastmod.text = str(sitemap_data["lastmod"])

    # Write to file
    if pretty:
        xml_str = minidom.parseString(tostring(sitemapindex)).toprettyxml(indent="  ")
        xml_str = "\n".join([line for line in xml_str.split("\n") if line.strip()])
    else:
        xml_str = tostring(sitemapindex, encoding="unicode")

    output_path.write_text(xml_str, encoding="utf-8")

    return len(sitemaps)


def create_sitemap_asset(
    name: str,
    source_asset: str,
    url_column: str,
    base_url: str = "",
    lastmod_column: str | None = None,
    changefreq: Literal["always", "hourly", "daily", "weekly", "monthly", "yearly", "never"]
    | None = None,
    priority: float | str | None = None,
    output_path: str | Path | None = None,
    group_name: str = "sitemaps",
) -> dg.AssetsDefinition:
    """Create a sitemap asset from a source DataFrame asset.

    Args:
        name: Name of the sitemap asset
        source_asset: Name of source asset (must return DataFrame)
        url_column: Column containing URLs (or URL path if base_url provided)
        base_url: Base URL to prepend to paths (e.g., "https://example.com")
        lastmod_column: Column containing last modification dates
        changefreq: Change frequency for all URLs
        priority: Priority for all URLs (0.0-1.0) or column name
        output_path: Path to write sitemap.xml (default: data/output/sitemaps/{name}.xml)
        group_name: Dagster group name

    Returns:
        Sitemap asset definition

    Example:
        ```python
        from cogapp_deps.dagster import create_sitemap_asset

        sitemap = create_sitemap_asset(
            name="sitemap_artworks",
            source_asset="artworks_catalog",
            url_column="slug",
            base_url="https://example.com/artworks",
            lastmod_column="updated_at",
            changefreq="monthly",
            priority="0.8",
        )
        ```
    """

    if output_path is None:
        output_path = Path(f"data/output/sitemaps/{name}.xml")
    else:
        output_path = Path(output_path)

    @dg.asset(
        name=name,
        kinds={"sitemap", "xml"},
        group_name=group_name,
    )
    def sitemap_asset(context: dg.AssetExecutionContext, **kwargs) -> dict:
        """Generate sitemap from source data."""
        # Get source DataFrame
        source_df: pl.DataFrame = kwargs[source_asset]

        # Build URLs
        urls = []
        for row in source_df.iter_rows(named=True):
            url_data = {}

            # Build full URL
            url_path = row[url_column]
            if base_url:
                # Ensure no double slashes
                url_data["loc"] = f"{base_url.rstrip('/')}/{url_path.lstrip('/')}"
            else:
                url_data["loc"] = url_path

            # Add lastmod if provided
            if lastmod_column and lastmod_column in row:
                lastmod_value = row[lastmod_column]
                if lastmod_value:
                    # Convert to ISO 8601 format
                    if isinstance(lastmod_value, datetime):
                        url_data["lastmod"] = lastmod_value.strftime("%Y-%m-%d")
                    else:
                        url_data["lastmod"] = str(lastmod_value)

            # Add changefreq if provided
            if changefreq:
                url_data["changefreq"] = changefreq

            # Add priority if provided
            if priority:
                # Check if priority is a column name or a value
                if isinstance(priority, str) and priority in row:
                    url_data["priority"] = str(row[priority])
                else:
                    url_data["priority"] = str(priority)

            urls.append(url_data)

        # Generate sitemap
        url_count = generate_sitemap_xml(urls, output_path)

        context.log.info(f"Generated sitemap with {url_count:,} URLs at {output_path}")

        # Add metadata
        context.add_output_metadata(
            {
                "url_count": url_count,
                "sitemap_path": str(output_path),
                "sitemap_size_bytes": output_path.stat().st_size,
                "base_url": base_url or "n/a",
            }
        )

        return {
            "sitemap_path": str(output_path),
            "url_count": url_count,
        }

    return sitemap_asset


def create_image_sitemap_asset(
    name: str,
    source_asset: str,
    page_url_column: str,
    image_url_column: str,
    base_url: str = "",
    image_base_url: str = "",
    title_column: str | None = None,
    caption_column: str | None = None,
    output_path: str | Path | None = None,
    group_name: str = "sitemaps",
) -> dg.AssetsDefinition:
    """Create an image sitemap asset from a source DataFrame.

    Args:
        name: Name of the sitemap asset
        source_asset: Name of source asset
        page_url_column: Column containing page URLs
        image_url_column: Column containing image URLs
        base_url: Base URL for pages
        image_base_url: Base URL for images (defaults to base_url)
        title_column: Column containing image titles
        caption_column: Column containing image captions
        output_path: Path to write sitemap
        group_name: Dagster group name

    Returns:
        Image sitemap asset definition

    Example:
        ```python
        sitemap = create_image_sitemap_asset(
            name="sitemap_images",
            source_asset="artworks_with_images",
            page_url_column="artwork_url",
            image_url_column="image_url",
            base_url="https://example.com",
            title_column="artwork_title",
        )
        ```
    """

    if output_path is None:
        output_path = Path(f"data/output/sitemaps/{name}.xml")
    else:
        output_path = Path(output_path)

    if not image_base_url:
        image_base_url = base_url

    @dg.asset(
        name=name,
        kinds={"sitemap", "xml", "images"},
        group_name=group_name,
    )
    def image_sitemap_asset(context: dg.AssetExecutionContext, **kwargs) -> dict:
        """Generate image sitemap from source data."""
        source_df: pl.DataFrame = kwargs[source_asset]

        # Group by page URL and collect images
        from collections import defaultdict

        url_images = defaultdict(list)

        for row in source_df.iter_rows(named=True):
            page_url = row[page_url_column]
            if base_url:
                page_url = f"{base_url.rstrip('/')}/{page_url.lstrip('/')}"

            image_data = {}

            # Image URL
            image_url = row[image_url_column]
            if image_base_url:
                image_data["loc"] = f"{image_base_url.rstrip('/')}/{image_url.lstrip('/')}"
            else:
                image_data["loc"] = image_url

            # Optional fields
            if title_column and title_column in row:
                image_data["title"] = str(row[title_column])

            if caption_column and caption_column in row:
                image_data["caption"] = str(row[caption_column])

            url_images[page_url].append(image_data)

        # Build URL list
        urls = [{"loc": page_url, "images": images} for page_url, images in url_images.items()]

        # Generate sitemap
        url_count = generate_image_sitemap_xml(urls, output_path)

        total_images = sum(len(url["images"]) for url in urls)

        context.log.info(
            f"Generated image sitemap with {url_count:,} pages "
            f"and {total_images:,} images at {output_path}"
        )

        context.add_output_metadata(
            {
                "page_count": url_count,
                "image_count": total_images,
                "sitemap_path": str(output_path),
                "sitemap_size_bytes": output_path.stat().st_size,
            }
        )

        return {
            "sitemap_path": str(output_path),
            "page_count": url_count,
            "image_count": total_images,
        }

    return image_sitemap_asset


__all__ = [
    "generate_sitemap_xml",
    "generate_image_sitemap_xml",
    "generate_sitemap_index_xml",
    "create_sitemap_asset",
    "create_image_sitemap_asset",
]
