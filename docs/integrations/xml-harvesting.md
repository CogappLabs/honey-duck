# XML Harvesting

Parse XML files into Polars DataFrames, with nested elements stored as JSON columns.

## Quick Start

```python
import dagster as dg
import polars as pl
from pathlib import Path
from cogapp_libs.dagster import parse_xml_file

@dg.asset(group_name="harvest", kinds={"xml"})
def plant_catalog(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Harvest plant catalog from XML."""
    records = parse_xml_file(
        file_path=Path("examples/data/plant_catalog.xml"),
        record_path="./PLANT",  # Direct children of root
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
    context.add_output_metadata({"row_count": len(df)})
    return df
```

**Output:**
```
shape: (5, 8)
┌──────────┬─────────────┬─────────────────┬────────┬──────────────┬───────┬─────────────────────┬─────────────────────┐
│ plant_id ┆ common_name ┆ botanical_name  ┆ zone   ┆ light        ┆ price ┆ images              ┆ companions          │
╞══════════╪═════════════╪═════════════════╪════════╪══════════════╪═══════╪═════════════════════╪═════════════════════╡
│ P001     ┆ Bloodroot   ┆ Sanguinaria ... ┆ 4      ┆ Mostly Shady ┆ $2.44 ┆ [{"type": "thumb... ┆ [{"value": "Tril... │
│ P002     ┆ Columbine   ┆ Aquilegia ...   ┆ 3      ┆ Mostly Shady ┆ $9.37 ┆ [{"type": "thumb... ┆ [{"value": "Fern"}] │
│ ...      ┆ ...         ┆ ...             ┆ ...    ┆ ...          ┆ ...   ┆ ...                 ┆ ...                 │
└──────────┴─────────────┴─────────────────┴────────┴──────────────┴───────┴─────────────────────┴─────────────────────┘
```

---

## Field Mapping

The `fields` parameter maps output column names to XPath expressions that extract values from each record element.

### Example XML

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CATALOG>
  <PLANT id="P001">
    <COMMON>Bloodroot</COMMON>
    <BOTANICAL>Sanguinaria canadensis</BOTANICAL>
    <ZONE>4</ZONE>
    <LIGHT>Mostly Shady</LIGHT>
    <PRICE>$2.44</PRICE>
    <IMAGES>
      <IMAGE type="thumbnail">bloodroot_thumb.jpg</IMAGE>
      <IMAGE type="full">bloodroot_full.jpg</IMAGE>
    </IMAGES>
    <COMPANIONS>
      <PLANT>Trillium</PLANT>
      <PLANT>Hepatica</PLANT>
    </COMPANIONS>
  </PLANT>
</CATALOG>
```

### Mapping This XML

```python
records = parse_xml_file(
    file_path=Path("plant_catalog.xml"),

    # record_path: Which elements are the "rows"?
    # Use "./PLANT" for direct children of root (not "//PLANT" which matches nested ones too)
    record_path="./PLANT",

    # fields: Simple values - one value per record
    fields={
        "plant_id": "./@id",              # Attribute on PLANT element
        "common_name": "./COMMON/text()", # Text inside <COMMON>
        "botanical_name": "./BOTANICAL/text()",
        "zone": "./ZONE/text()",
        "light": "./LIGHT/text()",
        "price": "./PRICE/text()",
    },

    # nested_fields: Repeated elements → JSON array
    nested_fields={
        "images": "./IMAGES/IMAGE",       # All <IMAGE> elements → JSON
        "companions": "./COMPANIONS/PLANT", # All nested <PLANT> elements → JSON
    },
)
```

### XPath Patterns

| Pattern | What it extracts | Example |
|---------|------------------|---------|
| `./@attr` | Attribute on current element | `./@id` → `"P001"` |
| `./ELEM/text()` | Text content of child element | `./COMMON/text()` → `"Bloodroot"` |
| `./PARENT/CHILD/text()` | Nested element text | `./PRICING/RETAIL/text()` |
| `./PARENT/@attr` | Attribute on child element | `./IMAGE/@type` → `"thumbnail"` |

### Nested Fields → JSON

Elements matched by `nested_fields` are serialized as JSON arrays:

```python
nested_fields={
    "images": "./IMAGES/IMAGE",
}
```

Each `<IMAGE>` element becomes a JSON object with:
- All attributes (e.g., `type="thumbnail"`)
- Text content as `value` (e.g., `"bloodroot_thumb.jpg"`)

**Result:**
```json
[
  {"type": "thumbnail", "value": "bloodroot_thumb.jpg"},
  {"type": "full", "value": "bloodroot_full.jpg"}
]
```

---

## S3 Files

```python
from cogapp_libs.dagster import parse_xml_s3

@dg.asset(group_name="harvest", kinds={"xml", "s3"})
def s3_xml_harvest(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Harvest XML from S3 bucket."""
    records = parse_xml_s3(
        bucket="my-data-bucket",
        key="catalogs/plants.xml",
        record_path="./PLANT",
        fields={
            "plant_id": "./@id",
            "common_name": "./COMMON/text()",
        },
        # For LocalStack:
        # endpoint_url="http://localhost:4566",
    )

    return pl.DataFrame(records)
```

---

## HTTP/RSS Feeds

```python
from cogapp_libs.dagster import parse_xml_http

@dg.asset(group_name="harvest", kinds={"xml", "http"})
def rss_feed_harvest(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Harvest articles from RSS feed."""
    records = parse_xml_http(
        url="https://example.com/feed.rss",
        record_path=".//item",
        fields={
            "title": "./title/text()",
            "link": "./link/text()",
            "pub_date": "./pubDate/text()",
        },
    )

    return pl.DataFrame(records)
```

---

## Large File Streaming

For files >500MB, use streaming to avoid memory issues:

```python
from cogapp_libs.dagster import parse_xml_streaming

@dg.asset(group_name="harvest", kinds={"xml"})
def large_xml_harvest(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Stream large XML file with constant memory."""
    records = list(parse_xml_streaming(
        file_path=Path("data/large_catalog.xml"),
        record_tag="PLANT",  # Tag name, NOT XPath
        fields={
            "plant_id": "./@id",
            "common_name": "./COMMON/text()",
        },
    ))

    return pl.DataFrame(records)
```

!!! warning "record_tag vs record_path"
    Streaming uses `record_tag` (just the tag name like `"PLANT"`) instead of
    `record_path` (XPath like `"./PLANT"`). This is required for incremental parsing.

---

## Namespace Handling

For namespaced XML (Atom, SOAP, OAI-PMH):

```python
records = parse_xml_file(
    file_path=Path("feed.xml"),
    record_path=".//atom:entry",
    fields={
        "id": "./atom:id/text()",
        "title": "./atom:title/text()",
        "author": "./atom:author/atom:name/text()",
    },
    namespaces={
        "atom": "http://www.w3.org/2005/Atom",
    },
)
```

---

## Available Functions

| Function | Use Case |
|----------|----------|
| `parse_xml_file` | Local XML files |
| `parse_xml_string` | XML content as string/bytes |
| `parse_xml_s3` | S3 buckets (with optional LocalStack) |
| `parse_xml_http` | HTTP endpoints, RSS, APIs |
| `parse_xml_streaming` | Large files (>500MB) |

All functions accept:
- `fields`: Simple value extraction (one value per record)
- `nested_fields`: Repeated elements → JSON arrays
- `namespaces`: XML namespace prefix mappings

---

## Troubleshooting

### No records found

Your `record_path` doesn't match any elements. Debug with:

```python
import xml.etree.ElementTree as ET
tree = ET.parse("data.xml")
root = tree.getroot()
print(f"Root tag: {root.tag}")
print(f"Children: {[child.tag for child in root]}")
```

### Getting duplicate/nested records

If using `//ELEM`, it matches ALL elements with that tag, including nested ones.
Use `./ELEM` to match only direct children of the root.

```python
# BAD: Matches <PLANT> inside <COMPANIONS> too
record_path="//PLANT"

# GOOD: Only top-level <PLANT> elements
record_path="./PLANT"
```

### Namespace prefix not defined

Add namespace mappings:

```python
namespaces = {"ns": "http://example.com/schema"}
record_path = ".//ns:record"
fields = {"id": "./ns:id/text()"}
```

---

## API Reference

::: cogapp_libs.dagster.xml_sources
    options:
      show_root_heading: false
      members:
        - parse_xml_file
        - parse_xml_string
        - parse_xml_streaming
        - parse_xml_http
        - parse_xml_s3
