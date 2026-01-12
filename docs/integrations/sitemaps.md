# Sitemap Generation Guide

Complete guide to generating XML sitemaps from Dagster pipeline data for SEO and search engine discoverability.

## What are Sitemaps?

Sitemaps are XML files that inform search engines about the pages, images, and other content on your site. They help search engines:
- Discover all pages on your site
- Understand your site structure
- Know when pages were last updated
- Prioritize which pages to crawl
- Index images and videos

**Google supports:**
- Standard sitemaps (up to 50,000 URLs per file)
- Image sitemaps (enhance image search results)
- Video sitemaps (video content discovery)
- News sitemaps (news articles)
- Sitemap indices (collections of multiple sitemaps)

---

## Quick Start

### 1. Install (Already Included)

Sitemap utilities are part of `cogapp_deps.dagster`:

```python
from cogapp_deps.dagster import (
    create_sitemap_asset,
    create_image_sitemap_asset,
    generate_sitemap_xml,
)
```

### 2. Create a Standard Sitemap

```python
import dagster as dg
from cogapp_deps.dagster import create_sitemap_asset

# Define your data asset
@dg.asset
def artworks_catalog() -> pl.DataFrame:
    return pl.DataFrame({
        "artwork_id": [1, 2, 3],
        "slug": ["mona-lisa", "starry-night", "the-scream"],
        "updated_at": ["2024-01-15", "2024-01-14", "2024-01-13"],
    })

# Create sitemap asset automatically
sitemap_artworks = create_sitemap_asset(
    name="sitemap_artworks",
    source_asset="artworks_catalog",
    url_column="slug",
    base_url="https://example.com/artworks",
    lastmod_column="updated_at",
    changefreq="monthly",
    priority="0.8",
)

# That's it! When materialized, generates:
# data/output/sitemaps/sitemap_artworks.xml
```

### 3. Submit to Google

```bash
# Sitemap location
https://example.com/sitemap.xml

# Submit via Google Search Console
# Or add to robots.txt:
echo "Sitemap: https://example.com/sitemap.xml" >> robots.txt
```

---

## Standard Sitemap

### Basic Example

```python
from cogapp_deps.dagster import create_sitemap_asset

sitemap = create_sitemap_asset(
    name="sitemap_main",
    source_asset="published_pages",
    url_column="url_path",
    base_url="https://cogapp.com",
    lastmod_column="published_date",
    changefreq="weekly",
    priority="0.7",
)
```

**Input DataFrame:**
```python
pl.DataFrame({
    "url_path": ["/about", "/services", "/contact"],
    "published_date": ["2024-01-10", "2024-01-12", "2024-01-15"],
})
```

**Output XML (`sitemap_main.xml`):**
```xml
<?xml version="1.0" ?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://cogapp.com/about</loc>
    <lastmod>2024-01-10</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.7</priority>
  </url>
  <url>
    <loc>https://cogapp.com/services</loc>
    <lastmod>2024-01-12</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.7</priority>
  </url>
  <url>
    <loc>https://cogapp.com/contact</loc>
    <lastmod>2024-01-15</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.7</priority>
  </url>
</urlset>
```

---

## Image Sitemap

For better image search results in Google Images.

### Example

```python
from cogapp_deps.dagster import create_image_sitemap_asset

sitemap_images = create_image_sitemap_asset(
    name="sitemap_images",
    source_asset="artworks_with_images",
    page_url_column="artwork_slug",
    image_url_column="image_path",
    base_url="https://example.com/artworks",
    image_base_url="https://cdn.example.com",
    title_column="artwork_title",
    caption_column="description",
)
```

**Input DataFrame:**
```python
pl.DataFrame({
    "artwork_slug": ["mona-lisa", "mona-lisa", "starry-night"],
    "image_path": ["/images/mona-lisa-main.jpg", "/images/mona-lisa-detail.jpg", "/images/starry-night.jpg"],
    "artwork_title": ["Mona Lisa", "Mona Lisa (Detail)", "The Starry Night"],
    "description": ["Famous portrait by Leonardo da Vinci", "Close-up of the smile", "Vincent van Gogh's masterpiece"],
})
```

**Output XML:**
```xml
<?xml version="1.0" ?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
        xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">
  <url>
    <loc>https://example.com/artworks/mona-lisa</loc>
    <image:image>
      <image:loc>https://cdn.example.com/images/mona-lisa-main.jpg</image:loc>
      <image:title>Mona Lisa</image:title>
      <image:caption>Famous portrait by Leonardo da Vinci</image:caption>
    </image:image>
    <image:image>
      <image:loc>https://cdn.example.com/images/mona-lisa-detail.jpg</image:loc>
      <image:title>Mona Lisa (Detail)</image:title>
      <image:caption>Close-up of the smile</image:caption>
    </image:image>
  </url>
  <url>
    <loc>https://example.com/artworks/starry-night</loc>
    <image:image>
      <image:loc>https://cdn.example.com/images/starry-night.jpg</image:loc>
      <image:title>The Starry Night</image:title>
      <image:caption>Vincent van Gogh's masterpiece</image:caption>
    </image:image>
  </url>
</urlset>
```

---

## Sitemap Index

When you have multiple sitemaps (> 50,000 URLs or multiple content types).

### Example

```python
@dg.asset
def sitemap_index(
    context: dg.AssetExecutionContext,
    sitemap_artworks: dict,
    sitemap_images: dict,
    sitemap_artists: dict,
) -> dict:
    """Create sitemap index referencing all sitemaps."""
    from cogapp_deps.dagster import generate_sitemap_index_xml
    from datetime import datetime

    sitemaps = [
        {
            "loc": "https://example.com/sitemap_artworks.xml",
            "lastmod": datetime.now().strftime("%Y-%m-%d"),
        },
        {
            "loc": "https://example.com/sitemap_images.xml",
            "lastmod": datetime.now().strftime("%Y-%m-%d"),
        },
        {
            "loc": "https://example.com/sitemap_artists.xml",
            "lastmod": datetime.now().strftime("%Y-%m-%d"),
        },
    ]

    output_path = Path("data/output/sitemaps/sitemap_index.xml")
    count = generate_sitemap_index_xml(sitemaps, output_path)

    context.log.info(f"Created sitemap index with {count} sitemaps")

    return {"sitemap_path": str(output_path), "sitemap_count": count}
```

**Output XML (`sitemap_index.xml`):**
```xml
<?xml version="1.0" ?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap>
    <loc>https://example.com/sitemap_artworks.xml</loc>
    <lastmod>2024-01-15</lastmod>
  </sitemap>
  <sitemap>
    <loc>https://example.com/sitemap_images.xml</loc>
    <lastmod>2024-01-15</lastmod>
  </sitemap>
  <sitemap>
    <loc>https://example.com/sitemap_artists.xml</loc>
    <lastmod>2024-01-15</lastmod>
  </sitemap>
</sitemapindex>
```

---

## Advanced Usage

### Dynamic Priority Based on Data

```python
# Calculate priority from data (e.g., view count)
@dg.asset
def artworks_with_priority() -> pl.DataFrame:
    return pl.DataFrame({
        "slug": ["mona-lisa", "unknown-work"],
        "view_count": [1000000, 100],
    }).with_columns(
        # Normalize views to 0.5-1.0 priority range
        (pl.col("view_count") / pl.col("view_count").max() * 0.5 + 0.5)
        .alias("priority")
    )

sitemap = create_sitemap_asset(
    name="sitemap_dynamic",
    source_asset="artworks_with_priority",
    url_column="slug",
    base_url="https://example.com/artworks",
    priority="priority",  # Column name, not fixed value
)
```

### Multiple Language Versions

```python
# Generate sitemaps for each language
for lang in ["en", "fr", "es"]:
    create_sitemap_asset(
        name=f"sitemap_{lang}",
        source_asset=f"pages_{lang}",
        url_column="slug",
        base_url=f"https://example.com/{lang}",
    )
```

### Custom Changefreq Logic

```python
@dg.asset
def pages_with_changefreq() -> pl.DataFrame:
    return pl.DataFrame({
        "slug": ["/news/article", "/about", "/archive"],
        "page_type": ["news", "static", "archive"],
    }).with_columns(
        pl.col("page_type")
        .map_dict({
            "news": "daily",
            "static": "monthly",
            "archive": "never",
        })
        .alias("changefreq")
    )

sitemap = create_sitemap_asset(
    name="sitemap_smart",
    source_asset="pages_with_changefreq",
    url_column="slug",
    base_url="https://example.com",
    changefreq="daily",  # Will be overridden by column if present
)
```

---

## Integration with Elasticsearch

Generate sitemaps from Elasticsearch indices:

```python
@dg.asset(io_manager_key="elasticsearch_io_manager")
def artworks_searchable(artworks_transform: pl.DataFrame) -> pl.DataFrame:
    """Index artworks to Elasticsearch."""
    return artworks_transform

@dg.asset
def sitemap_from_elasticsearch(
    artworks_searchable: pl.DataFrame,  # Auto-loaded from ES
) -> dict:
    """Generate sitemap from Elasticsearch data."""
    from cogapp_deps.dagster import generate_sitemap_xml

    urls = [
        {
            "loc": f"https://example.com/artworks/{row['artwork_id']}",
            "lastmod": row.get("updated_at", ""),
            "priority": "0.8",
        }
        for row in artworks_searchable.iter_rows(named=True)
    ]

    output_path = Path("data/output/sitemaps/sitemap_from_es.xml")
    count = generate_sitemap_xml(urls, output_path)

    return {"url_count": count}
```

---

## Complete Pipeline Example

```python
import dagster as dg
import polars as pl
from pathlib import Path
from cogapp_deps.dagster import (
    create_sitemap_asset,
    create_image_sitemap_asset,
    generate_sitemap_index_xml,
)

# 1. Data assets
@dg.asset
def artworks_catalog() -> pl.DataFrame:
    return pl.DataFrame({
        "artwork_id": [1, 2, 3],
        "slug": ["mona-lisa", "starry-night", "the-scream"],
        "updated_at": ["2024-01-15", "2024-01-14", "2024-01-13"],
        "priority_score": [1.0, 0.9, 0.85],
    })

@dg.asset
def artwork_images() -> pl.DataFrame:
    return pl.DataFrame({
        "artwork_slug": ["mona-lisa", "starry-night"],
        "image_url": ["/images/mona-lisa.jpg", "/images/starry-night.jpg"],
        "title": ["Mona Lisa", "The Starry Night"],
    })

@dg.asset
def artists_catalog() -> pl.DataFrame:
    return pl.DataFrame({
        "artist_slug": ["da-vinci", "van-gogh", "munch"],
        "updated_at": ["2024-01-10", "2024-01-11", "2024-01-12"],
    })

# 2. Sitemap assets
sitemap_artworks = create_sitemap_asset(
    name="sitemap_artworks",
    source_asset="artworks_catalog",
    url_column="slug",
    base_url="https://example.com/artworks",
    lastmod_column="updated_at",
    priority="priority_score",  # Use data column
    changefreq="monthly",
)

sitemap_images = create_image_sitemap_asset(
    name="sitemap_images",
    source_asset="artwork_images",
    page_url_column="artwork_slug",
    image_url_column="image_url",
    base_url="https://example.com/artworks",
    title_column="title",
)

sitemap_artists = create_sitemap_asset(
    name="sitemap_artists",
    source_asset="artists_catalog",
    url_column="artist_slug",
    base_url="https://example.com/artists",
    lastmod_column="updated_at",
    changefreq="weekly",
    priority="0.7",
)

# 3. Sitemap index
@dg.asset(deps=["sitemap_artworks", "sitemap_images", "sitemap_artists"])
def sitemap_index(context: dg.AssetExecutionContext) -> dict:
    """Create master sitemap index."""
    from datetime import datetime

    sitemaps = [
        {"loc": "https://example.com/sitemap_artworks.xml", "lastmod": datetime.now().strftime("%Y-%m-%d")},
        {"loc": "https://example.com/sitemap_images.xml", "lastmod": datetime.now().strftime("%Y-%m-%d")},
        {"loc": "https://example.com/sitemap_artists.xml", "lastmod": datetime.now().strftime("%Y-%m-%d")},
    ]

    output_path = Path("data/output/sitemaps/sitemap_index.xml")
    count = generate_sitemap_index_xml(sitemaps, output_path)

    context.add_output_metadata({"sitemap_count": count})

    return {"sitemap_path": str(output_path)}

# 4. Upload to S3 (optional)
@dg.asset(deps=["sitemap_index"])
def upload_sitemaps_to_s3(context: dg.AssetExecutionContext) -> dict:
    """Upload all sitemaps to S3."""
    import boto3

    s3 = boto3.client("s3")
    bucket = "example-com"
    uploaded = []

    for sitemap_file in Path("data/output/sitemaps").glob("*.xml"):
        s3_key = f"sitemaps/{sitemap_file.name}"
        s3.upload_file(
            str(sitemap_file),
            bucket,
            s3_key,
            ExtraArgs={"ContentType": "application/xml"}
        )
        uploaded.append(sitemap_file.name)
        context.log.info(f"Uploaded {sitemap_file.name} to s3://{bucket}/{s3_key}")

    return {"files_uploaded": uploaded}
```

---

## Deployment

### 1. Static Hosting (Nginx)

```nginx
# nginx.conf
server {
    listen 80;
    server_name example.com;

    location /sitemap {
        root /var/www/sitemaps;
        add_header Content-Type application/xml;
    }

    # Main sitemap
    location = /sitemap.xml {
        alias /var/www/sitemaps/sitemap_index.xml;
        add_header Content-Type application/xml;
    }
}
```

### 2. S3 + CloudFront

```python
# Upload to S3, serve via CloudFront
s3.upload_file(
    "sitemap_index.xml",
    "example-com",
    "sitemap.xml",
    ExtraArgs={
        "ContentType": "application/xml",
        "CacheControl": "max-age=3600",  # 1 hour cache
    }
)
```

### 3. Dynamic Generation (Flask/Django)

```python
# Serve directly from Dagster output
from flask import Flask, send_file

app = Flask(__name__)

@app.route('/sitemap.xml')
def sitemap():
    return send_file(
        'data/output/sitemaps/sitemap_index.xml',
        mimetype='application/xml'
    )
```

---

## Best Practices

### 1. Update Frequency

- **News/Blog**: Daily sitemaps, daily regeneration
- **Product catalog**: Weekly sitemaps, weekly regeneration
- **Static pages**: Monthly sitemaps, on-demand regeneration

### 2. Priority Guidelines

```python
# Priority recommendations
{
    "homepage": "1.0",
    "main_categories": "0.9",
    "popular_pages": "0.8",
    "standard_pages": "0.5-0.7",
    "old_content": "0.3-0.4",
}
```

### 3. Change Frequency

```python
# Changefreq recommendations
{
    "homepage": "daily",
    "news": "hourly" or "daily",
    "blog_posts": "weekly",
    "products": "daily" or "weekly",
    "static_pages": "monthly",
    "archives": "yearly" or "never",
}
```

### 4. Size Limits

- Max 50,000 URLs per sitemap file
- Max 50MB uncompressed
- Split large sitemaps into multiple files
- Use sitemap index for > 50k URLs

```python
# Auto-split large sitemaps
if url_count > 50000:
    # Create multiple files: sitemap_1.xml, sitemap_2.xml, etc.
    # Then create sitemap_index.xml
    pass
```

---

## Troubleshooting

### Issue: Sitemap Not Being Crawled

**Check:**
1. Submitted to Google Search Console?
2. Listed in robots.txt?
3. Accessible at expected URL?
4. Valid XML format?

**Solution:**
```bash
# Validate XML
xmllint --noout sitemap.xml

# Check accessibility
curl https://example.com/sitemap.xml

# Submit to Google
# → Google Search Console → Sitemaps → Add sitemap URL
```

### Issue: Images Not Appearing in Google Images

**Check:**
1. Image URLs are absolute (not relative)
2. Images are accessible (not 404)
3. ContentType is correct (`image/jpeg`, etc.)

**Solution:**
```python
# Use absolute URLs
image_base_url="https://cdn.example.com"  # Not relative paths
```

---

## Resources

- **Google Sitemap Guidelines**: https://developers.google.com/search/docs/crawling-indexing/sitemaps/overview
- **Sitemap XML Format**: https://www.sitemaps.org/protocol.html
- **Image Sitemaps**: https://developers.google.com/search/docs/crawling-indexing/sitemaps/image-sitemaps
- **Sitemap Validators**: https://www.xml-sitemaps.com/validate-xml-sitemap.html

---

**Boost your SEO with automated sitemap generation!** 
