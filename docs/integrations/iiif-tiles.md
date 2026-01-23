# IIIF Tile Generation with Dagster

Complete guide to generating IIIF (International Image Interoperability Framework) tiles from high-resolution images using pyvips in Dagster pipelines.

## What is IIIF?

IIIF is a set of open standards for delivering high-quality, attributed digital objects online at scale. It's used by:
- Museums and galleries
- Libraries and archives
- Digital humanities projects
- Cultural heritage institutions

**Key Benefits:**
- Zoom into gigapixel images smoothly
- Deliver images efficiently (only load visible tiles)
- Consistent viewer experience across institutions
- Deep zoom without downloading full image

---

## Architecture Overview

```
High-Res Image → Tile Generation → IIIF Manifest → IIIF Server → Web Viewer
    (TIFF)          (pyvips)         (JSON)        (Cantaloupe)   (OpenSeadragon)
```

**In Dagster:**
```
artwork_images → generate_iiif_tiles → create_iiif_manifest → upload_to_s3
   (asset)             (asset)              (asset)            (asset)
```

---

## Installation

```bash
# Install libvips (the underlying C library)
# macOS:
brew install vips

# Ubuntu/Debian:
sudo apt-get install libvips libvips-dev

# Install Python wrapper
pip install pyvips

# Optional: AWS for S3 upload
pip install boto3
```

---

## Basic Tile Generation Asset

```python
import dagster as dg
import polars as pl
from pathlib import Path
import pyvips
from cogapp_libs.dagster import add_dataframe_metadata, track_timing

@dg.asset(
    kinds={"image", "iiif"},
    group_name="image_processing",
)
def generate_iiif_tiles(
    context: dg.AssetExecutionContext,
    artwork_images: pl.DataFrame,  # DataFrame with image paths
) -> pl.DataFrame:
    """Generate IIIF tiles from high-resolution images using pyvips.

    Input DataFrame columns:
    - artwork_id: Unique identifier
    - image_path: Path to high-res image (TIFF, JPEG, PNG)
    - width: Original image width (optional, will be calculated)
    - height: Original image height (optional, will be calculated)

    Output DataFrame columns:
    - artwork_id: Same as input
    - tiles_path: Path to generated tiles directory
    - tile_count: Number of tiles generated
    - max_zoom_level: Maximum zoom level
    - width: Image width in pixels
    - height: Image height in pixels
    """
    results = []

    with track_timing(context, "tile_generation"):
        for row in artwork_images.iter_rows(named=True):
            artwork_id = row["artwork_id"]
            image_path = Path(row["image_path"])

            context.log.info(f"Processing {artwork_id}: {image_path}")

            # Output directory for this artwork's tiles
            tiles_dir = Path(f"data/output/iiif/tiles/{artwork_id}")
            tiles_dir.mkdir(parents=True, exist_ok=True)

            # Load image with pyvips (lazy loading, very fast)
            image = pyvips.Image.new_from_file(str(image_path), access="sequential")

            # Calculate zoom levels based on image size
            max_dimension = max(image.width, image.height)
            max_zoom = 0
            while (256 * (2 ** max_zoom)) < max_dimension:
                max_zoom += 1

            context.log.info(
                f"{artwork_id}: {image.width}x{image.height}px, "
                f"max zoom: {max_zoom}"
            )

            # Generate tiles using DeepZoom format (IIIF compatible)
            # Tile size: 256x256 pixels (IIIF standard)
            # Overlap: 1 pixel (reduces seam artifacts)
            image.dzsave(
                str(tiles_dir / artwork_id),
                suffix=".jpg",  # JPEG tiles
                tile_size=256,
                overlap=1,
                depth="onetile",  # One tile per level
                layout="iiif",  # IIIF layout
                id=artwork_id,  # IIIF identifier
            )

            # Count generated tiles
            tile_count = sum(1 for _ in tiles_dir.rglob("*.jpg"))

            results.append({
                "artwork_id": artwork_id,
                "tiles_path": str(tiles_dir),
                "tile_count": tile_count,
                "max_zoom_level": max_zoom,
                "width": image.width,
                "height": image.height,
            })

            context.log.info(
                f"{artwork_id}: Generated {tile_count} tiles "
                f"at {max_zoom + 1} zoom levels"
            )

    result_df = pl.DataFrame(results)

    add_dataframe_metadata(
        context,
        result_df,
        total_tiles=result_df["tile_count"].sum(),
        avg_tiles_per_image=result_df["tile_count"].mean(),
    )

    return result_df
```

---

## IIIF Manifest Generation

IIIF manifests are JSON files describing the image and how to display it.

```python
import json
from datetime import datetime

@dg.asset(
    kinds={"iiif", "json"},
    group_name="image_processing",
)
def create_iiif_manifests(
    context: dg.AssetExecutionContext,
    generate_iiif_tiles: pl.DataFrame,
    artwork_metadata: pl.DataFrame,  # Artwork titles, artists, etc.
) -> pl.DataFrame:
    """Generate IIIF Presentation API 3.0 manifests for each artwork.

    Output: DataFrame with manifest paths
    """
    results = []

    # Join tile data with metadata
    combined = generate_iiif_tiles.join(
        artwork_metadata,
        on="artwork_id",
        how="left"
    )

    for row in combined.iter_rows(named=True):
        artwork_id = row["artwork_id"]
        manifest_path = Path(f"data/output/iiif/manifests/{artwork_id}.json")
        manifest_path.parent.mkdir(parents=True, exist_ok=True)

        # IIIF Presentation API 3.0 manifest
        manifest = {
            "@context": "http://iiif.io/api/presentation/3/context.json",
            "id": f"https://example.com/iiif/{artwork_id}/manifest.json",
            "type": "Manifest",
            "label": {
                "en": [row.get("title", f"Artwork {artwork_id}")]
            },
            "metadata": [
                {
                    "label": {"en": ["Artist"]},
                    "value": {"en": [row.get("artist_name", "Unknown")]}
                },
                {
                    "label": {"en": ["Date"]},
                    "value": {"en": [str(row.get("year", "Unknown"))]}
                },
            ],
            "items": [
                {
                    "id": f"https://example.com/iiif/{artwork_id}/canvas",
                    "type": "Canvas",
                    "width": row["width"],
                    "height": row["height"],
                    "items": [
                        {
                            "id": f"https://example.com/iiif/{artwork_id}/page",
                            "type": "AnnotationPage",
                            "items": [
                                {
                                    "id": f"https://example.com/iiif/{artwork_id}/annotation",
                                    "type": "Annotation",
                                    "motivation": "painting",
                                    "body": {
                                        "id": f"https://example.com/iiif/{artwork_id}/full/max/0/default.jpg",
                                        "type": "Image",
                                        "format": "image/jpeg",
                                        "service": [
                                            {
                                                "id": f"https://example.com/iiif/{artwork_id}",
                                                "type": "ImageService3",
                                                "profile": "level2",
                                                "width": row["width"],
                                                "height": row["height"],
                                            }
                                        ],
                                        "width": row["width"],
                                        "height": row["height"],
                                    },
                                    "target": f"https://example.com/iiif/{artwork_id}/canvas",
                                }
                            ],
                        }
                    ],
                }
            ],
        }

        # Write manifest
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        results.append({
            "artwork_id": artwork_id,
            "manifest_path": str(manifest_path),
        })

        context.log.info(f"Generated manifest for {artwork_id}")

    result_df = pl.DataFrame(results)

    add_dataframe_metadata(context, result_df)

    return result_df
```

---

## Advanced: Parallel Processing

For large collections, process images in parallel:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

@dg.asset
def generate_iiif_tiles_parallel(
    context: dg.AssetExecutionContext,
    artwork_images: pl.DataFrame,
) -> pl.DataFrame:
    """Generate tiles in parallel using thread pool."""

    def process_single_image(row):
        """Process one image (thread-safe)."""
        artwork_id = row["artwork_id"]
        image_path = Path(row["image_path"])

        tiles_dir = Path(f"data/output/iiif/tiles/{artwork_id}")
        tiles_dir.mkdir(parents=True, exist_ok=True)

        image = pyvips.Image.new_from_file(str(image_path), access="sequential")

        image.dzsave(
            str(tiles_dir / artwork_id),
            suffix=".jpg",
            tile_size=256,
            overlap=1,
            layout="iiif",
            id=artwork_id,
        )

        tile_count = sum(1 for _ in tiles_dir.rglob("*.jpg"))

        return {
            "artwork_id": artwork_id,
            "tiles_path": str(tiles_dir),
            "tile_count": tile_count,
            "width": image.width,
            "height": image.height,
        }

    results = []

    # Process with thread pool (max 4 concurrent)
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(process_single_image, row): row
            for row in artwork_images.iter_rows(named=True)
        }

        for future in as_completed(futures):
            row = futures[future]
            try:
                result = future.result()
                results.append(result)
                context.log.info(f"Completed {result['artwork_id']}")
            except Exception as e:
                context.log.error(f"Failed {row['artwork_id']}: {e}")

    return pl.DataFrame(results)
```

---

## Upload to S3

```python
@dg.asset(
    kinds={"s3", "iiif"},
    group_name="image_processing",
)
def upload_iiif_to_s3(
    context: dg.AssetExecutionContext,
    generate_iiif_tiles: pl.DataFrame,
    create_iiif_manifests: pl.DataFrame,
) -> pl.DataFrame:
    """Upload IIIF tiles and manifests to S3."""
    import boto3
    from pathlib import Path

    s3 = boto3.client("s3")
    bucket = "my-iiif-bucket"
    results = []

    # Upload tiles
    for row in generate_iiif_tiles.iter_rows(named=True):
        artwork_id = row["artwork_id"]
        tiles_dir = Path(row["tiles_path"])

        uploaded = 0
        for tile_file in tiles_dir.rglob("*.jpg"):
            # Preserve directory structure in S3
            relative_path = tile_file.relative_to(tiles_dir.parent)
            s3_key = f"iiif/tiles/{relative_path}"

            s3.upload_file(
                str(tile_file),
                bucket,
                s3_key,
                ExtraArgs={"ContentType": "image/jpeg"}
            )
            uploaded += 1

        context.log.info(f"Uploaded {uploaded} tiles for {artwork_id}")

        results.append({
            "artwork_id": artwork_id,
            "s3_bucket": bucket,
            "s3_prefix": f"iiif/tiles/{artwork_id}",
            "files_uploaded": uploaded,
        })

    # Upload manifests
    for row in create_iiif_manifests.iter_rows(named=True):
        artwork_id = row["artwork_id"]
        manifest_path = Path(row["manifest_path"])

        s3_key = f"iiif/manifests/{artwork_id}.json"
        s3.upload_file(
            str(manifest_path),
            bucket,
            s3_key,
            ExtraArgs={"ContentType": "application/json"}
        )

    result_df = pl.DataFrame(results)
    add_dataframe_metadata(context, result_df)

    return result_df
```

---

## IIIF Server Setup

### Option 1: Cantaloupe (Java)

```bash
# Download Cantaloupe
wget https://github.com/cantaloupe-project/cantaloupe/releases/download/v5.0.5/cantaloupe-5.0.5.zip
unzip cantaloupe-5.0.5.zip

# Configure (cantaloupe.properties)
FilesystemSource.BasicLookupStrategy.path_prefix = /path/to/tiles/
endpoint.iiif.2.enabled = true
endpoint.iiif.3.enabled = true

# Run
java -Dcantaloupe.config=./cantaloupe.properties -Xmx2g -jar cantaloupe-5.0.5.jar
```

### Option 2: IIPImage (C++)

```bash
# Install
apt-get install iipimage-server

# Configure nginx
location /iiif/ {
    fastcgi_pass  localhost:9000;
    fastcgi_param PATH_INFO $fastcgi_script_name;
    fastcgi_param REQUEST_METHOD $request_method;
    fastcgi_param QUERY_STRING $query_string;
    fastcgi_param CONTENT_TYPE $content_type;
    fastcgi_param CONTENT_LENGTH $content_length;
}
```

### Option 3: Static Tiles (No Server)

If you generate tiles with `layout="iiif"`, you can serve them statically:

```bash
# Nginx config
location /iiif/ {
    root /var/www/iiif;
    add_header Access-Control-Allow-Origin *;
}
```

---

## Web Viewer Integration

### OpenSeadragon

```html
<!DOCTYPE html>
<html>
<head>
    <script src="https://cdn.jsdelivr.net/npm/openseadragon@4.0/build/openseadragon/openseadragon.min.js"></script>
</head>
<body>
    <div id="viewer" style="width: 100%; height: 800px;"></div>

    <script>
        var viewer = OpenSeadragon({
            id: "viewer",
            prefixUrl: "https://cdn.jsdelivr.net/npm/openseadragon@4.0/build/openseadragon/images/",
            tileSources: {
                "@context": "http://iiif.io/api/image/3/context.json",
                "id": "https://example.com/iiif/artwork123",
                "type": "ImageService3",
                "profile": "level2",
                "protocol": "http://iiif.io/api/image",
                "width": 5000,
                "height": 7000,
                "tiles": [{
                    "width": 256,
                    "scaleFactors": [1, 2, 4, 8, 16, 32]
                }]
            }
        });
    </script>
</body>
</html>
```

### Mirador (IIIF Viewer)

```html
<div id="mirador"></div>

<script src="https://unpkg.com/mirador@latest/dist/mirador.min.js"></script>
<script>
    Mirador.viewer({
        id: 'mirador',
        windows: [{
            manifestId: 'https://example.com/iiif/artwork123/manifest.json',
        }]
    });
</script>
```

---

## Complete Pipeline Example

```python
from pathlib import Path
import dagster as dg
import polars as pl
from cogapp_libs.dagster import add_dataframe_metadata

# 1. List artwork images
@dg.asset
def artwork_images(context) -> pl.DataFrame:
    """Scan directory for high-res images."""
    image_dir = Path("data/input/images")

    images = []
    for img_path in image_dir.glob("*.tif"):
        artwork_id = img_path.stem
        images.append({
            "artwork_id": artwork_id,
            "image_path": str(img_path),
        })

    return pl.DataFrame(images)


# 2. Generate tiles (defined above)
# @dg.asset
# def generate_iiif_tiles(...): ...


# 3. Create manifests (defined above)
# @dg.asset
# def create_iiif_manifests(...): ...


# 4. Upload to S3 (defined above)
# @dg.asset
# def upload_iiif_to_s3(...): ...


# 5. Update search index
@dg.asset(io_manager_key="elasticsearch_io_manager")
def artwork_search_index(
    create_iiif_manifests: pl.DataFrame,
    artwork_metadata: pl.DataFrame,
) -> pl.DataFrame:
    """Index artwork metadata + IIIF URLs for search."""
    return create_iiif_manifests.join(
        artwork_metadata,
        on="artwork_id"
    ).with_columns([
        pl.col("manifest_path").str.replace(
            "data/output/iiif/manifests",
            "https://example.com/iiif/manifests"
        ).alias("manifest_url")
    ])
```

---

## Performance Tips

### 1. Use TIFF Pyramids

For very large images, create pyramidal TIFFs first:

```python
# Convert to pyramidal TIFF
image = pyvips.Image.new_from_file("input.jpg")
image.tiffsave(
    "output.tif",
    tile=True,
    pyramid=True,
    compression="jpeg",
    tile_width=256,
    tile_height=256,
)
```

### 2. Optimize JPEG Quality

```python
image.dzsave(
    str(tiles_dir / artwork_id),
    suffix=".jpg[Q=85]",  # 85% quality (balance size/quality)
    tile_size=256,
)
```

### 3. Use WebP for Modern Browsers

```python
image.dzsave(
    str(tiles_dir / artwork_id),
    suffix=".webp[Q=80]",  # WebP tiles (smaller files)
    tile_size=256,
)
```

### 4. Pre-generate Thumbnails

```python
# Generate thumbnail for previews
thumbnail = image.thumbnail_image(512)
thumbnail.jpegsave(f"thumbnails/{artwork_id}.jpg", Q=90)
```

---

## Resources

- **IIIF Specs**: https://iiif.io/api/
- **pyvips Docs**: https://libvips.github.io/pyvips/
- **Cantaloupe**: https://cantaloupe-project.github.io/
- **OpenSeadragon**: https://openseadragon.github.io/
- **Mirador**: https://projectmirador.org/

---

**IIIF enables world-class image delivery for cultural heritage!** 🖼️
