"""Create SQLite database with media references for artworks.

Media table has artwork_id foreign key and sort_order for multiple images per artwork.
The media is joined with artworks in the transform step based on artwork_id.

Schema:
    media (
        media_id INTEGER PRIMARY KEY,
        artwork_id INTEGER NOT NULL,     -- references artworks.artwork_id
        sort_order INTEGER NOT NULL,     -- 1 = primary image
        filename TEXT NOT NULL,
        media_type TEXT NOT NULL,
        file_format TEXT NOT NULL,
        width_px INTEGER,
        height_px INTEGER,
        file_size_kb INTEGER,
        alt_text TEXT,
        created_date TEXT
    )
"""

import csv
import random
import sqlite3
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
MEDIA_DB = DATA_DIR / "input" / "media.db"
ARTWORKS_CSV = DATA_DIR / "input" / "artworks.csv"


def create_media_db() -> None:
    """Create SQLite database with media table linked to artworks via artwork_id."""
    # Remove existing db if present
    if MEDIA_DB.exists():
        MEDIA_DB.unlink()

    conn = sqlite3.connect(MEDIA_DB)
    cursor = conn.cursor()

    # Create media table with artwork_id FK and sort_order
    cursor.execute("""
        CREATE TABLE media (
            media_id INTEGER PRIMARY KEY,
            artwork_id INTEGER NOT NULL,
            sort_order INTEGER NOT NULL DEFAULT 1,
            filename TEXT NOT NULL,
            media_type TEXT NOT NULL,
            file_format TEXT NOT NULL,
            width_px INTEGER,
            height_px INTEGER,
            file_size_kb INTEGER,
            alt_text TEXT,
            created_date TEXT
        )
    """)

    # Create index for efficient artwork lookups
    cursor.execute("CREATE INDEX idx_media_artwork ON media(artwork_id, sort_order)")

    # Read artworks to generate corresponding media (artworks CSV unchanged)
    with open(ARTWORKS_CSV) as f:
        reader = csv.DictReader(f)
        artworks = list(reader)

    # Generate media for each artwork (multiple per artwork with sort order)
    media_records = []
    media_id = 1

    for artwork in artworks:
        artwork_id = int(artwork["artwork_id"])
        title = artwork["title"]

        # Create a sanitized filename base
        filename_base = title.lower().replace(" ", "_").replace("'", "").replace('"', "")[:30]
        sort_order = 1

        # Primary image for every artwork (sort_order=1)
        media_records.append({
            "media_id": media_id,
            "artwork_id": artwork_id,
            "sort_order": sort_order,
            "filename": f"{filename_base}_main.jpg",
            "media_type": "primary",
            "file_format": "jpg",
            "width_px": random.choice([1920, 2560, 3840]),
            "height_px": random.choice([1080, 1440, 2160]),
            "file_size_kb": random.randint(500, 5000),
            "alt_text": f"{title} - primary image",
            "created_date": f"202{random.randint(0, 4)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
        })
        media_id += 1
        sort_order += 1

        # High-res scan (60% chance)
        if random.random() < 0.6:
            media_records.append({
                "media_id": media_id,
                "artwork_id": artwork_id,
                "sort_order": sort_order,
                "filename": f"{filename_base}_hires.tiff",
                "media_type": "high_res_scan",
                "file_format": "tiff",
                "width_px": 8000,
                "height_px": 6000,
                "file_size_kb": random.randint(20000, 80000),
                "alt_text": f"{title} - high resolution scan",
                "created_date": f"202{random.randint(0, 4)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
            })
            media_id += 1
            sort_order += 1

        # Detail shots (40% chance for 1-3 detail shots)
        if random.random() < 0.4:
            num_details = random.randint(1, 3)
            for i in range(num_details):
                media_records.append({
                    "media_id": media_id,
                    "artwork_id": artwork_id,
                    "sort_order": sort_order,
                    "filename": f"{filename_base}_detail_{i + 1}.jpg",
                    "media_type": "detail",
                    "file_format": "jpg",
                    "width_px": 4000,
                    "height_px": 3000,
                    "file_size_kb": random.randint(1000, 4000),
                    "alt_text": f"{title} - detail {i + 1}",
                    "created_date": f"202{random.randint(0, 4)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
                })
                media_id += 1
                sort_order += 1

        # Frame photo (25% chance)
        if random.random() < 0.25:
            media_records.append({
                "media_id": media_id,
                "artwork_id": artwork_id,
                "sort_order": sort_order,
                "filename": f"{filename_base}_frame.png",
                "media_type": "frame",
                "file_format": "png",
                "width_px": 4000,
                "height_px": 3000,
                "file_size_kb": random.randint(2000, 8000),
                "alt_text": f"{title} - frame and mounting",
                "created_date": f"202{random.randint(0, 4)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
            })
            media_id += 1
            sort_order += 1

    # Insert media records
    cursor.executemany(
        """
        INSERT INTO media (media_id, artwork_id, sort_order, filename, media_type,
                          file_format, width_px, height_px, file_size_kb, alt_text, created_date)
        VALUES (:media_id, :artwork_id, :sort_order, :filename, :media_type,
                :file_format, :width_px, :height_px, :file_size_kb, :alt_text, :created_date)
        """,
        media_records,
    )

    conn.commit()

    # Print summary (artworks CSV is NOT modified)
    cursor.execute("SELECT COUNT(*) FROM media")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT media_type, COUNT(*) FROM media GROUP BY media_type ORDER BY COUNT(*) DESC")
    by_type = cursor.fetchall()
    cursor.execute("SELECT AVG(cnt) FROM (SELECT COUNT(*) as cnt FROM media GROUP BY artwork_id)")
    avg_per_artwork = cursor.fetchone()[0]

    print(f"Created {total} media records in {MEDIA_DB}")
    print(f"\nMedia summary:")
    print(f"  Total records: {total}")
    print(f"  Avg per artwork: {avg_per_artwork:.1f}")
    print(f"  By type:")
    for media_type, count in by_type:
        print(f"    {media_type}: {count}")

    conn.close()


if __name__ == "__main__":
    create_media_db()
