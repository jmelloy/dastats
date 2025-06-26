#!/usr/bin/env python3
"""
Image comparison script that:
1. Walks through images/ directory and calculates SHA hashes and perceptual hashes
2. Walks through Obsidian folder and does the same
3. Compares the two sets to find matches
4. For matches, extracts metadata from the database and decorates YAML sidecar files
"""

import os
import hashlib
import sqlite3
import json
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import imagehash
from PIL import Image
import logging
import yaml
import re
from collections import defaultdict
from datetime import datetime
import urllib.parse

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def decode_obsidian_url(url: str) -> str:
    """Decode an obsidian URL."""
    return Path(
        "/Users/jeffreymelloy/Library/Mobile Documents/iCloud~md~obsidian/Documents/obsidian/"
        + [f for f in urllib.parse.unquote(url).split("&") if f.startswith("file=")][0][
            5:
        ]
        + ".md"
    )


class ImageComparer:
    def __init__(
        self,
        images_dir: str,
        obsidian_dir: str,
        db_path: str,
        cache_file: str = "image_hashes_cache.json",
        force_recalculate: bool = False,
    ):
        self.images_dir = Path(images_dir)
        self.obsidian_dir = Path(obsidian_dir)
        self.db_path = db_path
        self.cache_file = Path(cache_file)
        self.force_recalculate = force_recalculate
        self.hash_cache = self.load_hash_cache()

    def load_hash_cache(self) -> Dict[str, Dict]:
        """Load existing hash cache from file."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, "r") as f:
                    cache = json.load(f)

                # Clean up stale entries (files that no longer exist)
                original_count = len(cache)
                stale_keys = []
                for cache_key, cache_data in cache.items():
                    path, mtime, size = cache_key.split(":")
                    file_path = Path(path)
                    if not file_path.exists():
                        stale_keys.append(cache_key)
                    else:
                        st = file_path.stat()
                        # Check if the file has been modified since the cache was created
                        if st.st_mtime > float(mtime) or st.st_size != int(size):
                            stale_keys.append(cache_key)

                for key in stale_keys:
                    del cache[key]

                if stale_keys:
                    logger.info(f"Removed {len(stale_keys)} stale cache entries")

                logger.info(
                    f"Loaded hash cache with {len(cache)} entries (cleaned from {original_count})"
                )
                return cache
            except Exception as e:
                logger.error(f"Error loading hash cache: {e}")
        return {}

    def save_hash_cache(self):
        """Save hash cache to file."""
        try:
            with open(self.cache_file, "w") as f:
                json.dump(self.hash_cache, f, indent=2)
            logger.info(f"Saved hash cache with {len(self.hash_cache)} entries")
        except Exception as e:
            logger.error(f"Error saving hash cache: {e}")

    def get_file_hash_key(self, file_path: Path) -> str:
        """Generate a cache key for a file based on path and modification time."""
        try:
            stat = file_path.stat()
            return f"{file_path}:{stat.st_mtime}:{stat.st_size}"
        except Exception:
            return str(file_path)

    def calculate_image_hashes(self, image_path: Path) -> Tuple[str, str]:
        """Calculate SHA256 and perceptual hash for an image, using cache if available."""
        cache_key = self.get_file_hash_key(image_path)

        # Check cache first (unless force_recalculate is True)
        if not self.force_recalculate and cache_key in self.hash_cache:
            cached_data = self.hash_cache[cache_key]
            logger.debug(f"Using cached hashes for {image_path.name}")
            return cached_data["sha256"], cached_data["phash"]

        try:
            # Calculate SHA256
            with open(image_path, "rb") as f:
                sha_hash = hashlib.sha256(f.read()).hexdigest()

            # Calculate perceptual hash
            with Image.open(image_path) as img:
                phash = str(imagehash.average_hash(img))

            # Cache the results
            self.hash_cache[cache_key] = {
                "sha256": sha_hash,
                "phash": phash,
                "path": str(image_path),
            }

            return sha_hash, phash
        except Exception as e:
            logger.error(f"Error processing {image_path}: {e}")
            return None, None

    def walk_directory_for_images(self, directory: Path) -> Dict[str, Dict]:
        """Walk through directory and calculate hashes for all images."""
        image_extensions = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp"}
        image_data = {}

        for file_path in directory.rglob("*"):
            if file_path.is_file() and file_path.suffix.lower() in image_extensions:
                sha_hash, phash = self.calculate_image_hashes(file_path)
                if sha_hash and phash:
                    image_data[str(file_path)] = {
                        "sha256": sha_hash,
                        "phash": phash,
                        "path": file_path,
                    }
                    logger.debug(f"Hashed: {file_path.name}")

        return image_data

    def find_matches(
        self, local_images: Dict, obsidian_images: Dict
    ) -> List[Tuple[Path, Path]]:
        """Find matching images between local and Obsidian directories."""
        matches = []

        # Create lookup dictionaries
        obsidian_sha_lookup = {
            data["sha256"]: path for path, data in obsidian_images.items()
        }
        obsidian_phash_lookup = {
            data["phash"]: path for path, data in obsidian_images.items()
        }

        for local_path, local_data in local_images.items():
            local_sha = local_data["sha256"]
            local_phash = local_data["phash"]

            # Check for SHA match first (exact match)
            if local_sha in obsidian_sha_lookup:
                obsidian_path = obsidian_sha_lookup[local_sha]
                matches.append((Path(local_path), Path(obsidian_path)))
                logger.info(
                    f"SHA match: {Path(local_path).name} <-> {Path(obsidian_path).name} ({local_sha})"
                )
                continue

            if local_phash in obsidian_phash_lookup:
                obsidian_path = obsidian_phash_lookup[local_phash]
                matches.append((Path(local_path), Path(obsidian_path)))
                logger.info(
                    f"Perceptual hash match: {Path(local_path).name} <-> {Path(obsidian_path).name} ({local_phash})"
                )
                continue

        return matches

    def extract_filename_uuid(self, filename: str) -> Optional[str]:
        """Extract UUID from filename like 'C6A9B6A3-73F0-4677-4157-1B55704C5B05_3ysvr7e8cp7yv3fc61cy1apf00.jpg'"""
        try:
            # Split by underscore and take the first part
            parts = filename.split("_")
            if len(parts) >= 2:
                uuid_part = parts[0]
                # Validate it looks like a UUID
                if len(uuid_part) == 36 and uuid_part.count("-") == 4:
                    return uuid_part
        except Exception as e:
            logger.error(f"Error extracting UUID from {filename}: {e}")
        return None

    def get_deviation_metadata(self, deviation_id: str) -> Optional[Dict]:
        """Get deviation metadata from database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Query for deviation data
            cursor.execute(
                """
                SELECT d.deviationid, d.url, d.title, d.is_favourited, d.is_deleted, d.is_published,
                       d.published_time, d.allows_comments, d.is_mature, d.is_downloadable,
                       d.download_filesize, d.excerpt,
                       dm.mature_level, dm.mature_classification,
                       s.favourites, s.comments, dm.tags
                FROM deviations d
                LEFT JOIN deviation_metadata dm ON d.deviationid = dm.deviationid
                LEFT JOIN (
                    SELECT deviationid, 
                           count(messageid) filter (where type = 'feedback.favourite') as favourites,
                           count(messageid) filter (where type = 'feedback.comment') as comments
                    FROM messages
                    GROUP BY deviationid
                ) s ON d.deviationid = s.deviationid
                WHERE d.deviationid = ?
            """,
                (deviation_id,),
            )

            r = cursor.fetchone()
            if r:
                # Get tags - they're stored as JSON strings within a JSON array
                row = dict(zip([c[0] for c in cursor.description], r))

                tags = []
                if row["tags"]:
                    # Parse JSON array of tag objects
                    try:
                        # First parse the outer JSON array
                        tags_array = json.loads(row["tags"])
                        # Then extract tag names from each tag object
                        for tag_obj_str in tags_array:
                            tag_obj = json.loads(tag_obj_str)
                            tags.append(tag_obj.get("tag_name", ""))
                    except Exception as e:
                        logger.error(f"Error parsing tags for {deviation_id}: {e}")
                        tags = []

                return {
                    "deviation_id": row["deviationid"],
                    "url": row["url"],
                    "title": row["title"],
                    "favourites": row["favourites"],
                    "comments": row["comments"],
                    "tags": tags,
                    "published_time": datetime.fromtimestamp(
                        int(row["published_time"])
                    ),
                }

            conn.close()
        except Exception as e:
            logger.error(f"Error querying database for {deviation_id}: {e}")

        return None

    def locate_markdown_file(self, obsidian_path: Path) -> Optional[Path]:
        """Locate the markdown file for a given obsidian path."""

        for file in obsidian_path.parent.parent.rglob("*.md"):
            contents = file.read_text()
            if obsidian_path.name in contents:
                return file
        return None

    def get_markdown_contents(self, markdown_file: Path) -> Tuple[str, dict]:
        """Get the contents of a markdown file."""
        contents = markdown_file.read_text()
        metadata = {}

        if len(contents.split("---")) < 2:
            return contents, {}

        metadata = yaml.safe_load(contents.split("---")[1])

        contents = "---".join(contents.split("---")[2:])

        return contents, metadata

    def format_deviation(self, deviation: dict) -> str:
        """Format deviation metadata into a string."""
        output = []

        output.append("")

        spaces = max(len(k) + 6 for k in deviation.keys())
        values = max(
            len(str(v)) + 2
            for k, v in deviation.items()
            if k not in ["deviation_id", "url", "title"]
        )

        output.append(f"| ID | [{deviation['deviation_id']}]({deviation['url']}) |")
        output.append("|" + "-" * spaces + "|" + "-" * values + "|")
        for k, v in deviation.items():
            if k not in ["deviation_id", "url", "title"]:
                output.append(
                    f"| **{k.title()}** {(spaces - len(k) -6)*' '}| {v} {(values -2 - len(str(v)))*' '}|"
                )

        output.append("")

        return "\n".join(output)

    def get_favs(self, formatted_deviation: str) -> int:
        """Get the number of favourites from a formatted deviation."""
        if match := re.search(r"\*\*Favourites\*\* +\| (\d+)", formatted_deviation):
            return int(match.groups()[0])
        return -1

    def add_deviation_metadata_to_markdown(
        self, image: Path, markdown_file: Path, deviation: dict
    ):
        """Add deviation metadata to a markdown file."""
        contents, metadata = self.get_markdown_contents(markdown_file)
        urls = metadata.get("deviationUrl", [])
        urls.append(deviation["url"])
        metadata["deviationUrl"] = list(set(urls))

        tags = metadata.get("tags", [])
        tags.extend(deviation.get("tags", []))
        metadata["tags"] = list(set(tags))

        if (deviation.get("favourites", 0) or 0) > metadata.get("favourites", 0):
            if metadata.get("cover") and image.name not in metadata["cover"]:
                metadata["cover"] = f"![[{image.name}]]"
            metadata["favourites"] = deviation.get("favourites", 0)
            metadata["comments"] = deviation.get("comments", 0)

        if "deviationId" in metadata:
            del metadata["deviationId"]

        if deviation["deviation_id"] in metadata:
            del metadata[deviation["deviation_id"]]

        output = []

        blocks = re.split(r"!\[\[(.*?)\]\]", contents)
        output.append(blocks.pop(0).strip())

        images = {}
        for i, block in enumerate(blocks):
            if i % 2 == 0 and i < len(blocks) - 1:
                images[block] = blocks[i + 1]

            if block == image.name:
                images[block] = self.format_deviation(deviation)

        for image, formatted_deviation in sorted(
            images.items(), key=lambda x: self.get_favs(x[1]), reverse=True
        ):
            output.append(f"![[{image}]]")
            output.append(formatted_deviation)

        text = f"---\n{yaml.dump(metadata)}\n---\n{'\n'.join(output)}"
        text = re.sub(r"\n\n+", "\n\n", text)
        markdown_file.write_text(text)

    def process_matches(self, matches: List[Tuple[Path, Path]]):
        """Process matches and print deviation stats."""
        for i, (local_path, obsidian_path) in enumerate(matches, 1):
            # Extract UUID from local filename
            local_filename = local_path.name
            deviation_id = self.extract_filename_uuid(local_filename)

            if not deviation_id:
                logger.warning(f"Could not extract UUID from {local_filename}")
                continue

            # Get metadata from database
            metadata = self.get_deviation_metadata(deviation_id)
            if not metadata:
                logger.warning(f"No metadata found for deviation {deviation_id}")
                continue

            # Print match information
            print(f"\n{i}. {local_path.name} <-> {obsidian_path.name}")
            markdown_file = self.locate_markdown_file(obsidian_path)
            if markdown_file:
                print(
                    f"   Markdown file: {markdown_file.relative_to(self.obsidian_dir)}"
                )
                self.add_deviation_metadata_to_markdown(
                    obsidian_path, markdown_file, metadata
                )
            else:
                print("   No markdown file found")

    def run(self, similarity_threshold: float = 90.0):
        """Main execution method."""
        logger.info("Starting image comparison...")

        # Step 1: Process local images directory
        logger.info("Processing local images directory...")
        local_images = self.walk_directory_for_images(self.images_dir)
        logger.info(f"Found {len(local_images)} images in local directory")

        # Step 2: Process Obsidian directory
        logger.info("Processing Obsidian directory...")
        obsidian_images = self.walk_directory_for_images(self.obsidian_dir)
        logger.info(f"Found {len(obsidian_images)} images in Obsidian directory")

        logger.info("Saving hash cache...")
        self.save_hash_cache()

        # Step 3: Find exact matches
        # logger.info("Finding exact matches...")
        # matches = self.find_matches(local_images, obsidian_images)
        # logger.info(f"Found {len(matches)} exact matches")

        # Step 4: Find similar images
        logger.info(f"Finding similar images (threshold: {similarity_threshold}%)...")
        similar_matches = self.find_similar_images(
            local_images, obsidian_images, similarity_threshold
        )
        self.process_similar_images(similar_matches)
        logger.info(f"Found {len(similar_matches)} similar images")

        logger.info(
            f"{len(local_images)} local images, {len(obsidian_images)} obsidian images, {len(matches)} exact matches, {len(similar_matches)} similar matches found"
        )

        # Step 5: Process exact matches
        if matches:
            logger.info("Processing exact matches...")
            self.process_matches(matches)

        # Step 6: Process similar matches
        if similar_matches:
            logger.info("Processing similar matches...")
            self.process_similar_images(similar_matches)

        logger.info("Image comparison completed!")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare images between local directory and Obsidian folder, then print deviation stats."
    )
    parser.add_argument(
        "--images-dir",
        default="images",
        help="Path to local images directory (default: images)",
    )
    parser.add_argument(
        "--obsidian-dir",
        default="/Users/jeffreymelloy/Library/Mobile Documents/iCloud~md~obsidian/Documents/obsidian/AI",
        help="Path to Obsidian directory",
    )
    parser.add_argument(
        "--db-path",
        default="deviantart_data.sqlite",
        help="Path to SQLite database (default: deviantart_data.sqlite)",
    )
    parser.add_argument(
        "--cache-file",
        default="image_hashes_cache.json",
        help="Path to hash cache file (default: image_hashes_cache.json)",
    )
    parser.add_argument(
        "--force-recalculate",
        action="store_true",
        help="Force recalculation of all hashes (ignore cache)",
    )
    parser.add_argument(
        "--similarity-threshold",
        type=float,
        default=90.0,
        help="Minimum similarity percentage for similar images (default: 90.0)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # Set logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Create comparer and run
    comparer = ImageComparer(
        images_dir=args.images_dir,
        obsidian_dir=args.obsidian_dir,
        db_path=args.db_path,
        cache_file=args.cache_file,
        force_recalculate=args.force_recalculate,
    )
    comparer.run(similarity_threshold=args.similarity_threshold)


if __name__ == "__main__":
    main()
