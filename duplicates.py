import os
import sqlite3
from PIL import Image
import imagehash
from collections import defaultdict
from da import DeviantArt


def find_duplicate_images(folder_path):
    """
    Find duplicate images in a folder using perceptual hashing.
    Returns a dictionary of hash values mapped to lists of duplicate image paths.
    """
    hash_dict = defaultdict(list)

    # Walk through all files in folder
    for root, dirs, files in os.walk(folder_path):
        for filename in files:
            if filename.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".bmp")):
                filepath = os.path.join(root, filename)
                try:
                    # Calculate perceptual hash of image
                    with Image.open(filepath) as img:
                        hash = str(imagehash.average_hash(img))
                        hash_dict[hash].append(filepath)
                except Exception as e:
                    print(f"Error processing {filepath}: {e}")

    # Filter to only hashes with duplicates
    duplicates = {k: v for k, v in hash_dict.items() if len(v) > 1}

    return duplicates


def main():
    import sys

    if len(sys.argv) < 2:
        print("Usage: python duplicates.py <folder_path> [sqlitedb]")
        sys.exit(1)
    folder = sys.argv[1]
    if len(sys.argv) > 2:
        sqlitedb = sys.argv[2]
    else:
        sqlitedb = "deviantart_data.sqlite"

    print("Scanning for duplicates...")
    duplicates = find_duplicate_images(folder)

    if not duplicates:
        print("No duplicate images found.")
        return

    conn = sqlite3.connect(sqlitedb)
    cursor = conn.cursor()

    print(f"\nFound {len(duplicates)} duplicate images:")
    for hash_value, filepaths in duplicates.items():
        print(f"\nDuplicate set (hash: {hash_value}):")
        for filepath in filepaths:
            uuid = os.path.basename(filepath).split(".")[0]
            cursor.execute("SELECT * FROM deviations WHERE deviationid = ?", (uuid,))
            cols = [desc[0] for desc in cursor.description]
            rec = cursor.fetchone()
            dev = {cols[i]: rec[i] for i in range(len(cols))}
            print(f"  {filepath} --> {dev['url']}")

    conn.close()


if __name__ == "__main__":
    main()
