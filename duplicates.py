import os
import sqlite3
from PIL import Image
import imagehash
from collections import defaultdict
from da import DeviantArt

def get_hashes(folder_path):
    """
    Get the hashes of all images in a folder.
    """
    hash_dict = defaultdict(list)

    for root, dirs, files in os.walk(folder_path):
        for filename in files:
            if filename.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".bmp")):
                filepath = os.path.join(root, filename)
                try:
                    with Image.open(filepath) as img:
                        hash = imagehash.average_hash(img)
                        hash_dict[hash].append(filepath)
                except Exception as e:
                    print(f"Error processing {filepath}: {e}")

    return hash_dict

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
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("folder", type=str, help="Path to folder to scan for duplicates", nargs="+")
    parser.add_argument("--sqlitedb", type=str, help="Path to SQLite database", default="deviantart_data.sqlite")
    
    args = parser.parse_args()

    duplicates = []
    hashes = {}
    for folder in args.folder:
        print(f"Scanning for duplicates in {folder}...")
        new_hashes = get_hashes(folder)
        for hash, filepaths in new_hashes.items():
            if hash in hashes:
                duplicates.append((hash, hashes[hash] + filepaths))
            
            for h in hashes:
                if abs(hash - h) < 3:
                    duplicates.append((hash, hashes[h] + filepaths))
            
            hashes[hash] = filepaths
    
    
    if not duplicates:
        print("No duplicate images found.")
        return

    conn = sqlite3.connect(args.sqlitedb)
    cursor = conn.cursor()

    print(f"\nFound {len(duplicates)} duplicate images:")
    for hash_value, filepaths in duplicates:
        print(f"\nDuplicate set (hash: {hash_value}):")
        for filepath in filepaths:
            uuid = os.path.basename(filepath).split(".")[0]
            cursor.execute("SELECT * FROM deviations WHERE deviationid = ?", (uuid,))
            cols = [desc[0] for desc in cursor.description]
            rec = cursor.fetchone()
            if rec is None:
                print(f"  {filepath} --> Not found in database")
                continue
            dev = {cols[i]: rec[i] for i in range(len(cols))}
            print(f"  {filepath} --> {dev['url']} ({dev['is_deleted']})")


        # for filepath in filepaths[1:]:
        #     print(f"  {filepath} --> Deleting")
        #     if os.path.exists(filepath):
        #         os.remove(filepath)

    conn.close()


if __name__ == "__main__":
    main()
