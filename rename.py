import os
import sqlite3
import shutil


def move_thumbs_to_subfolders(db_path, thumbs_dir, dryrun, destination):
    """
    Move thumbnail files from thumbs/{deviationid} into subfolders based on deviation IDs from SQLite DB
    """
    if not db_path:
        print("No SQLite database path provided")
        return

    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all deviation IDs
    cursor.execute("SELECT deviationid FROM deviations")
    deviation_ids = [row[0] for row in cursor.fetchall()]

    conn.close()

    thumbs_dir = "thumbs"
    if not os.path.exists(thumbs_dir):
        print(f"Thumbs directory '{thumbs_dir}' does not exist")
        return

    # Process each deviation ID
    for dev_id in deviation_ids:
        filename = f"{dev_id}.jpg"
        src_path = os.path.join(thumbs_dir, filename)
        if not os.path.exists(src_path):
            continue

        # Move the file to subfolder
        dest_path = os.path.join(destination, filename)
        if dryrun:
            print(f"Would move {src_path} to {dest_path}")
        else:
            try:
                shutil.move(src_path, dest_path)
                print(f"Moved {src_path} to {dest_path}")
            except Exception as e:
                print(f"Error moving {src_path}: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sqlitedb", type=str, required=True, help="Path to SQLite database"
    )
    parser.add_argument(
        "--thumbsdir", type=str, default="thumbs", help="Path to thumbs directory"
    )

    parser.add_argument(
        "--dryrun",
        action="store_true",
        help="Dry run the script without actually moving files",
    )

    parser.add_argument(
        "--destination", type=str, required=True, help="Path to destination directory"
    )

    args = parser.parse_args()
    # Create subfolder if it doesn't exist
    os.makedirs(args.destination, exist_ok=True)

    move_thumbs_to_subfolders(
        args.sqlitedb, args.thumbsdir, args.dryrun, args.destination
    )
