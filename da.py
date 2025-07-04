import requests
import sqlite3
import time
import os
import json
import logging
from datetime import datetime
import pandas as pd
from typing import Iterator
from models import (
    Deviation,
    DeviationActivity,
    Select,
    DeviationMetadata,
    User,
    Collection,
    Gallery,
    Message,
)
from utils import get_table_info, generate_alter_statements, create_temp_db_from_sql

logger = logging.getLogger(__name__)

file_path = os.path.dirname(os.path.abspath(__file__))

# Constants for DeviantArt API
API_BASE_URL = "https://www.deviantart.com/api/v1/oauth2"
COLLECTIONS_ENDPOINT = "/gallery/all"
WHOFAVED_ENDPOINT = "/deviation/whofaved"
METADATA_ENDPOINT = "/deviation/metadata"

AUTHORIZATION_BASE_URL = "https://www.deviantart.com/oauth2/authorize"
TOKEN_URL = "https://www.deviantart.com/oauth2/token"
REDIRECT_URI = "http://localhost:4444/callback"


def raise_for_status(response):
    try:
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Error: {e}")
        logger.warning(response.text)
        raise
    return response


class DeviantArt:
    def __init__(self, sqlitedb=None):
        self.access_token = ""
        self.refresh_token = ""
        self.expires = 0
        self.client_id = None
        self.client_secret = None

        if sqlitedb:
            self.sqlite_db = sqlitedb
        else:
            self.sqlite_db = os.path.join(file_path, "deviantart_data.sqlite")

        if os.path.exists(".credentials.json"):
            with open(".credentials.json", "r") as F:
                data = json.loads(F.read())
                self.client_id = data["client_id"]
                self.client_secret = data["client_secret"]

        if os.path.exists(".token.json"):
            with open(".token.json", "r") as F:
                data = json.loads(F.read())
                self.access_token = data["access_token"]
                self.refresh_token = data["refresh_token"]
                self.expires = data.get("expires_at", 0)

    def authorization_url(self):
        url = f"{AUTHORIZATION_BASE_URL}?client_id={self.client_id}&redirect_uri={REDIRECT_URI}&response_type=code&scope=browse message publish stash"
        logger.info(f"Authorization URL: {url}")
        return url

    def check_token(self):
        if not self.access_token:
            raise ValueError("No access token found.")
        if not self.expires or self.expires < int(time.time()):
            token_response = self.get_refresh_token()
            self.set_token(token_response)

        raise_for_status(
            requests.get(
                f"{API_BASE_URL}/placebo",
                params={"access_token": self.access_token},
            )
        )

    def set_credentials(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret

        with open(".credentials.json", "w") as F:
            F.write(
                json.dumps({"client_id": client_id, "client_secret": client_secret})
            )

    def set_token(self, data):
        self.access_token = data["access_token"]
        self.refresh_token = data["refresh_token"]
        self.expires = int(time.time()) + data["expires_in"]

        with open(".token.json", "w") as F:
            data["expires_at"] = int(time.time()) + data["expires_in"]
            F.write(json.dumps(data))

    def update_access_token(self, code):
        # Exchange the code for a token
        if not self.client_id or not self.client_secret:
            raise ValueError("Client ID and Client Secret not set.")
        logger.info(f"Updating access token with code: {code}")
        logger.info(f"Client ID: {self.client_id}")
        logger.info(f"Redirect URI: {REDIRECT_URI}")
        logger.info(f"Grant Type: authorization_code")

        token_response = raise_for_status(
            requests.post(
                TOKEN_URL,
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "code": code,
                    "redirect_uri": REDIRECT_URI,
                    "grant_type": "authorization_code",
                },
            )
        )

        logger.info(f"Token response: {token_response.json()}")

        self.set_token(token_response.json())

        return token_response.json()

    def get_refresh_token(self):
        if not self.client_id or not self.client_secret:
            raise ValueError("Client ID and Client Secret not set.")

        if not self.refresh_token:
            raise ValueError("No refresh token found.")

        logger.info(f"Refreshing access token with refresh token: {self.refresh_token}")
        logger.info(f"Client ID: {self.client_id}")
        logger.info(f"Client Secret: {self.client_secret}")
        logger.info(f"Grant Type: refresh_token")

        token_response = raise_for_status(
            requests.post(
                TOKEN_URL,
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "refresh_token": self.refresh_token,
                    "grant_type": "refresh_token",
                },
            )
        )

        return token_response.json()

    def _get_deviations(self, offset=0, limit=24, gallery="all", username=None):
        url = f"{API_BASE_URL}/gallery/{gallery}"
        params = {
            "access_token": self.access_token,
            "offset": offset,
            "limit": limit,
            "mature_content": True,
            "with_session": True,
        }
        if username:
            params["username"] = username
        response = raise_for_status(requests.get(url, params=params))

        return response.json()

    def get_all_deviations(self, gallery="all", offset=0, username=None):
        limit = 24
        has_more = True
        while has_more:
            logger.info(f"Fetching gallery {gallery}: offset={offset}, limit={limit}")
            data = self._get_deviations(
                offset, limit, gallery=gallery, username=username
            )
            results = data.get("results", [])
            has_more = data.get("has_more", False)
            offset = data.get("next_offset", 0)
            if not results:
                break

            for item in results:
                yield Deviation.from_json(item)

    def get_deviation(self, deviation_id) -> Deviation:
        url = f"{API_BASE_URL}/deviation/{deviation_id}"
        params = {"access_token": self.access_token}
        try:
            response = raise_for_status(requests.get(url, params=params))
            return Deviation.from_json(response.json())
        except Exception as e:
            logging.error(f"Error fetching deviation {deviation_id}: {e}")
            return None

    def get_whofaved(self, deviation_id, offset=0):
        url = f"{API_BASE_URL}/deviation/whofaved"

        limit = 50
        has_more = True

        sleep_time = 1

        while has_more:
            params = {
                "deviationid": deviation_id,
                "offset": offset,
                "limit": limit,
                "access_token": self.access_token,
            }
            try:
                response = raise_for_status(requests.get(url, params=params))
                metadata = response.json()
                has_more = metadata.get("has_more", False)
                offset = metadata.get("next_offset", 0)
                results = metadata.get("results", [])
                for item in results:
                    yield item

                sleep_time = max(1, int(sleep_time * 0.75))
            except Exception as e:
                logger.error(f"Error fetching whofaved for {deviation_id}: {e}")
                if e.response.status_code == 429:
                    logger.info(f"Rate limited, sleeping for {sleep_time} seconds")
                    time.sleep(sleep_time)
                    sleep_time *= 2
                else:
                    raise e

    def get_metadata(self, deviation_ids: list) -> Iterator[DeviationMetadata]:
        batch_size = 10
        sleep_time = 1

        def get_batch():
            a = []
            for i in range(0, min(batch_size, len(deviation_ids))):
                a.append(deviation_ids.pop(0))
            return a

        batch = get_batch()
        while batch:
            url = f"{API_BASE_URL}/deviation/metadata"
            params = {
                "deviationids[]": batch,
                "access_token": self.access_token,
                "ext_camera": "true",
                "ext_stats": "true",
                "ext_collection": "true",
                "ext_gallery": "true",
            }
            try:
                logger.info(
                    f"Fetching metadata for {len(batch)} deviations - {batch[0]} - {batch[-1]}"
                )
                response = raise_for_status(requests.get(url, params=params))
                metadata = response.json().get("metadata", [])

                for item in metadata:
                    yield DeviationMetadata.from_json(item)

                sleep_time = max(1, sleep_time / 2)

                batch = get_batch()
            except Exception as e:
                if e.response.status_code == 429:
                    logger.info(f"Rate limited, sleeping for {sleep_time} seconds")
                    sleep_time *= 2
                else:
                    logger.error(f"Error fetching metadata for {batch}: {e}")
                    raise e

            time.sleep(sleep_time)

    def get_user_info(self, username):
        url = f"{API_BASE_URL}/user/{username}"
        params = {
            "access_token": self.access_token,
        }
        response = raise_for_status(requests.get(url, params=params))
        return response.json()

    def _get_feed_stack(self, stackid, offset=0):
        url = f"{API_BASE_URL}/messages/feedback/{stackid}"
        params = {
            "access_token": self.access_token,
            "with_session": True,
            "limit": 50,
            "offset": offset,
            "mature_content": True,
        }
        response = raise_for_status(requests.get(url, params=params))
        return response.json()

    def _get_feed(self, cursor=None):

        url = f"{API_BASE_URL}/messages/feed"
        params = {
            "access_token": self.access_token,
            "with_session": True,
            "mature_content": True,
        }

        if cursor:
            params["cursor"] = cursor

        logger.info(f"Fetching feed with {cursor=}")

        response = raise_for_status(requests.get(url, params=params))
        return response.json()

    def get_feed(self):
        has_more = True
        cursor = None

        while has_more:
            time.sleep(3)
            data = self._get_feed(cursor)
            results = data.pop("results")

            logger.info(f"Feed data: {data}")
            for item in results:
                yield Message.from_json(item)

            cursor = data.get("cursor", None)
            has_more = data.get("has_more", False)

    def get_feed_stack(self, stackid):
        has_more = True
        offset = 0
        while has_more:
            time.sleep(1)
            logger.info(f"Fetching feed stack {stackid}: {offset=}")
            data = self._get_feed_stack(stackid, offset)
            results = data.pop("results")
            for item in results:
                yield Message.from_json(item)

            has_more = data.get("has_more", False)
            offset = data.get("next_offset", 0)

    def whoami(self):
        return raise_for_status(
            requests.get(
                f"{API_BASE_URL}/user/whoami",
                params={
                    "access_token": self.access_token,  # this doesn't work?
                },
            )
        ).json()


def populate_feed(da: DeviantArt, db: sqlite3.Connection):
    stacks = set()

    query = Select(Message, ["messageid"]).where(f"messageid is not null")
    rows = db.execute(query.sql()).fetchall()
    messages = {row[0] for row in rows}

    inserted = 0
    for item in da.get_feed():
        logger.debug(item)

        if item.stackid and item.stack_count > 1:
            stacks.add(item.stackid)
            item.timestamp = None

        item.deviationid = (item.deviation and item.deviation.deviationid) or (
            item.subject and item.subject.get("deviation", {}).get("deviationid")
        )

        item.insert(db, conflict_mode="replace")
        item.originator.insert(db, conflict_mode="replace")

        inserted += 1

        if item.messageid in messages:
            logger.info(
                f"Stopping feed collection because message {item.messageid} already exists"
            )
            break

    logger.info(f"Processed {inserted} stacks")


def populate_feed_stacks(da: DeviantArt, db: sqlite3.Connection):
    query = (
        Select(Message, ["stackid", "deviationid", "stack_count", "count(*)"])
        .where(f"stackid is not null and stack_count > 1")
        .group_by("stackid", "deviationid", "stack_count")
        .having(f"count(*) <> stack_count")
    )
    logger.info(query.sql())

    rows = db.execute(
        Select(Message, ["messageid"]).where(f"stackid is null").sql()
    ).fetchall()
    messages = {row[0] for row in rows}

    rows = db.execute(query.sql()).fetchall()
    for stack, deviationid, stack_count, count in rows:
        logger.info(f"Processing stack {stack}: {deviationid=} {stack_count=} {count=}")
        inserted = 0
        for item in da.get_feed_stack(stack):

            logger.debug(item)
            item.deviationid = (item.deviation and item.deviation.deviationid) or (
                item.subject and item.subject.get("deviation", {}).get("deviationid")
            )

            item.insert(
                db, conflict_mode="replace", allow_nulls=["stackid", "stack_count"]
            )

            item.originator.insert(db, conflict_mode="replace")

            if item.messageid in messages:
                logger.info(f"Message {item.messageid} already exists")
                break

            inserted += 1

        logger.info(f"Inserted {inserted} messages for stack {stack}")
        db.commit()


def populate_gallery(
    da: DeviantArt,
    db: sqlite3.Connection,
    gallery="all",
    username=None,
    full=False,
    offset=0,
):
    deviation_ids = []

    # These are ordered newest first
    for i, item in enumerate(
        da.get_all_deviations(gallery=gallery, offset=offset, username=username)
    ):
        logger.debug(item)
        deviation_ids.append(item.deviationid)
        author = item.author
        if author:
            r = author.insert(db, conflict_mode="replace")
            item.user_id = author.userid

        if item.thumbs and not os.path.exists(
            f"{file_path}/thumbs/{item.deviationid}.jpg"
        ):
            for thumb in item.thumbs:
                if thumb.src:
                    os.makedirs("thumbs", exist_ok=True)
                    res = requests.get(thumb.src)
                    if res.status_code == 200:
                        with open(f"thumbs/{item.deviationid}.jpg", "wb") as F:
                            F.write(res.content)
                        break

        q = Select(Deviation).where(f"deviations.deviationid == '{item.deviationid}'")
        rs = db.execute(q.sql())
        if rs.fetchone():
            logger.debug(f"Deviation {item.deviationid} already exists")
            if not full:
                break

        item.insert(db, conflict_mode="replace")

        if i % 24 == 0:
            db.commit()

    db.commit()

    if full and deviation_ids:
        q = f"""update deviations set is_deleted = true, updated_at = datetime('now') where deviationid not in ('{"','".join(deviation_ids)}')"""
        logger.info(q)
        db.execute(q)
        db.commit()


def populate_metadata(da: DeviantArt, db: sqlite3.Connection):
    select = (
        Select(
            Deviation,
            ["deviationid"],
        )
        .join(DeviationMetadata, on="deviationid", how="left")
        .where(
            "deviation_metadata.deviationid is null or mod(deviation_metadata.rowid, 24) = :mod"
        )
    )
    print(select.sql())

    rs = db.execute(select.sql(), {"mod": datetime.now().hour})
    rows = rs.fetchall()
    logger.info(f"Fetching metadata for {len(rows)} deviations")

    deviation_ids = [str(row[0]) for row in rows]
    for i, item in enumerate(da.get_metadata(deviation_ids)):
        user = item.author
        if user:
            user.insert(db, conflict_mode="replace")
            item.user_id = user.userid

        item.insert(db, conflict_mode="replace")

        for c in item.collections:
            r = c.insert(db, conflict_mode="replace")

        for g in item.galleries:
            r = g.insert(db, conflict_mode="replace")

        db.execute(
            f"UPDATE deviations SET stats = ?, title = ? WHERE deviationid = ?",
            (
                json.dumps(
                    {
                        "favourites": item.stats.favourites,
                        "comments": item.stats.comments,
                    }
                ),
                item.title,
                item.deviationid,
            ),
        )
        if i % 10 == 0:
            db.commit()
    db.commit()


def populate_favorites(da: DeviantArt, db: sqlite3.Connection):
    select = (
        Select(
            DeviationMetadata,
            [
                "deviationid",
                "cast(stats->'favourites' as integer)",
                f"count({DeviationActivity.table_name}.deviationid)",
            ],
        )
        .join(DeviationActivity, on="deviationid", how="left")
        .where(f"action = 'fave'")
        .group_by("1,2")
        .having(
            f"cast(stats->'favourites' as integer) <> count({DeviationActivity.table_name}.deviationid)"
        )
    )
    logger.info(select.sql())
    rows = db.execute(select.sql()).fetchall()

    for deviation_id, fav, count in rows:
        logger.info(f"Fetching /whofaved for {deviation_id=}: ({count=}, {fav=})")
        if count > fav:
            db.execute(
                f"DELETE FROM {DeviationActivity.table_name} WHERE deviationid = ?",
                (deviation_id,),
            )
            logger.info(f"Deleted {count - fav} rows for deviation: {deviation_id}")
            count = 0

        for item in da.get_whofaved(deviation_id, offset=count):
            user = User.from_json(item.get("user"))
            if user:
                user.insert(db, conflict_mode="replace")

                a = DeviationActivity(
                    deviationid=deviation_id,
                    userid=user.userid,
                    time=item.get("time"),
                    action="fave",
                    timestamp=datetime.fromtimestamp(item.get("time")),
                )
                a.insert(db, conflict_mode="ignore")
        db.commit()
        time.sleep(1)


def populate(da: DeviantArt, full=False, username=None, offset=0):

    da.check_token()

    with sqlite3.connect(da.sqlite_db) as db:
        for table in [
            User,
            Deviation,
            DeviationMetadata,
            DeviationActivity,
            Collection,
            Gallery,
            Message,
        ]:
            existing = get_table_info(da.sqlite_db, table.table_name)
            if not existing["columns"]:
                logging.info(table.create_table_sql())
                db.execute(table.create_table_sql())
                continue

            new_info = create_temp_db_from_sql(table.create_table_sql())

            alter_statements = generate_alter_statements(
                existing, new_info, table.table_name
            )

            for stmt in alter_statements:
                logging.info(stmt)
                db.execute(stmt)

        populate_gallery(
            da, db, gallery="all", username=username, full=full, offset=offset
        )
        db.commit()

        populate_metadata(da, db)
        db.commit()

        # populate_favorites(da, db)
        # db.commit()

        populate_feed(da, db)
        db.commit()

        populate_feed_stacks(da, db)
        db.commit()


def download_images(da: DeviantArt, output_folder="images"):
    """Download full size images for all deviations in the database"""
    import os
    import requests
    from pathlib import Path

    # Create output folder if it doesn't exist
    Path(output_folder).mkdir(exist_ok=True)

    with sqlite3.connect(da.sqlite_db) as db:
        # Get all deviations with content info
        cursor = db.execute("SELECT deviationid, title, content FROM deviations")
        deviations = cursor.fetchall()

        for deviationid, title, content in deviations:
            print(deviationid, title)

            if content := json.loads(content or "{}"):
                if full_size_url := content.get("src"):
                    # Determine file extension from URL
                    file_extension = full_size_url.split(".")[-1].split("?")[0]
                    if file_extension not in ["jpg", "jpeg", "png", "gif", "webp"]:
                        file_extension = "jpg"  # Default fallback

                    safe_title = "".join(
                        c for c in title if c.isalnum() or c in (" ", "-", "_")
                    ).rstrip()
                    safe_title = safe_title[:100]

                    filename = f"{deviationid}_{safe_title}.{file_extension}"
                    filepath = os.path.join(output_folder, filename)

                    if os.path.exists(filepath):
                        logging.info(f"Skipping {filename} because it already exists")
                        continue

                    deviation = da.get_deviation(deviationid)
                    if not deviation:
                        logging.warning(f"Deviation {deviationid} not found")
                        continue
                    if not deviation.content:
                        logging.warning(f"Deviation {deviationid} has no content")
                        continue

                    full_size_url = deviation.content.src
                    if not full_size_url:
                        logging.warning(f"Deviation {deviationid} has no full size URL")
                        continue

                    # Download the image
                    logging.info(f"Downloading {filename}...")
                    response = requests.get(full_size_url)
                    try:
                        response.raise_for_status()
                    except Exception as e:
                        logging.error(f"Error downloading {filename}: {e}")
                        continue

                    with open(filepath, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)

                    logging.info(f"Successfully downloaded {filename}")

                    # Add a small delay to be respectful to the API
                    time.sleep(0.5)

                else:
                    logging.warning(
                        f"No full size URL found for deviation {deviationid}"
                    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--username", type=str, default=None)
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--skip", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    if args.username:
        da = DeviantArt(sqlitedb=os.path.join(file_path, f"{args.username}.sqlite"))
    else:
        da = DeviantArt()

    da.check_token()

    # download_images(da)
    populate(da, args.full, args.username)

    print("Data collection completed.")

    with sqlite3.connect(da.sqlite_db) as db:
        rs = db.execute(
            """SELECT title, count(*) filter(where type='feedback.favourite' or type='feedback.collect'), count(*) filter(where type='feedback.comment'), url
            FROM deviations 
            join messages on deviations.deviationid = messages.deviationid
            group by title, url
            order by count(*) desc limit 15"""
        )
        from tabulate import tabulate

        headers = ["Title", "Favorites", "Comments", "URL"]
        print("\n" + tabulate(rs, headers=headers, tablefmt="grid"))
