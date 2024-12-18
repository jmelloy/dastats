import requests
import duckdb
import time
import os
import json
import logging
from datetime import datetime
import pandas as pd

from models import Deviation, DeviationActivity, Select

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Constants for DeviantArt API
API_BASE_URL = "https://www.deviantart.com/api/v1/oauth2"
COLLECTIONS_ENDPOINT = "/gallery/all"
WHOFAVED_ENDPOINT = "/deviation/whofaved"
METADATA_ENDPOINT = "/deviation/metadata"

AUTHORIZATION_BASE_URL = "https://www.deviantart.com/oauth2/authorize"
TOKEN_URL = "https://www.deviantart.com/oauth2/token"
REDIRECT_URI = "http://localhost:8080/callback"


def raise_for_status(response):
    try:
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Error: {e}")
        logger.warning(response.text)
        raise
    return response


class DeviantArt:
    def __init__(self):
        self.access_token = ""
        self.refresh_token = ""
        self.expires = 0
        self.client_id = None
        self.client_secret = None

        if os.path.exists(".token.json"):
            with open(".token.json", "r") as F:
                data = json.loads(F.read())
                self.access_token = data["access_token"]
                self.refresh_token = data["refresh_token"]
                self.expires = data.get("expires_at", 0)

        if os.path.exists(".credentials.json"):
            with open(".credentials.json", "r") as F:
                data = json.loads(F.read())
                self.client_id = data["client_id"]
                self.client_secret = data["client_secret"]

    def authorization_url(self):
        return f"{AUTHORIZATION_BASE_URL}?client_id={self.client_id}&redirect_uri={REDIRECT_URI}&response_type=code&scope=browse"

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

        self.set_token(token_response.json())

        return token_response.json()

    def get_refresh_token(self):
        if not self.client_id or not self.client_secret:
            raise ValueError("Client ID and Client Secret not set.")

        if not self.refresh_token:
            raise ValueError("No refresh token found.")

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

    def _get_deviations(self, offset=0, limit=24, gallery="all"):
        url = f"{API_BASE_URL}/gallery/{gallery}"
        params = {
            "access_token": self.access_token,
            "offset": offset,
            "limit": limit,
            "mature_content": True,
        }
        response = raise_for_status(requests.get(url, params=params))

        return response.json()

    def get_all_deviations(self, gallery="all"):
        offset = 0
        limit = 24
        has_more = True
        while has_more:
            logger.info(f"Fetching gallery {gallery}: offset={offset}, limit={limit}")
            data = self._get_deviations(offset, limit, gallery=gallery)
            results = data.get("results", [])
            has_more = data.get("has_more", False)
            offset = data.get("next_offset", 0)
            if not results:
                break

            # Insert deviations into the database
            for item in results:
                yield Deviation.from_json(item)

    def get_whofaved(self, deviation_id, offset=0):
        url = f"{API_BASE_URL}/deviation/whofaved"

        params = {
            "deviationid": deviation_id,
            "offset": offset,
            "access_token": self.access_token,
        }
        return raise_for_status(requests.get(url, params=params)).json()

    def get_metadata(self, deviation_ids: list):
        url = f"{API_BASE_URL}/deviation/metadata"
        params = {
            "deviationids": ",".join(deviation_ids),
            "access_token": self.access_token,
            "ext_camera": "true",
            "ext_stats": "true",
            "ext_collection": "true",
            "ext_gallery": "true",
        }
        return raise_for_status(requests.get(url, params=params)).json()

    def whoami(self):
        return raise_for_status(
            requests.get(
                f"{API_BASE_URL}/user/whoami",
                params={
                    "access_token": self.access_token,  # this doesn't work?
                },
            )
        ).json()


def populate_gallery(da: DeviantArt, gallery="all"):
    # Initialize DuckDB connection
    with duckdb.connect("deviantart_data.db", read_only=False) as db:
        logging.info(Deviation.create_table_sql())
        db.execute(Deviation.create_table_sql())

        for item in da.get_all_deviations(gallery=gallery):
            logger.debug(item)
            item.insert(db, duplicate="deviationid")


def populate_metadata(da: DeviantArt):
    with duckdb.connect("deviantart_data.db", read_only=False) as db:
        offset = 0
        rows = [1]
        limit = 10
        while rows:
            rows = db.execute(
                Select(Deviation, ["deviationid"]).sql(offset=offset, limit=limit)
            ).fetchall()

            deviation_ids = [row[0] for row in rows]
            response = da.get_metadata(deviation_ids)

            results = response.get("results", [])
            for item in results:
                deviation = Deviation.from_json(item)
                deviation.insert(db, duplicate="deviationid")

        # Throttle API calls to avoid rate limiting
        time.sleep(1)


def populate_favorites(da: DeviantArt):
    with duckdb.connect("deviantart_data.db", read_only=False) as db:
        logger.info(DeviationActivity.create_table_sql())
        db.execute(DeviationActivity.create_table_sql())
        select = (
            Select(
                Deviation,
                [
                    "deviationid",
                    "stats.favourites",
                    f"count({DeviationActivity.table_name}.deviationid)",
                ],
            )
            .join(DeviationActivity, on="deviationid", how="left")
            .group_by("deviationid", "stats.favourites")
            .having(
                f"stats.favourites <> count({DeviationActivity.table_name}.deviationid)"
            )
        )
        logger.info(select.sql())
        rows = db.execute(select.sql()).fetchall()

        for deviation_id, fav, count in rows:
            logger.info(f"Fetching /whofaved for deviation: {deviation_id}")
            try:
                offset = 0
                has_more = True
                while has_more:
                    metadata = da.get_whofaved(deviation_id, offset=offset)
                    has_more = metadata.get("has_more", False)
                    results = metadata.get("results", [])

                    if not results:
                        break

                    for item in results:
                        a = DeviationActivity(
                            deviationid=deviation_id,
                            userid=item.get("user", {}).get("userid"),
                            user=item.get("user"),
                            time=item.get("time"),
                            action="fave",
                            timestamp=datetime.fromtimestamp(item.get("time")),
                        )
                        a.insert(db, ignore_conflicts=True)

                    else:
                        offset = metadata.get("next_offset", 0)

                # Throttle API calls to avoid rate limiting
                time.sleep(1)
            except requests.RequestException as e:
                logger.error(f"Error fetching metadata for {deviation_id}: {e}")


def populate(da: DeviantArt):
    populate_gallery(da, gallery="all")
    # populate_metadata(da)
    populate_favorites(da)


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    da = DeviantArt()
    da.check_token()

    populate_gallery(da, gallery="all")
    populate_favorites(da)

    print("Data collection completed.")

    with duckdb.connect("deviantart_data.db", read_only=True) as db:
        df = db.execute(
            """SELECT title, stats.favourites, stats.comments, url
            FROM deviations order by stats.favourites + stats.comments desc limit 15"""
        ).fetch_df()

        with pd.option_context("display.max_colwidth", None):
            print(df)
