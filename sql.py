import duckdb
from models import *
from datetime import datetime, timedelta
import os

conn = None


def get_connection():
    global conn
    if not conn and os.path.exists("deviantart_data.db"):
        conn = duckdb.connect("deviantart_data.db", read_only=True)
    return conn


def top_by_activity(start_time, limit=10):

    # Connect to the database
    query = (
        Select(Deviation)
        .join(DeviationActivity, on="deviationid")
        .select(
            "deviations.*",
            "count(*) count",
        )
        .where(f"timestamp >= '{start_time}'")
        .group_by("deviations.*")
        .order_by("count(*) desc, published_time")
    )
    conn = get_connection()
    if not conn:
        logger.error("Database connection not available.")
        return []

    with conn.cursor() as cursor:
        print(query.sql(limit=limit))

        cursor.execute(query.sql(limit=limit))
        columns = [col[0].lower() for col in cursor.description]

        # Convert the cursor to a list of dictionaries
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def calculate_grouping_minutes(start_date, max_groups=100):
    """Calculates the number of minutes for grouping, expanding as the date range increases."""
    start_date = datetime.fromisoformat(start_date)
    now = datetime.now(tz=start_date.tzinfo)

    diff_minutes = int((now - start_date).total_seconds() / 60)

    if diff_minutes <= 1440 * 3:  # 3 days
        return 15
    elif diff_minutes <= 10080:  # 1 week
        return 60
    elif diff_minutes <= 43200:  # 1 month
        return 60 * 6
    else:
        return 60 * 24


def get_deviation_activity(deviationid, start_date):
    grouping_minutes = calculate_grouping_minutes(start_date)
    table_name = DeviationActivity.table_name

    query = f"""
        WITH grouped_data AS (
            SELECT to_timestamp((extract(epoch from timestamp) / {grouping_minutes * 60})::int * {grouping_minutes * 60}) as time_bucket,
                   COUNT(*) as count
            FROM {table_name}
            WHERE timestamp >= '{start_date}'
            AND deviationid = '{deviationid}'
            GROUP BY time_bucket
        ),
        time_series AS (
            SELECT
                gs.range as time_bucket
            FROM range(
                to_timestamp((extract(epoch from '{start_date}'::TIMESTAMPTZ) / {grouping_minutes * 60})::int * {grouping_minutes * 60}), '{datetime.now()}'::TIMESTAMPTZ, INTERVAL '{grouping_minutes} minute'
            ) gs
        )
        SELECT ts.time_bucket as timestamp, gd.count, COALESCE(gd.count, 0) as count
        FROM time_series ts
        LEFT JOIN grouped_data gd
        ON ts.time_bucket = gd.time_bucket
        ORDER BY ts.time_bucket
    """

    conn = get_connection()
    if not conn:
        logger.error("Database connection not available.")
        return []

    print(query)
    with conn.cursor() as cursor:
        cursor.execute(query)

        columns = [col[0].lower() for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
