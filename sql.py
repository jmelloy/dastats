import duckdb
from models import *
from datetime import datetime, timedelta


def top_by_activity(start_time=None, end_time=None, limit=10):

    # Connect to the database
    query = Select(
        Deviation,
        columns=[
            "deviationid",
            "title",
            "url",
            "published_time",
            "stats.favourites as favorites",
        ],
    )

    if start_time or end_time:
        query = (
            query.join(DeviationActivity, on="deviationid", how="left")
            .select(
                "deviationid", "title", "url", "published_time", "count(*) as favorites"
            )
            .group_by("deviations.*")
            .order_by("count(*) desc, published_time")
        )
        if start_time:
            query = query.where(f"timestamp >= '{start_time}'")
        if end_time:
            query = query.where(f"timestamp <= '{end_time}'")
    else:
        query = query.order_by("stats.favourites desc")

    with duckdb.connect(
        "deviantart_data.db", read_only=True, config={"access_mode": "READ_ONLY"}
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query.sql(limit=limit))
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_user_data(start_time, end_time, limit=10):

    # Connect to the database
    query = (
        Select(DeviationActivity)
        .join(User, on="userid")
        .select("userid", "username", "count(*) as favorites")
        .group_by("userid", "username")
        .order_by("count(*) desc")
    )

    if start_time:
        query = query.where(f"timestamp >= '{start_time}'")
    if end_time:
        query = query.where(f"timestamp <= '{end_time}'")

    with duckdb.connect(
        "deviantart_data.db", read_only=True, config={"access_mode": "READ_ONLY"}
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query.sql(limit=limit))
            columns = [col[0].lower() for col in cursor.description]
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

    with duckdb.connect(
        "deviantart_data.db", read_only=True, config={"access_mode": "READ_ONLY"}
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_publication_data(start_date, end_date):
    if not start_date:
        start_date = datetime(1970, 1, 1)
    if not end_date:
        end_date = datetime.now()

    query = f"""
    with deviationas as (
        SELECT to_timestamp(published_time::bigint)::date as date, COUNT(*) as count 
        FROM deviations
        WHERE published_time::bigint >= '{start_date.timestamp()}' AND published_time::bigint <= '{end_date.timestamp()}'
        GROUP BY 1
        ORDER BY 1
    ), activity as (
        SELECT timestamp::date as date, COUNT(*) as count
        FROM deviation_activity
        WHERE timestamp >= '{start_date}' AND timestamp <= '{end_date}'
        GROUP BY 1
        ORDER BY 1
    )
    SELECT coalesce(activity.date, deviationas.date) as date, coalesce(deviationas.count, 0) as deviations, coalesce(activity.count, 0) as favorites
    FROM activity
    FULL OUTER JOIN deviationas ON activity.date = deviationas.date
    """

    with duckdb.connect(
        "deviantart_data.db", read_only=True, config={"access_mode": "READ_ONLY"}
    ) as conn:
        with conn.cursor() as cursor:
            logger.info(query)
            cursor.execute(query)
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_gallery_data():
    query = """
        SELECT folderid, name
        FROM galleries
    """
    with duckdb.connect(
        "deviantart_data.db", read_only=True, config={"access_mode": "READ_ONLY"}
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
