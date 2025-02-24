import duckdb
from models import *
from datetime import datetime, timedelta


def top_by_activity(start_time=None, end_time=None, limit=10, gallery="all"):

    # Connect to the database
    query = Select(
        Deviation,
        columns=[
            "deviations.deviationid as deviationid",
            "deviations.title as title",
            "deviations.url as url",
            "deviations.published_time as published_time",
            "coalesce(deviation_metadata.stats.favourites, deviations.stats.favourites) as favorites",
            "deviation_metadata.stats.views as views",
            "deviation_metadata.stats.comments as comments",
            "deviation_metadata.stats.downloads as downloads",
        ],
    ).join(DeviationMetadata, on="deviationid", how="left")

    if gallery != "all":
        query = query.from_clause("unnest(deviation_metadata.galleries)")
        query = query.where(f"unnest.folderid = '{gallery}'")

    if start_time or end_time:
        query = (
            query.join(DeviationActivity, on="deviationid", how="left")
            .select(
                "deviationid",
                "deviations.title",
                "deviations.url",
                "deviations.published_time",
                "count(*) as favorites",
            )
            .group_by("all")
            .order_by("count(*) desc, deviations.published_time")
        )
        if start_time:
            query = query.where(f"timestamp >= '{start_time}'")
        if end_time:
            query = query.where(f"timestamp <= '{end_time}'")
    else:
        query = query.order_by(
            "deviations.stats.favourites desc, deviations.published_time"
        )

    with duckdb.connect("deviantart_data.db", read_only=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query.sql(limit=limit))
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_user_data(start_time, end_time, limit=10, gallery="all"):

    # Connect to the database
    query = (
        Select(DeviationActivity)
        .join(User, on="userid")
        .select("userid", "username", "usericon", "count(*) as favorites")
        .group_by("userid", "username", "usericon")
        .order_by("count(*) desc")
    )

    if gallery != "all":
        query = query.join(DeviationMetadata, on="deviationid")
        query = query.from_clause(" unnest(deviation_metadata.galleries)")
        query = query.where(f"unnest.folderid = '{gallery}'")

    if start_time:
        query = query.where(f"timestamp >= '{start_time}'")
    if end_time:
        query = query.where(f"timestamp <= '{end_time}'")

    with duckdb.connect("deviantart_data.db", read_only=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query.sql(limit=limit))
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


def calculate_grouping_minutes(start_date, end_date, max_groups=100):
    """Calculates the number of minutes for grouping, expanding as the date range increases."""
    if not end_date:
        end_date = datetime.now(tz=start_date.tzinfo)
    return 1440

    diff_minutes = int((end_date - start_date).total_seconds() / 60)

    if diff_minutes <= 1440 * 3:  # 3 days
        return 15
    elif diff_minutes <= 10080:  # 1 week
        return 60
    elif diff_minutes <= 43200:  # 1 month
        return 60 * 6
    else:
        return 60 * 24


def get_deviation_activity(deviationid, start_date, end_date):
    grouping_minutes = calculate_grouping_minutes(start_date, end_date)

    query = f"""
        WITH grouped_data AS (
            SELECT to_timestamp((extract(epoch from timestamp) / {grouping_minutes * 60})::int * {grouping_minutes * 60}) as time_bucket,
                   COUNT(*) as count
            FROM {DeviationActivity.table_name}
            WHERE timestamp >= '{start_date}'
            AND timestamp <= '{end_date}'
            AND deviationid = '{deviationid}'
            GROUP BY time_bucket
        ),
        time_series AS (
            SELECT
                gs.range as time_bucket
            FROM range(
                to_timestamp((extract(epoch from '{start_date}'::TIMESTAMPTZ) / {grouping_minutes * 60})::int * {grouping_minutes * 60}), '{end_date}'::TIMESTAMPTZ, INTERVAL '{grouping_minutes} minute'
            ) gs
        )
        SELECT ts.time_bucket as timestamp, gd.count, COALESCE(gd.count, 0) as count
        FROM time_series ts
        LEFT JOIN grouped_data gd
        ON ts.time_bucket = gd.time_bucket
        ORDER BY ts.time_bucket
    """

    with duckdb.connect("deviantart_data.db", True) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_publication_data(start_date, end_date, gallery="all"):
    if gallery == "all":
        gallery = None

    if not start_date:
        start_date = datetime(1970, 1, 1)
    if not end_date:
        end_date = datetime.now()

    if gallery:
        gallery_join = f"join deviation_metadata using (deviationid), unnest(deviation_metadata.galleries)"
        gallery_where = f"unnest.folderid = '{gallery}'"

    query = f"""
    with deviationas as (
        SELECT to_timestamp(published_time::bigint)::date as date, COUNT(distinct deviationid) as count 
        FROM deviations
        {gallery_join if gallery else ''}
        WHERE published_time::bigint >= '{start_date.timestamp()}' AND published_time::bigint <= '{end_date.timestamp()}'
        {" and " +gallery_where if gallery else ''}
        GROUP BY 1
        ORDER BY 1
    ), activity as (
        SELECT timestamp::date as date, COUNT(distinct deviationid) as count
        FROM deviation_activity
        {gallery_join if gallery else ''}
        WHERE timestamp >= '{start_date}' AND timestamp <= '{end_date}'
        {" and " +gallery_where if gallery else ''}
        GROUP BY 1
        ORDER BY 1
    )
    SELECT coalesce(activity.date, deviationas.date) as date, coalesce(deviationas.count, 0) as deviations, coalesce(activity.count, 0) as favorites
    FROM activity
    FULL OUTER JOIN deviationas ON activity.date = deviationas.date
    """

    with duckdb.connect("deviantart_data.db", read_only=True) as conn:
        with conn.cursor() as cursor:
            logger.debug(query)
            cursor.execute(query)
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_gallery_data():
    query = """
        SELECT folderid, name || ' (' || count(*) || ')' as name
        FROM galleries, deviation_metadata, unnest(deviation_metadata.galleries)
        WHERE folderid = unnest.folderid
        GROUP BY folderid, name
        ORDER BY count(*) DESC
    """
    with duckdb.connect("deviantart_data.db", read_only=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
