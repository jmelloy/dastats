with dev as (
    select
        deviationid,
        count(*)
    from
        deviation_activity
    where
        timestamp > '{start_date}'
    group by
        deviationid
    order by
        count(*) desc
    limit 10
),
timestamps as (
    select
        deviations.deviationid,
        url,
        date_trunc('minute', timestamp) as timestamp,
    title,
    stats,
    count(*) as deviation_activity_count
from
    deviations
    join dev on deviations.deviationid = dev.deviationid
    join deviation_activity on deviations.deviationid = deviation_activity.deviationid
    where
        timestamp > '{start_date}'
    group by
        deviations.deviationid,
        url,
        date_trunc('minute', timestamp),
        stats,
        title
)
select
    deviationid,
    url,
    title,
    stats,
    list (struct_pack(ts := timestamp, count := deviation_activity_count)) as timestamps
from
    timestamps
group by
    deviationid,
    url,
    stats,
    title
