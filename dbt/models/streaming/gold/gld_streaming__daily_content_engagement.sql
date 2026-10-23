select
    event_date,
    content_id,
    content_type,
    count(*) filter (where event_type = 'start') as plays,
    count(distinct user_id) as unique_viewers,
    round(sum(watch_seconds) / 3600.0, 4) as watch_hours,
    count(*) filter (where event_type = 'complete') as completions,
    round(
        (count(*) filter (where event_type = 'complete'))::numeric
        / nullif(count(*) filter (where event_type = 'start'), 0), 4
    ) as completion_rate,
    _batch_id
from {{ ref('slv_streaming__playback_events') }}
group by event_date, content_id, content_type, _batch_id
