select
    event_date,
    user_id,
    user_plan as plan,
    count(*) filter (where event_type = 'start') as sessions,
    sum(watch_seconds) as watch_seconds,
    _batch_id
from {{ ref('slv_streaming__playback_events') }}
group by event_date, user_id, user_plan, _batch_id
