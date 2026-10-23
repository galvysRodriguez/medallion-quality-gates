with events as (
    select distinct
        trim(event_id) as event_id,
        trim(user_id) as user_id,
        trim(content_id) as content_id,
        cast(event_ts as timestamptz) as event_ts,
        lower(trim(event_type)) as event_type,
        cast(position_seconds as integer) as position_seconds,
        coalesce(cast(watch_seconds as integer), 0) as watch_seconds,
        lower(trim(device)) as device,
        _batch_id
    from {{ ref('brz_streaming__playback_events') }}
)

select
    e.*,
    cast(e.event_ts at time zone 'UTC' as date) as event_date,
    u.plan as user_plan,
    u.signup_date as user_signup_date,
    c.content_type,
    c.duration_seconds as content_duration_seconds
from events e
left join {{ ref('slv_streaming__users') }} u on u.user_id = e.user_id
left join {{ ref('slv_streaming__content') }} c on c.content_id = e.content_id
