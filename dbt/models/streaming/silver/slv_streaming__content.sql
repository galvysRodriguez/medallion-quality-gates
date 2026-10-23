select distinct
    trim(content_id) as content_id,
    trim(title) as title,
    lower(trim(content_type)) as content_type,
    cast(duration_seconds as integer) as duration_seconds,
    cast(release_date as date) as release_date,
    _batch_id
from {{ ref('brz_streaming__content') }}
