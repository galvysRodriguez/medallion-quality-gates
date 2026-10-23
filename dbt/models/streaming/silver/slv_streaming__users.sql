select distinct
    trim(user_id) as user_id,
    upper(trim(country)) as country,
    lower(trim(plan)) as plan,
    cast(signup_date as date) as signup_date,
    _batch_id
from {{ ref('brz_streaming__users') }}
