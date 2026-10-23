select distinct
    trim(customer_id) as customer_id,
    lower(trim(email)) as email,
    upper(trim(country)) as country,
    cast(signup_date as date) as signup_date,
    _batch_id
from {{ ref('brz_ecommerce__customers') }}
