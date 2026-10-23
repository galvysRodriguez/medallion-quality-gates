-- Exact duplicates (replayed records) are collapsed; conflicting ones are left for the unique rule.
select distinct
    trim(account_id) as account_id,
    trim(customer_name) as customer_name,
    upper(trim(country)) as country,
    upper(trim(currency)) as currency,
    cast(opened_at as date) as opened_at,
    lower(trim(status)) as status,
    cast(credit_limit as numeric(18, 2)) as credit_limit,
    _batch_id
from {{ ref('brz_fintech__accounts') }}
