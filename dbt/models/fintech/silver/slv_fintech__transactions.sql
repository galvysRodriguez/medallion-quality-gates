with transactions as (
    select distinct
        trim(transaction_id) as transaction_id,
        trim(account_id) as account_id,
        cast(txn_ts as timestamptz) as txn_ts,
        lower(trim(txn_type)) as txn_type,
        cast(amount as numeric) as amount,
        upper(trim(currency)) as currency,
        nullif(trim(merchant_category), '') as merchant_category,
        lower(trim(status)) as status,
        _batch_id
    from {{ ref('brz_fintech__transactions') }}
)

select
    t.*,
    case when t.txn_type in ('deposit', 'transfer_in') then t.amount else -t.amount end as signed_amount,
    a.currency as account_currency,
    a.status as account_status
from transactions t
left join {{ ref('slv_fintech__accounts') }} a on a.account_id = t.account_id
