-- One row per account and day with the posted money movement.
select
    t.account_id,
    cast(t.txn_ts at time zone 'UTC' as date) as position_date,
    t.currency,
    a.credit_limit,
    sum(greatest(t.signed_amount, 0)) as inflows,
    sum(greatest(-t.signed_amount, 0)) as outflows,
    sum(t.signed_amount) as net_flow,
    count(*) as txn_count,
    t._batch_id
from {{ ref('slv_fintech__transactions') }} t
join {{ ref('slv_fintech__accounts') }} a on a.account_id = t.account_id
where t.status = 'posted'
group by 1, 2, 3, 4, t._batch_id
