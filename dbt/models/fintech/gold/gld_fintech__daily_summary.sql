select
    position_date,
    currency,
    count(distinct account_id) as active_accounts,
    sum(inflows) as inflows,
    sum(outflows) as outflows,
    sum(net_flow) as net_flow,
    sum(txn_count) as txn_count,
    _batch_id
from {{ ref('gld_fintech__account_daily_positions') }}
group by position_date, currency, _batch_id
