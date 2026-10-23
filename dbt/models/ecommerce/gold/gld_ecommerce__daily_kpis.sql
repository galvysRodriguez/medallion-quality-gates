select
    order_date,
    count(*) as orders,
    count(distinct customer_id) as customers,
    sum(items_net_total) as revenue,
    round(sum(items_net_total) / count(*), 2) as average_order_value,
    _batch_id
from {{ ref('slv_ecommerce__orders') }}
where status not in ('cancelled', 'refunded')
group by order_date, _batch_id
