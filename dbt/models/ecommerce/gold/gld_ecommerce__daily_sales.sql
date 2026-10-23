-- Revenue per day and category; cancelled and refunded orders don't count.
select
    o.order_date,
    i.category,
    count(distinct o.order_id) as orders,
    sum(i.quantity) as units,
    sum(i.gross_amount) as gross_revenue,
    sum(i.discount) as discounts,
    sum(i.net_amount) as net_revenue,
    o._batch_id
from {{ ref('slv_ecommerce__orders') }} o
join {{ ref('slv_ecommerce__order_items') }} i on i.order_id = o.order_id
where o.status not in ('cancelled', 'refunded')
group by o.order_date, i.category, o._batch_id
