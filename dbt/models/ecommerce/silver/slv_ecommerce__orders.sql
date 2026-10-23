with orders as (
    select distinct
        trim(order_id) as order_id,
        trim(customer_id) as customer_id,
        cast(order_ts as timestamptz) as order_ts,
        lower(trim(status)) as status,
        upper(trim(shipping_country)) as shipping_country,
        cast(total_amount as numeric(12, 2)) as total_amount,
        _batch_id
    from {{ ref('brz_ecommerce__orders') }}
),

lines as (
    select order_id, count(*) as item_count, sum(net_amount) as items_net_total
    from {{ ref('slv_ecommerce__order_items') }}
    group by order_id
)

select
    o.*,
    cast(o.order_ts at time zone 'UTC' as date) as order_date,
    coalesce(l.item_count, 0) as item_count,
    coalesce(l.items_net_total, 0) as items_net_total
from orders o
left join lines l on l.order_id = o.order_id
