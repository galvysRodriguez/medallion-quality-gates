with items as (
    select distinct
        trim(order_id) as order_id,
        cast(line_number as integer) as line_number,
        trim(sku) as sku,
        cast(quantity as integer) as quantity,
        cast(unit_price as numeric(12, 2)) as unit_price,
        coalesce(cast(discount as numeric(12, 2)), 0) as discount,
        _batch_id
    from {{ ref('brz_ecommerce__order_items') }}
)

select
    i.*,
    i.quantity * i.unit_price as gross_amount,
    i.quantity * i.unit_price - i.discount as net_amount,
    p.category,
    p.active as product_active
from items i
left join {{ ref('slv_ecommerce__products') }} p on p.sku = i.sku
