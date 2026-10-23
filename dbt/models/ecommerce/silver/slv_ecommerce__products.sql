select distinct
    trim(sku) as sku,
    trim(name) as name,
    lower(trim(category)) as category,
    cast(unit_price as numeric(12, 2)) as unit_price,
    cast(active as boolean) as active,
    _batch_id
from {{ ref('brz_ecommerce__products') }}
