select
    product_id,
    sku,
    name as product_name,
    category,
    cast(price as decimal(10, 2)) as current_price,
    active,
    {{ to_utc('updated_at') }} as updated_at
from {{ source('raw', 'products') }}
where not is_deleted
