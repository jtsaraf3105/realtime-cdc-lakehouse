select
    order_item_id,
    order_id,
    product_id,
    quantity,
    cast(unit_price as decimal(10, 2)) as unit_price,
    is_deleted
from {{ source('raw', 'order_items') }}
