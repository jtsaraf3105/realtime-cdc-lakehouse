select
    product_id,
    sku,
    product_name,
    category,
    current_price,
    active
from {{ ref('stg_products') }}
