-- Order lines priced at the unit price captured when the order was placed (not today's price).
select
    i.order_item_id,
    i.order_id,
    i.product_id,
    p.category,
    i.quantity,
    i.unit_price,
    i.quantity * i.unit_price as line_amount
from {{ ref('stg_order_items') }} as i
left join {{ ref('stg_products') }} as p on i.product_id = p.product_id
where not i.is_deleted
