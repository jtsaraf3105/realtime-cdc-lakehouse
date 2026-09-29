-- Order header total must equal the sum of its (non-deleted) lines, to the cent.
select order_id, total_amount, lines_amount
from {{ ref('int_orders_enriched') }}
where not is_deleted and line_count > 0 and abs(total_amount - lines_amount) > 0.01
