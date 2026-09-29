-- Grain: one row per order (purged orders excluded from the fact; they remain in staging).
select
    order_id,
    customer_id,
    customer_version_lsn,
    customer_city_at_order,
    customer_country_at_order,
    customer_tier_at_order,
    status,
    order_date,
    created_at,
    updated_at,
    line_count,
    units,
    total_amount,
    status in ('paid', 'shipped', 'delivered') as is_revenue
from {{ ref('int_orders_enriched') }}
where not is_deleted
