-- Orders with line totals and the customer's attributes AS OF the order time (SCD2 point-in-time
-- join): a customer who moved from Pune to Berlin keeps Pune on their older orders.
with lines as (
    select
        order_id,
        count(*) as line_count,
        sum(quantity) as units,
        sum(line_amount) as lines_amount
    from {{ ref('int_order_lines') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    h.version_lsn as customer_version_lsn,
    h.city as customer_city_at_order,
    h.country as customer_country_at_order,
    h.tier as customer_tier_at_order,
    o.status,
    o.created_at,
    o.updated_at,
    cast(o.created_at as date) as order_date,
    o.total_amount,
    coalesce(l.line_count, 0) as line_count,
    coalesce(l.units, 0) as units,
    coalesce(l.lines_amount, 0) as lines_amount,
    o.is_deleted
from {{ ref('stg_orders') }} as o
left join lines as l on o.order_id = l.order_id
left join {{ ref('stg_customer_history') }} as h
    on
        o.customer_id = h.customer_id
        and o.created_at >= h.valid_from
        and (h.valid_to is null or o.created_at < h.valid_to)
