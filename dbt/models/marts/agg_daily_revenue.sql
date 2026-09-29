-- Daily revenue from paid/shipped/delivered orders, by customer country at order time.
select
    order_date,
    customer_country_at_order as country,
    count(*) as orders,
    sum(units) as units,
    sum(total_amount) as revenue,
    sum(total_amount) / count(*) as avg_order_value
from {{ ref('fct_orders') }}
where is_revenue
group by 1, 2
