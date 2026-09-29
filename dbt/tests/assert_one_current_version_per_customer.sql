-- SCD2 invariant: each (non-deleted) customer has exactly one open version.
select customer_id, count(*) as n_current
from {{ ref('dim_customers') }}
where is_current
group by customer_id
having count(*) <> 1
