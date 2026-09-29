-- SCD2 invariant: a customer's versions tile time without overlaps.
select a.customer_id
from {{ ref('dim_customers') }} as a
join {{ ref('dim_customers') }} as b
    on a.customer_id = b.customer_id and a.customer_sk <> b.customer_sk
    and a.valid_from < coalesce(b.valid_to, timestamp '9999-12-31')
    and b.valid_from < coalesce(a.valid_to, timestamp '9999-12-31')
