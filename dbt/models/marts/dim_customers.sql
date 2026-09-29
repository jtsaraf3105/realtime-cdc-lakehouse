-- SCD Type 2 dimension: one row per customer version; filter is_current for today's view.
select
    {{ dbt_utils_surrogate_key(['customer_id', 'version_lsn']) }} as customer_sk,
    customer_id,
    email,
    full_name,
    city,
    country,
    tier,
    valid_from,
    valid_to,
    is_current
from {{ ref('stg_customer_history') }}
where not is_deleted
