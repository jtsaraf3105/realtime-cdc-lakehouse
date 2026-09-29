-- Current customers (deleted customers excluded).
select
    customer_id,
    lower(email) as email,
    full_name,
    city,
    country,
    tier,
    {{ to_utc('created_at') }} as created_at,
    {{ to_utc('updated_at') }} as updated_at
from {{ source('raw', 'customers') }}
where not is_deleted
