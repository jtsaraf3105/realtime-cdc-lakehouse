-- Orders, including soft-deleted (purged cancelled) ones so history stays complete.
select
    order_id,
    customer_id,
    status,
    cast(total_amount as decimal(12, 2)) as total_amount,
    {{ to_utc('created_at') }} as created_at,
    {{ to_utc('updated_at') }} as updated_at,
    is_deleted
from {{ source('raw', 'orders') }}
