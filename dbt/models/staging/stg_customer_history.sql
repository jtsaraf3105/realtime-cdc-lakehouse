-- One row per customer version (SCD2). valid_to is null for the open (current) version.
select
    customer_id,
    _lsn as version_lsn,
    lower(email) as email,
    full_name,
    city,
    country,
    tier,
    {{ to_utc('valid_from') }} as valid_from,
    {{ to_utc('valid_to') }} as valid_to,
    is_current,
    is_deleted
from {{ source('raw', 'customers_history') }}
