{#- Normalise any timestamp / ISO-8601 string (e.g. Debezium's "2026-09-29T16:33:20.39Z") to a
    naive UTC timestamp. Mixing naive and tz-aware timestamps makes the warehouse interpret the
    naive side in the *session* time zone — on an IST laptop that shifted every order 5.5 h
    before its customer existed and broke the SCD2 point-in-time join. -#}
{% macro to_utc(col) -%}
    {{ return(adapter.dispatch('to_utc')(col)) }}
{%- endmacro %}

{% macro default__to_utc(col) -%}
    timezone('UTC', cast({{ col }} as timestamptz))
{%- endmacro %}

{% macro snowflake__to_utc(col) -%}
    convert_timezone('UTC', to_timestamp_tz({{ col }}))::timestamp_ntz
{%- endmacro %}
