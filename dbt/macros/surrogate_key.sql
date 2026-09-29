{# Minimal portable surrogate key (md5 of the concatenated columns) — avoids a package
   dependency; equivalent to dbt_utils.generate_surrogate_key for our use. #}
{% macro dbt_utils_surrogate_key(cols) -%}
    md5({% for c in cols %}coalesce(cast({{ c }} as varchar), '_null_'){% if not loop.last %} || '|' || {% endif %}{% endfor %})
{%- endmacro %}
