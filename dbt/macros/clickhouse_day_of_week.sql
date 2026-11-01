{% macro clickhouse__day_of_week(date, isoweek=true) -%}
{% if isoweek %}toDayOfWeek({{ date }}){% else %}toDayOfWeek({{ date }}, 3) + 1{% endif %}
{%- endmacro %}
