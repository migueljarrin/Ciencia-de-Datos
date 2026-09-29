{#- Usa el nombre de schema tal cual (BRONZE, SILVER, GOLD, REFERENCE)
    en lugar del prefijo por defecto de dbt (ANALYTICS_BRONZE, ...). -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
