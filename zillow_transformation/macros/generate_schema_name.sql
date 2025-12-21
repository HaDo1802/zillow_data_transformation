{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-- Always use the target schema, ignore custom_schema_name --#}
    {{ target.schema }}
{%- endmacro %}