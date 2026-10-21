{#
    Override the default dbt schema generation logic to use the folder name as the schema name.
#}
{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {%- if custom_schema_name is not none -%}
        {%- set folder_schema = custom_schema_name | trim -%}
    {%- elif node is not none and node.resource_type == 'model' and node.fqn | length > 3 -%}
        {%- set folder_schema = clean_folder_name(node.fqn[2]) | trim -%}
    {%- else -%}
        {%- set folder_schema = none -%}
    {%- endif -%}

    {%- if folder_schema is none -%}
        {{ target.schema }}
    {%- elif target.name == 'prod' -%}
        {{ folder_schema }}
    {%- else -%}
        {{ target.schema }}_{{ folder_schema }}
    {%- endif -%}
{%- endmacro %}
