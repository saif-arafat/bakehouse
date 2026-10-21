{#
    Override the default catalog (database) name for a model. 
    Folder layout drives Unity Catalog placement: models/<layer>/<folder>/model.sql
    lands in catalog <layer>, schema <folder> (see generate_schema_name).
    Ordering prefixes are dropped: 01_bronze -> bronze, A_conformed -> conformed.
#}
{% macro clean_folder_name(folder) -%}
    {%- set parts = folder.split('_', 1) -%}
    {%- if parts | length == 2 and (parts[0].isdigit() or parts[0] | length == 1) -%}
        {{ parts[1] }}
    {%- else -%}
        {{ folder }}
    {%- endif -%}
{%- endmacro %}

{#
    Catalog = layer folder. An explicit database config still wins; seeds and
    anything outside a layer folder stay in the profile's catalog.
#}
{% macro generate_database_name(custom_database_name=none, node=none) -%}
    {%- if custom_database_name is not none -%}
        {{ custom_database_name | trim }}
    {%- elif node is not none and node.resource_type == 'model' and node.fqn | length > 2 -%}
        {{ clean_folder_name(node.fqn[1]) }}
    {%- else -%}
        {{ target.database }}
    {%- endif -%}
{%- endmacro %}
