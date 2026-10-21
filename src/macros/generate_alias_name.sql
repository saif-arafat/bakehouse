{#
   Override the default alias for the bronze layer.
#}
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name is none and node.name.startswith('brz_') and '__' in node.name -%}
        {{ node.name.split('__', 1)[1] }}
    {%- else -%}
        {{ default__generate_alias_name(custom_alias_name, node) }}
    {%- endif -%}
{%- endmacro %}
