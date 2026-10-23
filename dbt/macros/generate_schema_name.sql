{#-
    Derives the schema from the folder layout so new domains need no configuration:
      models/<domain>/<layer>/...  ->  <domain>_<layer>   (gold -> <domain>_gold_staging)
    An explicit `schema` config (e.g. dq_audit for tests) is used verbatim.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set fqn = node.fqn -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- elif node.resource_type == 'model' and fqn | length >= 4 and fqn[2] in ('bronze', 'silver', 'gold') -%}
        {{ fqn[1] }}_{{ 'gold_staging' if fqn[2] == 'gold' else fqn[2] }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
