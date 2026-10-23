{#- Bronze models: the rows of `relation` that belong to the batch being processed. -#}
{% macro select_current_batch(relation) -%}
    {%- set batch_id = var('batch_id') -%}
    select *
    from {{ relation }}
    {% if batch_id -%}
        {%- if not modules.re.match('^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$', batch_id | string) -%}
            {{ exceptions.raise_compiler_error("invalid batch_id: " ~ batch_id) }}
        {%- endif %}
    where _batch_id = '{{ batch_id }}'
    {%- else %}
    where _batch_id = (select max(_batch_id) from {{ relation }})
    {%- endif %}
{%- endmacro %}
