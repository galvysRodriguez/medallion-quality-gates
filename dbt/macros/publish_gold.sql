{#-
    Write-Audit-Publish: gold is built and tested in <domain>_gold_staging; only after
    every rule passed does this swap it into <domain>_gold, atomically, in one statement.
    The previous version is kept as <domain>_gold_previous for rollback.

        dbt run-operation publish_gold --args '{domain: fintech}'
-#}
{% macro publish_gold(domain) %}
    {%- if not modules.re.match('^[a-z][a-z0-9_]*$', domain) -%}
        {{ exceptions.raise_compiler_error("invalid domain: " ~ domain) }}
    {%- endif -%}
    {%- set staging = domain ~ '_gold_staging' -%}
    {%- set live = domain ~ '_gold' -%}
    {%- set previous = domain ~ '_gold_previous' -%}
    {%- set reader = var('gold_reader_role') -%}
    {% set sql %}
        do $$
        begin
            if not exists (select 1 from pg_namespace where nspname = '{{ staging }}') then
                raise exception 'nothing to publish: schema {{ staging }} does not exist';
            end if;
            drop schema if exists {{ previous }} cascade;
            if exists (select 1 from pg_namespace where nspname = '{{ live }}') then
                alter schema {{ live }} rename to {{ previous }};
            end if;
            alter schema {{ staging }} rename to {{ live }};
            if exists (select 1 from pg_roles where rolname = '{{ reader }}') then
                grant usage on schema {{ live }} to {{ reader }};
                grant select on all tables in schema {{ live }} to {{ reader }};
            end if;
        end $$;
    {% endset %}
    {% do run_query(sql) %}
    {{ log("published " ~ staging ~ " -> " ~ live, info=true) }}
{% endmacro %}

{#- Roll back to the previous gold version: dbt run-operation rollback_gold --args '{domain: fintech}' -#}
{% macro rollback_gold(domain) %}
    {%- if not modules.re.match('^[a-z][a-z0-9_]*$', domain) -%}
        {{ exceptions.raise_compiler_error("invalid domain: " ~ domain) }}
    {%- endif -%}
    {% set sql %}
        do $$
        begin
            if not exists (select 1 from pg_namespace where nspname = '{{ domain }}_gold_previous') then
                raise exception 'no previous gold version for {{ domain }}';
            end if;
            drop schema if exists {{ domain }}_gold cascade;
            alter schema {{ domain }}_gold_previous rename to {{ domain }}_gold;
        end $$;
    {% endset %}
    {% do run_query(sql) %}
    {{ log("rolled back " ~ domain ~ "_gold to its previous version", info=true) }}
{% endmacro %}
