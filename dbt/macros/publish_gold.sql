{#-
    Write-Audit-Publish with history.

    Gold is built and tested in <domain>_gold_staging, which only ever holds the batch being
    processed. Once every rule passed, this upserts that batch as a partition of
    <domain>_gold (the consumer schema): for each staging table, delete the batch's rows
    from the live table and insert the audited ones — all tables in one transaction, so
    consumers never see a half-published batch. Re-publishing a batch replaces it.

        dbt run-operation publish_gold --args '{domain: fintech, batch_id: 2026-10-22}'
        dbt run-operation rollback_gold --args '{domain: fintech, batch_id: 2026-10-22}'
-#}

{% macro _validate_publish_args(domain, batch_id) %}
    {%- if not modules.re.match('^[a-z][a-z0-9_]*$', domain | string) -%}
        {{ exceptions.raise_compiler_error("invalid domain: " ~ domain) }}
    {%- endif -%}
    {%- if not modules.re.match('^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$', batch_id | string) -%}
        {{ exceptions.raise_compiler_error("invalid batch_id: " ~ batch_id) }}
    {%- endif -%}
{% endmacro %}

{% macro publish_gold(domain, batch_id) %}
    {{ _validate_publish_args(domain, batch_id) }}
    {%- set staging = domain ~ '_gold_staging' -%}
    {%- set live = domain ~ '_gold' -%}
    {%- set reader = var('gold_reader_role') -%}
    {% set sql %}
        do $$
        declare
            t record;
            col record;
            stray bigint;
            cols text;
        begin
            if not exists (select 1 from pg_namespace where nspname = '{{ staging }}') then
                raise exception 'nothing to publish: schema {{ staging }} does not exist';
            end if;
            create schema if not exists {{ live }};

            for t in
                select c.relname as name
                from pg_class c join pg_namespace n on n.oid = c.relnamespace
                where n.nspname = '{{ staging }}' and c.relkind = 'r'
                order by c.relname
            loop
                if not exists (
                    select 1 from information_schema.columns
                    where table_schema = '{{ staging }}' and table_name = t.name and column_name = '_batch_id'
                ) then
                    raise exception 'gold table %.% has no _batch_id column', '{{ staging }}', t.name;
                end if;
                -- Staging must contain exactly the audited batch, nothing stale.
                execute format('select count(*) from %I.%I where _batch_id is distinct from %L',
                               '{{ staging }}', t.name, '{{ batch_id }}') into stray;
                if stray > 0 then
                    raise exception '%.% has % rows from another batch than {{ batch_id }}',
                                    '{{ staging }}', t.name, stray;
                end if;

                execute format('create table if not exists %I.%I (like %I.%I)',
                               '{{ live }}', t.name, '{{ staging }}', t.name);
                -- Schema evolution: columns added to a gold model are added to its history.
                for col in
                    select a.attname, format_type(a.atttypid, a.atttypmod) as type
                    from pg_attribute a
                    join pg_class cl on cl.oid = a.attrelid
                    join pg_namespace n on n.oid = cl.relnamespace
                    where n.nspname = '{{ staging }}' and cl.relname = t.name and a.attnum > 0 and not a.attisdropped
                      and a.attname not in (
                          select column_name from information_schema.columns
                          where table_schema = '{{ live }}' and table_name = t.name
                      )
                loop
                    execute format('alter table %I.%I add column %I %s', '{{ live }}', t.name, col.attname, col.type);
                end loop;

                select string_agg(format('%I', column_name), ', ' order by ordinal_position) into cols
                from information_schema.columns
                where table_schema = '{{ staging }}' and table_name = t.name;

                execute format('delete from %I.%I where _batch_id = %L', '{{ live }}', t.name, '{{ batch_id }}');
                execute format('insert into %I.%I (%s) select %s from %I.%I',
                               '{{ live }}', t.name, cols, cols, '{{ staging }}', t.name);
            end loop;

            if exists (select 1 from pg_roles where rolname = '{{ reader }}') then
                grant usage on schema {{ live }} to {{ reader }};
                grant select on all tables in schema {{ live }} to {{ reader }};
            end if;
        end $$;
    {% endset %}
    {% do run_query(sql) %}
    {{ log("published batch " ~ batch_id ~ " from " ~ staging ~ " into " ~ live, info=true) }}
{% endmacro %}

{#- Unpublish one batch: removes its partition from every <domain>_gold table. -#}
{% macro rollback_gold(domain, batch_id) %}
    {{ _validate_publish_args(domain, batch_id) }}
    {%- set live = domain ~ '_gold' -%}
    {% set sql %}
        do $$
        declare t record;
        begin
            for t in
                select c.relname as name
                from pg_class c join pg_namespace n on n.oid = c.relnamespace
                where n.nspname = '{{ live }}' and c.relkind = 'r'
            loop
                execute format('delete from %I.%I where _batch_id = %L', '{{ live }}', t.name, '{{ batch_id }}');
            end loop;
        end $$;
    {% endset %}
    {% do run_query(sql) %}
    {{ log("removed batch " ~ batch_id ~ " from " ~ live, info=true) }}
{% endmacro %}
