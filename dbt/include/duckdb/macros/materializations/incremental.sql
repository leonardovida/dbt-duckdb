{% materialization incremental, adapter="duckdb", supported_languages=['sql', 'python'] -%}

  {%- set language = model['language'] -%}
  -- only create temp tables if using local duckdb, as it is not currently supported for remote databases
  {%- set temporary = not adapter.is_motherduck() -%}

  -- relations
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set temp_relation = make_temp_relation(target_relation)-%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation)-%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}

  -- configs
  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (should_full_refresh()  or existing_relation.is_view) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set partitioned_by = none -%}
  {%- if existing_relation is none or full_refresh_mode -%}
    {%- set partitioned_by = duckdb__get_partitioned_by(target_relation, false) -%}
  {%- endif -%}
  {%- set sorted_by = duckdb__get_sorted_by(target_relation, false) -%}
  {%- set sorted_by_statement = none -%}
  {%- if sorted_by or (duckdb__has_sorted_by_config() and adapter.is_ducklake(target_relation)) -%}
    {%- set sorted_by_statement = duckdb__get_sorted_by_statement(target_relation, false) -%}
  {%- endif -%}
  {%- set skip_auto_begin = partitioned_by and adapter.is_ducklake(target_relation) -%}

  -- the temp_ and backup_ relations should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation. This has to happen before
  -- BEGIN, in a separate transaction
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
   -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {% set to_drop = [] %}
  {% if not temporary %}
    -- if not using a temporary table we will update the temp relation to use a different temp schema ("dbt_temp" by default)
    -- for microbatch with concurrent batches, include batch timestamps in the identifier to avoid collisions
    {%- set batch_id = '' -%}
    {%- set batch_ctx = model.get('batch') -%}
    {%- if batch_ctx and batch_ctx.get('event_time_start') -%}
        {%- set batch_id = batch_ctx.get('event_time_start') | string | replace('-', '') | replace(':', '') | replace(' ', '_') | replace('+', '') -%}
    {%- endif -%}
    {% set temp_relation = temp_relation.incorporate(path=adapter.get_temp_relation_path(this, batch_id)) %}
    {% do run_query(create_schema(temp_relation)) %}
    {% if not adapter.disable_transactions() %}
      {% do adapter.commit() %}
    {% endif %}
    -- then drop the temp relation after we insert the incremental data into the target relation
    {% do to_drop.append(temp_relation) %}
  {% endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if sorted_by and language != 'sql' and (existing_relation is none or full_refresh_mode) %}
    {% do exceptions.raise_compiler_error("DuckLake `sorted_by` is currently supported only for SQL models during initial and full-refresh builds") %}
  {% endif %}

  {% if existing_relation is none %}
    {% if sorted_by %}
      {% set build_sql = create_empty_table_as(False, target_relation, compiled_code, language) %}
      {% set build_relation = target_relation %}
    {% else %}
      {% set build_sql = create_table_as(False, target_relation, compiled_code, language, partitioned_by=partitioned_by) %}
    {% endif %}
  {% elif full_refresh_mode %}
    {% if sorted_by %}
      {% set build_sql = create_empty_table_as(False, intermediate_relation, compiled_code, language) %}
      {% set build_relation = intermediate_relation %}
    {% else %}
      {% set build_sql = create_table_as(False, intermediate_relation, compiled_code, language, partitioned_by=partitioned_by) %}
    {% endif %}
    {% set need_swap = true %}
  {% else %}
    {% if sorted_by_statement %}
      {% call statement('ducklake_sorted_by_target') -%}
        {{ sorted_by_statement }};
      {%- endcall %}
    {% endif %}
    {% if language == 'python' %}
      {% set build_python = create_table_as(temporary, temp_relation, compiled_code, language, partitioned_by=none) %}
      {% call statement("pre", language=language) %}
        {{- build_python }}
      {% endcall %}
    {% else %} {# SQL #}
      {% do run_query(create_table_as(temporary, temp_relation, compiled_code, language, partitioned_by=none)) %}
    {% endif %}
    {% do adapter.expand_target_column_types(
             from_relation=temp_relation,
             to_relation=target_relation) %}
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}

    {#-- Get the incremental_strategy, the macro to use for the strategy, and build the sql --#}
    {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}
    {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
    {% set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) %}
    {% set strategy_arg_dict = ({'target_relation': target_relation, 'temp_relation': temp_relation, 'unique_key': unique_key, 'dest_columns': dest_columns, 'incremental_predicates': incremental_predicates }) %}
    {% set build_sql = strategy_sql_macro_func(strategy_arg_dict) %}
    {% set language = "sql" %}

  {% endif %}

  {% if sorted_by and (existing_relation is none or full_refresh_mode) %}
    {% call statement("main", language=language) %}
        {{- build_sql }}
    {% endcall %}
    {% if partitioned_by %}
      {% call statement('ducklake_partitioned_by') -%}
        {{ duckdb__alter_table_set_partitioned_by(build_relation, partitioned_by) }}
      {%- endcall %}
    {% endif %}
    {% set build_sorted_by_statement = duckdb__get_sorted_by_statement(build_relation, false) %}
    {% if build_sorted_by_statement %}
      {% call statement('ducklake_sorted_by_build') -%}
        {{ build_sorted_by_statement }};
      {%- endcall %}
    {% endif %}
    {% call statement('ducklake_insert', language=language) -%}
      {{- insert_into_table(build_relation, compiled_code, language) }}
    {%- endcall %}
  {% else %}
    {% call statement("main", language=language, auto_begin=not skip_auto_begin) %}
        {{- build_sql }}
    {% endcall %}
  {% endif %}

  {% if need_swap %}
      {#-- Drop indexes on target relation before renaming to backup to avoid dependency errors --#}
      {% do drop_indexes_on_relation(target_relation) %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% do adapter.rename_relation(intermediate_relation, target_relation) %}
      {% do to_drop.append(backup_relation) %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {# Align order with table materialization to avoid MotherDuck alter conflicts #}
  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% if sorted_by %}
    {% do ducklake_flush_relation(target_relation) %}
  {% endif %}

  {% for rel in to_drop %}
      {# On MotherDuck the temp relation is a real table; dropping it cascades indexes. Avoid extra ALTERs. #}
      {% if not adapter.is_motherduck() %}
        {% do drop_indexes_on_relation(rel) %}
      {% endif %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
