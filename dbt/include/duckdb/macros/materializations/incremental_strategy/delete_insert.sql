{% macro duckdb__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set order_by_sql = ducklake_sorted_order_by_sql(target) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not string %}
            delete from {{target }} as DBT_INCREMENTAL_TARGET
            using {{ source }}
            where (
                {% for key in unique_key %}
                    {{ source }}.{{ key }} = DBT_INCREMENTAL_TARGET.{{ key }}
                    {{ "and " if not loop.last}}
                {% endfor %}
                {% if incremental_predicates %}
                    {% for predicate in incremental_predicates %}
                        and {{ predicate }}
                    {% endfor %}
                {% endif %}
            );
        {% else %}
            delete from {{ target }}
            where (
                {{ unique_key }}) in (
                select ({{ unique_key }})
                from {{ source }}
            )
            {%- if incremental_predicates %}
                {% for predicate in incremental_predicates %}
                    and {{ predicate }}
                {% endfor %}
            {%- endif -%};

        {% endif %}
    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
        {% if order_by_sql %}
          order by {{ order_by_sql }}
        {% endif %}
    )

{%- endmacro %}
