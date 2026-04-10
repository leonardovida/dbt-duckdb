import re
from pathlib import Path

import pytest
from dbt.tests.util import run_dbt


models__sorted_by_table = """
{{ config(materialized='view', sorted_by=['event_time ASC', 'event_type DESC NULLS FIRST']) }}

{{ render_sorted_by_sql() }}
"""

models__sorted_by_reset = """
{{ config(materialized='view', sorted_by=[]) }}

{{ render_sorted_by_sql() }}
"""

models__invalid_sorted_by = """
{{ config(materialized='view', sorted_by={'event_time': 'ASC'}) }}

{{ render_sorted_by_sql() }}
"""

macros__render_sorted_by_sql = """
{% macro render_sorted_by_sql(compiled_code='select 1 as event_time, 2 as event_type', temporary=false, language='sql') %}
  {%- set sorted_by_statement = duckdb__get_sorted_by_statement(this, temporary) -%}
  {{ create_empty_table_as(temporary, this, compiled_code, language) }}
  {% if sorted_by_statement %}
    {{ sorted_by_statement }};
  {% endif %}
  {{ insert_into_table(this, compiled_code, language) }}
{% endmacro %}
"""


def read_compiled_file(project, model_name, extension):
    compiled_root = Path(project.project_root) / "target" / "compiled"
    matches = list(compiled_root.rglob(f"{model_name}.{extension}"))
    assert matches, f"Compiled file not found for model '{model_name}.{extension}'"

    project_name = getattr(project, "project_name", None)
    if project_name:
        named_matches = [path for path in matches if project_name in path.parts]
        if named_matches:
            matches = named_matches

    assert len(matches) == 1, f"Expected one compiled file for '{model_name}', found {len(matches)}"
    return matches[0].read_text()


class BaseSortedByCompile:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "ducklake_sorted_by",
            "macro-paths": ["macros"],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sorted_by_table.sql": models__sorted_by_table,
            "sorted_by_reset.sql": models__sorted_by_reset,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "render_sorted_by_sql.sql": macros__render_sorted_by_sql,
        }


class TestDucklakeSortedByCompile(BaseSortedByCompile):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        target = dict(dbt_profile_target)
        target["is_ducklake"] = True
        return target

    def test_sorted_by_emits_set_and_order_by(self, project):
        run_dbt(["compile"])
        sql = read_compiled_file(project, "sorted_by_table", "sql")
        assert re.search(
            r"set\s+sorted\s+by\s*\(\s*event_time\s+ASC\s*,\s*event_type\s+DESC\s+NULLS\s+FIRST\s*\)",
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        assert re.search(
            r"order\s+by\s+event_time\s+ASC\s*,\s*event_type\s+DESC\s+NULLS\s+FIRST",
            sql,
            re.IGNORECASE | re.DOTALL,
        )

    def test_sorted_by_empty_list_emits_reset(self, project):
        run_dbt(["compile"])
        sql = read_compiled_file(project, "sorted_by_reset", "sql")
        assert re.search(r"reset\s+SORTED\s+BY", sql, re.IGNORECASE | re.DOTALL)


class TestNonDucklakeSortedByCompile(BaseSortedByCompile):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        target = dict(dbt_profile_target)
        target.pop("is_ducklake", None)
        return target

    def test_sorted_by_is_ignored(self, project):
        run_dbt(["compile"])
        sql = read_compiled_file(project, "sorted_by_table", "sql").lower()
        assert "set sorted by" not in sql
        assert "order by event_time asc" not in sql


class TestSortedByValidation:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "ducklake_sorted_by_validation",
            "macro-paths": ["macros"],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid_sorted_by.sql": models__invalid_sorted_by,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "render_sorted_by_sql.sql": macros__render_sorted_by_sql,
        }

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        target = dict(dbt_profile_target)
        target["is_ducklake"] = True
        return target

    def test_sorted_by_values_must_be_strings_or_lists(self, project):
        with pytest.raises(Exception, match="`sorted_by` must be a string or a list of SQL strings"):
            run_dbt(["compile"])
