import unittest
from argparse import Namespace
from unittest import mock

from dbt.flags import set_from_args
from dbt_common.exceptions import DbtRuntimeError
from packaging.version import Version

from dbt.adapters.duckdb import DuckDBAdapter
from dbt.adapters.duckdb.relation import DuckDBRelation
from tests.unit.utils import config_from_parts_or_dicts


class TestDuckDBAdapterDuckLakeSortedBy(unittest.TestCase):
    def setUp(self):
        set_from_args(Namespace(STRICT_MODE=True), {})

        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "duckdb",
                    "path": ":memory:",
                }
            },
            "target": "test",
        }

        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "config-version": 2,
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg, cli_vars={})
        self.adapter = DuckDBAdapter(self.config, mock.MagicMock())
        self.relation = DuckDBRelation.create(database="ducklake_db", schema="main", identifier="events")
        self.adapter.config.credentials._ducklake_dbs.add("ducklake_db")
        self.adapter.config.credentials._motherduck_dbs.add("ducklake_db")

    def test_sorted_by_list_renders_set_statement(self):
        sql = self.adapter.ducklake_table_option_sql(
            self.relation,
            "sorted_by",
            ["event_time ASC", "event_type DESC NULLS FIRST"],
        )
        self.assertEqual(
            f"alter table {self.relation} set SORTED BY (event_time ASC, event_type DESC NULLS FIRST)",
            sql,
        )

    def test_ducklake_table_option_preserves_parenthesized_sql(self):
        sql = self.adapter.ducklake_table_option_sql(
            self.relation,
            "sorted_by",
            "(date_trunc('hour', event_time) ASC)",
        )
        self.assertEqual(
            f"alter table {self.relation} set SORTED BY (date_trunc('hour', event_time) ASC)",
            sql,
        )

    def test_ducklake_table_option_empty_value_resets(self):
        sorted_sql = self.adapter.ducklake_table_option_sql(self.relation, "sorted_by", [])

        self.assertEqual(f"alter table {self.relation} reset SORTED BY", sorted_sql)

    def test_ducklake_order_by_sql_normalizes_parentheses(self):
        self.assertEqual(
            "event_time ASC, event_type DESC NULLS FIRST",
            self.adapter.ducklake_order_by_sql("(event_time ASC, event_type DESC NULLS FIRST)"),
        )

    def test_ducklake_table_option_rejects_invalid_config_types(self):
        with self.assertRaisesRegex(DbtRuntimeError, "`sorted_by` must be a string or a list of SQL strings"):
            self.adapter.ducklake_table_option_sql(self.relation, "sorted_by", {"col": "event_time"})

    @mock.patch.object(DuckDBAdapter, "duckdb_version", new_callable=mock.PropertyMock, return_value=Version("1.4.4"))
    def test_sorted_by_requires_newer_local_duckdb(self, _version):
        self.adapter.config.credentials._motherduck_dbs.discard("ducklake_db")
        with self.assertRaisesRegex(DbtRuntimeError, "`sorted_by` requires MotherDuck DuckLake or local DuckDB >= 1.5.1"):
            self.adapter.ducklake_table_option_sql(self.relation, "sorted_by", ["event_time ASC"])

    @mock.patch.object(DuckDBAdapter, "duckdb_version", new_callable=mock.PropertyMock, return_value=Version("1.4.4"))
    def test_sorted_by_allowed_for_motherduck_relations(self, _version):
        self.adapter.config.credentials._motherduck_dbs.add("ducklake_db")
        sql = self.adapter.ducklake_table_option_sql(self.relation, "sorted_by", ["event_time ASC"])
        self.assertEqual(f"alter table {self.relation} set SORTED BY (event_time ASC)", sql)
