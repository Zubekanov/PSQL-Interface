import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


def _install_psycopg2_stub() -> None:
    class Composable:
        def as_string(self, conn=None) -> str:
            raise NotImplementedError

    class SQL(Composable):
        def __init__(self, text: str):
            self.text = str(text)

        def as_string(self, conn=None) -> str:
            return self.text

        def format(self, *args, **kwargs):
            def _to_sql(value):
                return value.as_string(None) if isinstance(value, Composable) else str(value)

            arg_values = [_to_sql(a) for a in args]
            kw_values = {k: _to_sql(v) for k, v in kwargs.items()}
            return SQL(self.text.format(*arg_values, **kw_values))

        def join(self, seq):
            parts = [p.as_string(None) if isinstance(p, Composable) else str(p) for p in seq]
            return SQL(self.text.join(parts))

    class Identifier(Composable):
        def __init__(self, *parts):
            self.parts = tuple(str(p) for p in parts)

        def as_string(self, conn=None) -> str:
            return ".".join(f'"{p}"' for p in self.parts)

    class Placeholder(Composable):
        def as_string(self, conn=None) -> str:
            return "%s"

    class ThreadedConnectionPool:
        def __init__(self, minconn, maxconn, **kwargs):
            self.minconn = minconn
            self.maxconn = maxconn
            self.kwargs = dict(kwargs)

        def getconn(self):
            return None

        def putconn(self, conn):
            return None

        def closeall(self):
            return None

    psycopg2_module = types.ModuleType("psycopg2")
    sql_module = types.ModuleType("psycopg2.sql")
    pool_module = types.ModuleType("psycopg2.pool")

    sql_module.Composable = Composable
    sql_module.SQL = SQL
    sql_module.Identifier = Identifier
    sql_module.Placeholder = Placeholder
    pool_module.ThreadedConnectionPool = ThreadedConnectionPool

    psycopg2_module.sql = sql_module
    psycopg2_module.pool = pool_module

    sys.modules["psycopg2"] = psycopg2_module
    sys.modules["psycopg2.sql"] = sql_module
    sys.modules["psycopg2.pool"] = pool_module


_install_psycopg2_stub()

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.table_enforcer import TableEnforcer  # noqa: E402


def _basic_table_config(name: str) -> dict:
    return {
        "table_name": name,
        "columns": [
            {
                "name": "id",
                "type": "integer",
                "primary_key": True,
                "nullable": False,
            }
        ],
    }


def _column_info(
    *,
    udt_name: str,
    data_type: str,
    is_nullable: str,
    column_default=None,
    character_maximum_length=None,
    numeric_precision=None,
    numeric_scale=None,
) -> dict:
    return {
        "udt_name": udt_name,
        "data_type": data_type,
        "is_nullable": is_nullable,
        "column_default": column_default,
        "character_maximum_length": character_maximum_length,
        "numeric_precision": numeric_precision,
        "numeric_scale": numeric_scale,
    }


class TestTableEnforcer(unittest.TestCase):
    def _make_client(self):
        client = mock.Mock()
        client.ensure_schema.return_value = None
        client.table_exists.return_value = False
        client.list_indexes.return_value = []
        client.list_tables.return_value = []
        client.get_column_info.return_value = {}
        client.constraint_exists.return_value = False
        client.list_constraints.return_value = []
        client.list_constraint_indexes.return_value = []
        client.get_constraint_columns.return_value = []
        client.delete_rows_with_filters.return_value = 0
        return client

    def test_enforce_from_directory_with_table_filter(self):
        client = self._make_client()
        enforcer = TableEnforcer(client)

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            (tmp_path / "users.json").write_text(json.dumps(_basic_table_config("users")), encoding="utf-8")
            (tmp_path / "sessions.json").write_text(json.dumps(_basic_table_config("sessions")), encoding="utf-8")

            configured = enforcer.enforce(table="users", config_dir=tmp_path, safe_mode=True)

        self.assertEqual(configured, {("public", "users")})
        client.create_table.assert_called_once()
        create_args, create_kwargs = client.create_table.call_args
        self.assertEqual(create_args[0], "public")
        self.assertEqual(create_args[1], "users")
        self.assertEqual(create_kwargs.get("if_not_exists"), True)

    def test_enforce_from_file_list_and_drop_unconfigured_tables(self):
        client = self._make_client()
        client.table_exists.return_value = True
        client.get_column_info.return_value = {
            "id": _column_info(udt_name="int4", data_type="integer", is_nullable="NO"),
        }
        client.list_tables.return_value = ["users", "stale"]

        enforcer = TableEnforcer(client)
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "users.json"
            cfg_path.write_text(json.dumps(_basic_table_config("users")), encoding="utf-8")
            configured = enforcer.enforce(
                config_files=[cfg_path],
                safe_mode=True,
                drop_tables_not_in_config=True,
            )

        self.assertEqual(configured, {("public", "users")})
        client.create_table.assert_not_called()
        client.drop_table.assert_called_once_with("public", "stale", cascade=True, missing_ok=True)

    def test_forceful_mode_drops_extras_and_rebuilds_pk(self):
        client = self._make_client()
        client.table_exists.return_value = True
        client.get_column_info.return_value = {
            "id": _column_info(udt_name="int4", data_type="integer", is_nullable="NO"),
            "legacy": _column_info(udt_name="text", data_type="text", is_nullable="YES"),
        }
        client.list_constraints.return_value = []
        client.constraint_exists.return_value = False
        client.list_indexes.return_value = [{"indexname": "legacy_idx", "indexdef": "..."}]
        client.list_constraint_indexes.return_value = []

        enforcer = TableEnforcer(client)
        configured = enforcer.enforce(
            config_payloads=[_basic_table_config("users")],
            safe_mode=False,
        )

        self.assertEqual(configured, {("public", "users")})
        client.drop_column.assert_called_once_with("public", "users", "legacy", cascade=True, missing_ok=True)
        client.add_constraint.assert_called_with("public", "users", 'CONSTRAINT "users_pkey" PRIMARY KEY ("id")')
        client.drop_index.assert_called_once_with("public", "legacy_idx", missing_ok=True)

    def test_enforce_plan_only_returns_actions_and_skips_mutations(self):
        client = self._make_client()
        enforcer = TableEnforcer(client)

        out = enforcer.enforce(
            config_payloads=[_basic_table_config("users")],
            safe_mode=True,
            plan_only=True,
        )

        configured, actions = out
        self.assertEqual(configured, {("public", "users")})
        self.assertTrue(any("CREATE SCHEMA IF NOT EXISTS public" in action for action in actions))
        self.assertTrue(any("CREATE TABLE public.users" in action for action in actions))
        client.ensure_schema.assert_not_called()
        client.create_table.assert_not_called()

    def test_validate_config_rejects_invalid_payloads(self):
        client = self._make_client()
        enforcer = TableEnforcer(client)

        with self.assertRaisesRegex(ValueError, "non-empty 'columns'"):
            enforcer.validate_config({"table_name": "users", "columns": []})

        bad_index = {
            "table_name": "users",
            "columns": [{"name": "id", "type": "integer"}],
            "indexes": [{"name": "users_bad_idx", "columns": ["missing_col"]}],
        }
        with self.assertRaisesRegex(ValueError, "unknown columns"):
            enforcer.validate_config(bad_index)

        with self.assertRaisesRegex(ValueError, "duplicate column"):
            enforcer.validate_config(
                {
                    "table_name": "users",
                    "columns": [
                        {"name": "id", "type": "integer"},
                        {"name": "id", "type": "integer"},
                    ],
                }
            )

    def test_enforce_requires_source(self):
        client = self._make_client()
        enforcer = TableEnforcer(client)

        with self.assertRaisesRegex(ValueError, "At least one config source"):
            enforcer.enforce()

    def test_cleanup_nulls_uses_delete_rows_with_filters(self):
        client = self._make_client()
        client.delete_rows_with_filters.return_value = 0
        enforcer = TableEnforcer(client)

        enforcer._cleanup_nulls("public", "users", "email")

        client.delete_rows_with_filters.assert_called_once_with(
            "public.users",
            raw_conditions='"email" IS NULL',
        )

    def test_cleanup_nulls_logs_when_rows_deleted(self):
        client = self._make_client()
        client.delete_rows_with_filters.return_value = 3
        enforcer = TableEnforcer(client)

        with self.assertLogs("src.table_enforcer", level="WARNING") as logs:
            enforcer._cleanup_nulls("public", "users", "email")

        self.assertTrue(any("Deleted 3 rows with NULL public.users.email" in msg for msg in logs.output))

    def test_cleanup_nulls_plan_only_records_action(self):
        client = self._make_client()
        enforcer = TableEnforcer(client)
        enforcer._plan_only = True

        enforcer._cleanup_nulls("public", "users", "email")

        self.assertEqual(enforcer.last_plan, ['DELETE FROM public.users WHERE "email" IS NULL'])
        client.delete_rows_with_filters.assert_not_called()


if __name__ == "__main__":
    unittest.main()
