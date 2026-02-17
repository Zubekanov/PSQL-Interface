import sys
import types
import unittest
from pathlib import Path
from threading import RLock
from unittest import mock


def _install_psycopg2_stub() -> None:
    """Provide a tiny psycopg2/sql surface so tests run without external deps."""

    def _quote_ident(part: str) -> str:
        return '"' + str(part).replace('"', '""') + '"'

    class Composable:
        def as_string(self, conn=None) -> str:
            raise NotImplementedError

        def __add__(self, other):
            return SQL(self.as_string(None) + _to_sql(other))

        def __radd__(self, other):
            return SQL(_to_sql(other) + self.as_string(None))

    class SQL(Composable):
        def __init__(self, text: str):
            self.text = str(text)

        def as_string(self, conn=None) -> str:
            return self.text

        def format(self, *args, **kwargs):
            arg_values = [_to_sql(a) for a in args]
            kw_values = {k: _to_sql(v) for k, v in kwargs.items()}
            return SQL(self.text.format(*arg_values, **kw_values))

        def join(self, seq):
            return SQL(self.text.join(_to_sql(v) for v in seq))

    class Composed(SQL):
        pass

    class Identifier(Composable):
        def __init__(self, *parts):
            self.parts = tuple(str(p) for p in parts)

        def as_string(self, conn=None) -> str:
            return ".".join(_quote_ident(p) for p in self.parts)

    class Placeholder(Composable):
        def as_string(self, conn=None) -> str:
            return "%s"

    class _NoopCursor:
        description = None

        def execute(self, query, params):
            return None

        def fetchall(self):
            return []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _NoopConnection:
        def __init__(self):
            self.autocommit = False

        def cursor(self):
            return _NoopCursor()

        def commit(self):
            return None

        def rollback(self):
            return None

    class ThreadedConnectionPool:
        def __init__(self, minconn, maxconn, **kwargs):
            self.minconn = minconn
            self.maxconn = maxconn
            self.kwargs = dict(kwargs)
            self.closed = False
            self._conn = _NoopConnection()

        def getconn(self):
            return self._conn

        def putconn(self, conn):
            return None

        def closeall(self):
            self.closed = True

    def _to_sql(value) -> str:
        if isinstance(value, Composable):
            return value.as_string(None)
        return str(value)

    psycopg2_module = types.ModuleType("psycopg2")
    sql_module = types.ModuleType("psycopg2.sql")
    pool_module = types.ModuleType("psycopg2.pool")

    sql_module.Composable = Composable
    sql_module.Composed = Composed
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

from src.psql_client import JoinSpec, PSQLClient, RawCondition, sql  # noqa: E402


class FakeCursor:
    def __init__(self, *, description=None, rows=None, raise_on_execute=None):
        self.description = description
        self._rows = list(rows or [])
        self.raise_on_execute = raise_on_execute
        self.executed = []

    def execute(self, query, params):
        self.executed.append((query, list(params)))
        if self.raise_on_execute is not None:
            raise self.raise_on_execute

    def fetchall(self):
        return list(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    def __init__(self, cursor=None):
        self.autocommit = False
        self._cursor = cursor or FakeCursor()
        self.commit_calls = 0
        self.rollback_calls = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commit_calls += 1

    def rollback(self):
        self.rollback_calls += 1


class RecordingPool:
    instances = []

    def __init__(self, minconn, maxconn, **kwargs):
        self.minconn = minconn
        self.maxconn = maxconn
        self.kwargs = dict(kwargs)
        self.closeall_calls = 0
        self.putconn_calls = []
        self.raise_on_put = None
        self._conn = FakeConnection()
        self.__class__.instances.append(self)

    def getconn(self):
        return self._conn

    def putconn(self, conn):
        self.putconn_calls.append(conn)
        if self.raise_on_put is not None:
            raise self.raise_on_put

    def closeall(self):
        self.closeall_calls += 1


def make_client(pool=None) -> PSQLClient:
    client = object.__new__(PSQLClient)
    client.database = "postgres"
    client.user = "postgres"
    client.host = None
    client.port = None
    client._closed = False
    client._state_lock = RLock()
    client._cache_key = None
    client._conn_kwargs = {}
    client.pool = pool or mock.Mock(minconn=1, maxconn=10)
    return client


def render(query) -> str:
    if isinstance(query, str):
        return query
    return query.as_string(None)


class PSQLClientTestCase(unittest.TestCase):
    def tearDown(self):
        PSQLClient.closeall()
        RecordingPool.instances.clear()


class TestDataStructures(PSQLClientTestCase):
    def test_joinspec_and_rawcondition(self):
        spec = JoinSpec("LEFT JOIN", "public.roles r", "r.id = users.role_id")
        self.assertEqual(spec.join_type, "LEFT JOIN")
        cond = RawCondition("users.active = %s")
        self.assertEqual(cond.params, ())


class TestCacheAndLifecycle(PSQLClientTestCase):
    def test_freeze_conn_kwargs(self):
        frozen = PSQLClient._freeze_conn_kwargs({"b": 2, "a": [1, 2]})
        self.assertEqual(frozen, (("a", "[1, 2]"), ("b", 2)))
        self.assertIsNone(PSQLClient._freeze_conn_kwargs({}))

    def test_cache_reuse_close_and_closeall(self):
        with mock.patch("src.psql_client.ThreadedConnectionPool", RecordingPool):
            first = PSQLClient.get(database="db", user="u")
            second = PSQLClient.get(database="db", user="u")
            self.assertIs(first, second)

            cache_key = first._cache_key
            first.close()
            first.close()
            self.assertNotIn(cache_key, PSQLClient._cache)
            self.assertEqual(first.pool.closeall_calls, 1)

            third = PSQLClient.get(database="db", user="u")
            self.assertIsNot(first, third)
            PSQLClient.closeall()
            self.assertEqual(PSQLClient._cache, {})

    def test_repr_get_conn_and_put_conn(self):
        with mock.patch("src.psql_client.ThreadedConnectionPool", RecordingPool):
            client = PSQLClient.get(database="mydb", user="alice", host="db.local", port=5432, minconn=2, maxconn=8)
        self.assertEqual(repr(client), '<PSQLClient alice@db.local:5432/mydb pool=2-8>')

        closed_client = make_client(pool=RecordingPool(1, 2))
        closed_client._closed = True
        with self.assertRaisesRegex(RuntimeError, "closed"):
            closed_client._get_conn()

        pool = RecordingPool(1, 2)
        pool.raise_on_put = RuntimeError("put failed")
        client2 = make_client(pool=pool)
        with self.assertRaisesRegex(RuntimeError, "put failed"):
            client2._put_conn(object())

        client2._closed = True
        client2._put_conn(object())

class TestExecutionInternals(PSQLClientTestCase):
    def test_normalize_query_and_rows_from_cursor(self):
        client = make_client()
        self.assertEqual(client._normalize_query(None, "SELECT 1"), "SELECT 1")
        self.assertEqual(client._normalize_query(None, sql.SQL("SELECT 2")), "SELECT 2")

        cur_none = FakeCursor(description=None, rows=[(1,)])
        self.assertIsNone(PSQLClient._rows_from_cursor(cur_none))

        cur_rows = FakeCursor(description=[("id",), ("name",)], rows=[(1, "A"), (2, "B")])
        self.assertEqual(
            PSQLClient._rows_from_cursor(cur_rows),
            [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}],
        )

    def test_execute_on_conn_commit_rollback_autocommit(self):
        client = make_client()

        cur_ok = FakeCursor(description=[("id",)], rows=[(1,)])
        conn_ok = FakeConnection(cur_ok)
        out = client._execute_on_conn(conn_ok, "SELECT 1", [7], autocommit=False)
        self.assertEqual(out, [{"id": 1}])
        self.assertEqual(conn_ok.commit_calls, 1)
        self.assertEqual(conn_ok.rollback_calls, 0)

        cur_fail = FakeCursor(raise_on_execute=RuntimeError("boom"))
        conn_fail = FakeConnection(cur_fail)
        with self.assertRaisesRegex(RuntimeError, "boom"):
            client._execute_on_conn(conn_fail, "SELECT 1", [7], autocommit=False)
        self.assertEqual(conn_fail.commit_calls, 0)
        self.assertEqual(conn_fail.rollback_calls, 1)

        cur_auto = FakeCursor(description=None)
        conn_auto = FakeConnection(cur_auto)
        conn_auto.autocommit = False
        out2 = client._execute_on_conn(conn_auto, "VACUUM", autocommit=True)
        self.assertIsNone(out2)
        self.assertFalse(conn_auto.autocommit)

    def test_execute_execute_query_and_execute_autocommit(self):
        conn = FakeConnection()
        pool = mock.Mock()
        pool.getconn.return_value = conn
        client = make_client(pool=pool)

        with mock.patch.object(client, "_execute_on_conn", return_value=[{"ok": 1}]) as exec_on_conn:
            rows = client._execute("SELECT 1", [1])
        self.assertEqual(rows, [{"ok": 1}])
        exec_on_conn.assert_called_once_with(conn, "SELECT 1", [1], autocommit=False)
        pool.putconn.assert_called_once_with(conn)

        with mock.patch.object(client, "_execute", return_value=[{"id": 1}]) as execute_mock:
            out = client.execute_query("SELECT * FROM t", [1])
        self.assertEqual(out, [{"id": 1}])
        execute_mock.assert_called_once_with("SELECT * FROM t", [1])

        pool.reset_mock()
        pool.getconn.return_value = conn
        with mock.patch.object(client, "_execute_on_conn", return_value=None) as exec_on_conn2:
            client._execute_autocommit("CREATE DATABASE demo")
        exec_on_conn2.assert_called_once_with(conn, "CREATE DATABASE demo", None, autocommit=True)
        pool.putconn.assert_called_once_with(conn)

    def test_transaction_commit_and_rollback(self):
        conn = FakeConnection()
        pool = mock.Mock()
        pool.getconn.return_value = conn
        client = make_client(pool=pool)

        with client.transaction() as tx:
            tx.execute("SELECT 1", [7])
        self.assertEqual(conn.commit_calls, 1)
        self.assertEqual(conn.rollback_calls, 0)
        pool.putconn.assert_called_once_with(conn)

        conn2 = FakeConnection()
        pool2 = mock.Mock()
        pool2.getconn.return_value = conn2
        client2 = make_client(pool=pool2)

        with self.assertRaisesRegex(RuntimeError, "boom"):
            with client2.transaction() as tx2:
                tx2.execute("SELECT 1")
                raise RuntimeError("boom")
        self.assertEqual(conn2.commit_calls, 0)
        self.assertEqual(conn2.rollback_calls, 1)
        pool2.putconn.assert_called_once_with(conn2)


class TestDDLAndMetadataHelpers(PSQLClientTestCase):
    def test_boolean_existence_helpers(self):
        client = make_client()
        cases = [
            ("database_exists", ("db",)),
            ("schema_exists", ("public",)),
            ("table_exists", ("public", "users")),
            ("column_exists", ("public", "users", "id")),
            ("index_exists", ("public", "users_idx")),
            ("constraint_exists", ("public", "users", "users_pk")),
        ]
        for method_name, args in cases:
            method = getattr(client, method_name)
            with self.subTest(method=method_name, state="true"):
                with mock.patch.object(client, "_execute", return_value=[{"x": 1}]):
                    self.assertTrue(method(*args))
            with self.subTest(method=method_name, state="false"):
                with mock.patch.object(client, "_execute", return_value=[]):
                    self.assertFalse(method(*args))

    def test_database_and_schema_helpers(self):
        client = make_client()

        with mock.patch.object(client, "database_exists", return_value=True), mock.patch.object(client, "_execute_autocommit") as execute_auto:
            created = client.create_database("demo", exists_ok=True)
        self.assertFalse(created)
        execute_auto.assert_not_called()

        with mock.patch.object(client, "database_exists", return_value=False), mock.patch.object(client, "_execute_autocommit") as execute_auto2:
            created2 = client.create_database("demo", exists_ok=True)
        self.assertTrue(created2)
        self.assertIn("CREATE DATABASE", render(execute_auto2.call_args.args[0]))

        with mock.patch.object(client, "database_exists", return_value=False):
            with self.assertRaisesRegex(ValueError, "Database not found"):
                client.drop_database("missing", missing_ok=False)

        with mock.patch.object(client, "_execute_autocommit") as execute_auto3:
            dropped = client.drop_database("demo", missing_ok=True)
        self.assertTrue(dropped)
        self.assertIn("DROP DATABASE IF EXISTS", render(execute_auto3.call_args.args[0]))

        rows = [{"schema_name": "app"}, {"schema_name": "public"}]
        with mock.patch.object(client, "_execute", return_value=rows):
            self.assertEqual(client.list_schemas(exclude_system=True), ["app", "public"])

        with mock.patch.object(client, "_execute") as execute_mock:
            client.create_schema("app", exists_ok=True)
            client.drop_schema("app", cascade=True, missing_ok=True)
        self.assertIn("CREATE SCHEMA IF NOT EXISTS", render(execute_mock.call_args_list[0].args[0]))
        self.assertIn("DROP SCHEMA IF EXISTS", render(execute_mock.call_args_list[1].args[0]))
        self.assertIn("CASCADE", render(execute_mock.call_args_list[1].args[0]))

        with mock.patch.object(client, "create_schema") as create_schema:
            client.ensure_schema("app")
        create_schema.assert_called_once_with("app", exists_ok=True)

    def test_table_column_index_constraint_and_metadata_helpers(self):
        client = make_client()

        with mock.patch.object(client, "_execute", return_value=[{"table_name": "users"}]):
            self.assertEqual(client.list_tables("public"), ["users"])

        with mock.patch.object(client, "_execute", return_value=[{"column_name": "id"}, {"column_name": "name"}]):
            self.assertEqual(client._fetch_table_columns("public", "users"), ["id", "name"])

        with mock.patch.object(client, "_fetch_table_columns", return_value=["id", "name"]):
            self.assertEqual(client.get_table_columns("public", "users"), ["id", "name"])
            self.assertEqual(client._get_table_columns("public.users"), ["id", "name"])

        with mock.patch.object(client, "_get_table_columns", return_value=[]):
            with self.assertRaisesRegex(ValueError, "does not exist"):
                client._require_existing_table_columns("users")

        PSQLClient._validate_known_columns(["id", "name"], ["id"], "condition")
        with self.assertRaisesRegex(ValueError, "Invalid columns"):
            PSQLClient._validate_known_columns(["id"], ["missing"], "condition")

        with self.assertRaisesRegex(ValueError, "non-empty dict"):
            client.create_table("public", "users", {})

        with self.assertRaisesRegex(ValueError, "Invalid type"):
            client.create_table("public", "users", {"id": " "})

        with mock.patch.object(client, "_execute") as execute_mock:
            client.create_table("public", "users", {"id": "SERIAL PRIMARY KEY"}, if_not_exists=True, temporary=True)
            client.drop_table("public", "users", cascade=True, missing_ok=True)
        self.assertIn("CREATE TEMP TABLE IF NOT EXISTS", render(execute_mock.call_args_list[0].args[0]))
        self.assertIn("DROP TABLE IF EXISTS", render(execute_mock.call_args_list[1].args[0]))
        self.assertIn("CASCADE", render(execute_mock.call_args_list[1].args[0]))

        with mock.patch.object(client, "create_table") as create_table:
            client.ensure_table("public", "users", {"id": "INT"}, constraints=["PRIMARY KEY (id)"])
        create_table.assert_called_once_with("public", "users", {"id": "INT"}, ["PRIMARY KEY (id)"], if_not_exists=True)

        with self.assertRaisesRegex(ValueError, "non-empty list"):
            client.create_index("public", "users", "idx_users", [])

        with mock.patch.object(client, "_execute") as execute_mock:
            client.create_index("public", "users", "idx_users", ["email"], unique=True, if_not_exists=True)
            client.drop_index("public", "idx_users", missing_ok=True)
            client.drop_column("public", "users", "legacy", cascade=True, missing_ok=True)
            client.alter_column_type("public", "users", "age", "BIGINT", using="age::BIGINT")
            client.alter_column_nullability("public", "users", "email", nullable=False)
            client.alter_column_default("public", "users", "created_at", default_sql="NOW()")
            client.alter_column_default("public", "users", "created_at", drop=True)
            client.add_column("public", "users", "nickname", "TEXT")
            client.drop_constraint("public", "users", "users_email_key", missing_ok=True)
            client.add_constraint("public", "users", "CONSTRAINT users_email_key UNIQUE (email)")

        sql_texts = [render(c.args[0]) for c in execute_mock.call_args_list]
        self.assertIn("CREATE UNIQUE INDEX IF NOT EXISTS", sql_texts[0])
        self.assertIn("DROP INDEX IF EXISTS", sql_texts[1])
        self.assertIn("DROP COLUMN IF EXISTS", sql_texts[2])
        self.assertIn("CASCADE", sql_texts[2])
        self.assertIn("ALTER COLUMN \"age\" TYPE BIGINT USING age::BIGINT", sql_texts[3])
        self.assertIn("SET NOT NULL", sql_texts[4])
        self.assertIn("SET DEFAULT NOW()", sql_texts[5])
        self.assertIn("DROP DEFAULT", sql_texts[6])
        self.assertIn("ADD COLUMN", sql_texts[7])
        self.assertIn("DROP CONSTRAINT IF EXISTS", sql_texts[8])
        self.assertIn("ADD CONSTRAINT users_email_key UNIQUE (email)", sql_texts[9])

        with mock.patch.object(client, "_execute", return_value=[{"indexname": "idx", "indexdef": "..."}]):
            self.assertEqual(client.list_indexes("public", "users"), [{"indexname": "idx", "indexdef": "..."}])

        with mock.patch.object(client, "_execute", return_value=[{"constraint_name": "pk", "constraint_type": "PRIMARY KEY"}]):
            self.assertEqual(client.list_constraints("public", "users"), [{"constraint_name": "pk", "constraint_type": "PRIMARY KEY"}])

        with mock.patch.object(client, "_execute", return_value=[{"column_name": "id"}]):
            self.assertEqual(client.get_primary_key_columns("public", "users"), ["id"])

        with mock.patch.object(client, "_execute", return_value=[{"column_name": "email"}]):
            self.assertEqual(client.get_constraint_columns("public", "users", "users_email_key"), ["email"])

        with mock.patch.object(client, "_execute", return_value=[{"indexname": "users_pkey"}]):
            self.assertEqual(client.list_constraint_indexes("public", "users"), ["users_pkey"])

        rows = [
            {"column_name": "id", "data_type": "integer"},
            {"column_name": "email", "data_type": "text"},
        ]
        with mock.patch.object(client, "_execute", return_value=rows):
            info = client.get_column_info("public", "users")
        self.assertEqual(set(info.keys()), {"id", "email"})
        self.assertEqual(info["email"]["data_type"], "text")


class TestNameAndFilterHelpers(PSQLClientTestCase):
    def test_name_helpers(self):
        client = make_client()
        self.assertEqual(client._split_qualified("public.users"), ("public", "users"))
        self.assertEqual(client._split_qualified("users"), (None, "users"))
        self.assertEqual(client._split_qualified(("public", "users")), ("public", "users"))
        self.assertEqual(client._split_qualified(["users"]), (None, "users"))

        with self.assertRaisesRegex(ValueError, "length 1 or 2"):
            client._split_qualified(("a", "b", "c"))

        self.assertEqual(render(client._ident_qualified("public.users")), '"public"."users"')
        self.assertEqual(render(client._ident_qualified("users")), '"users"')

    def test_join_normalization_and_clause_builders(self):
        client = make_client()
        spec = client._normalize_join_spec(JoinSpec("left join", "public.roles r", "r.id = users.role_id"))
        self.assertEqual(spec.join_type, "LEFT JOIN")

        tuple_spec = client._normalize_join_spec(("left", "public.roles r", "r.id = users.role_id"))
        self.assertEqual(tuple_spec.join_type, "LEFT JOIN")

        string_spec = client._normalize_join_spec("INNER JOIN public.roles r ON r.id = users.role_id")
        self.assertEqual(string_spec.join_type, "INNER JOIN")

        with self.assertRaisesRegex(ValueError, "must have 3 items"):
            client._normalize_join_spec(("JOIN", "table_only"))  # type: ignore[arg-type]

        with self.assertRaisesRegex(ValueError, "Unsupported join entry type"):
            client._normalize_join_spec(123)  # type: ignore[arg-type]

        with self.assertRaisesRegex(ValueError, "String join entries"):
            client._normalize_join_spec("roles r")

        joins = [("LEFT", "public.roles r", "r.id = users.role_id")]
        normalized = client._normalize_joins(joins)
        self.assertEqual(len(normalized), 1)
        self.assertEqual(normalized[0].join_type, "LEFT JOIN")

        select_clause = client._build_select_from_clause("public.users", joins)
        self.assertIn('FROM "public"."users"', render(select_clause))
        self.assertIn("LEFT JOIN public.roles r ON r.id = users.role_id", render(select_clause))

        empty_rel, empty_ons = client._build_relation_clause(None, keyword="USING")
        self.assertEqual(render(empty_rel), "")
        self.assertEqual(empty_ons, [])

        rel, on_parts = client._build_relation_clause(joins, keyword="USING")
        self.assertIn("USING public.roles r", render(rel))
        self.assertEqual([render(p) for p in on_parts], ["r.id = users.role_id"])

    def test_raw_condition_and_where_builders(self):
        with self.assertRaisesRegex(ValueError, "without raw_conditions"):
            PSQLClient._normalize_raw_conditions(None, raw_params=[1])

        parts, params = PSQLClient._normalize_raw_conditions(
            [
                RawCondition("users.active = %s", (True,)),
                "users.age >= %s",
                sql.SQL("users.deleted_at IS NULL"),
            ],
            raw_params=[18],
        )
        self.assertEqual([render(p) for p in parts], ["users.active = %s", "users.age >= %s", "users.deleted_at IS NULL"])
        self.assertEqual(params, [True, 18])

        with self.assertRaisesRegex(ValueError, "empty SQL fragment"):
            PSQLClient._normalize_raw_conditions(["   "])

        with self.assertRaisesRegex(ValueError, "cannot be empty"):
            PSQLClient._normalize_raw_conditions([RawCondition("   ", ())])

        client = make_client()
        where_parts, where_params = client._build_where_parts(
            equalities={"id": 5},
            extra_conditions=[sql.SQL("roles.enabled = TRUE")],
            raw_conditions=RawCondition("users.active = %s", (True,)),
        )
        self.assertEqual([render(p) for p in where_parts], ['"id" = %s', "roles.enabled = TRUE", "users.active = %s"])
        self.assertEqual(where_params, [5, True])

class TestDataAccessFlows(PSQLClientTestCase):
    def test_insert_row(self):
        client = make_client()
        with self.assertRaisesRegex(ValueError, "empty"):
            client.insert_row("public.users", {})

        with mock.patch.object(client, "_execute", return_value=[{"id": 1, "name": "Alice"}]) as execute_mock:
            row = client.insert_row("public.users", {"name": "Alice", "active": True})
        self.assertEqual(row, {"id": 1, "name": "Alice"})
        self.assertEqual(execute_mock.call_args.args[1], ["Alice", True])
        self.assertIn('INSERT INTO "public"."users"', render(execute_mock.call_args.args[0]))

    def test_bulk_insert(self):
        client = make_client()

        with self.assertRaisesRegex(ValueError, "positive integer"):
            client.bulk_insert("users", [{"id": 1}], chunk_size=0)

        with self.assertRaisesRegex(ValueError, "requires do_nothing=True"):
            client.bulk_insert("users", [{"id": 1}], conflict_columns=["id"])

        with self.assertRaisesRegex(ValueError, "non-empty sequence"):
            client.bulk_insert("users", [])

        with self.assertRaisesRegex(ValueError, "same keys"):
            client.bulk_insert("users", [{"id": 1}, {"id": 2, "name": "n"}])

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id", "name"]), \
             mock.patch.object(client, "_execute", side_effect=[[{"?column?": 1}], [{"?column?": 1}]]) as execute_mock:
            affected = client.bulk_insert(
                "users",
                [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}],
                chunk_size=1,
                do_nothing=True,
                conflict_columns=["id"],
            )
        self.assertEqual(affected, 2)
        self.assertEqual(execute_mock.call_count, 2)
        self.assertEqual(execute_mock.call_args_list[0].args[1], [1, "A"])
        self.assertIn('INSERT INTO "users"', render(execute_mock.call_args_list[0].args[0]))
        self.assertIn('ON CONFLICT ("id") DO NOTHING', render(execute_mock.call_args_list[0].args[0]))

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id", "name"]), \
             mock.patch.object(client, "_execute", side_effect=[[{"id": 1}], [{"id": 2}]]) as execute_mock2:
            rows = client.bulk_insert(
                "users",
                [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}],
                chunk_size=1,
                returning=True,
            )
        self.assertEqual(rows, [{"id": 1}, {"id": 2}])
        self.assertIn("RETURNING *", render(execute_mock2.call_args_list[0].args[0]))

    def test_upsert_row_and_upsert_rows(self):
        client = make_client()

        with self.assertRaisesRegex(ValueError, "empty"):
            client.upsert_row("users", {}, conflict_columns=["id"])

        with self.assertRaisesRegex(ValueError, "non-empty sequence"):
            client.upsert_row("users", {"id": 1}, conflict_columns=[])

        with self.assertRaisesRegex(ValueError, "cannot be used when do_nothing=True"):
            client.upsert_row("users", {"id": 1}, conflict_columns=["id"], update_columns=["id"], do_nothing=True)

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id", "name", "active"]), \
             mock.patch.object(client, "_execute", return_value=[{"id": 1, "name": "Alice"}]) as execute_mock:
            row = client.upsert_row(
                "users",
                {"id": 1, "name": "Alice", "active": True},
                conflict_columns=["id"],
                update_columns=["name", "active"],
            )
        self.assertEqual(row, {"id": 1, "name": "Alice"})
        self.assertEqual(execute_mock.call_args.args[1], [1, "Alice", True])
        query_text = render(execute_mock.call_args.args[0])
        self.assertIn('ON CONFLICT ("id") DO UPDATE SET', query_text)
        self.assertIn('"name" = EXCLUDED."name"', query_text)
        self.assertIn('"active" = EXCLUDED."active"', query_text)

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id", "name"]), \
             mock.patch.object(client, "_execute", return_value=[]) as execute_mock2:
            out = client.upsert_row(
                "users",
                {"id": 1, "name": "Alice"},
                conflict_columns=["id"],
                do_nothing=True,
            )
        self.assertIsNone(out)
        self.assertIn("DO NOTHING", render(execute_mock2.call_args.args[0]))

        with self.assertRaisesRegex(ValueError, "positive integer"):
            client.upsert_rows("users", [{"id": 1}], conflict_columns=["id"], chunk_size=0)

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id", "name"]), \
             mock.patch.object(client, "_execute", side_effect=[[{"?column?": 1}, {"?column?": 1}], [{"?column?": 1}]]) as execute_mock3:
            affected = client.upsert_rows(
                "users",
                [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}, {"id": 3, "name": "C"}],
                conflict_columns=["id"],
                chunk_size=2,
                returning=False,
            )
        self.assertEqual(affected, 3)
        self.assertEqual(execute_mock3.call_count, 2)
        self.assertIn('ON CONFLICT ("id") DO UPDATE SET', render(execute_mock3.call_args_list[0].args[0]))

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id", "name"]), \
             mock.patch.object(client, "_execute", return_value=[{"id": 1}, {"id": 2}]) as execute_mock4:
            rows = client.upsert_rows(
                "users",
                [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}],
                conflict_columns=["id"],
                returning=True,
                do_nothing=True,
            )
        self.assertEqual(rows, [{"id": 1}, {"id": 2}])
        self.assertIn("DO NOTHING", render(execute_mock4.call_args.args[0]))

    def test_count_exists_and_single_row_helpers(self):
        client = make_client()

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id"]), \
             mock.patch.object(client, "_build_select_from_clause", return_value=sql.SQL(' FROM "users"')), \
             mock.patch.object(client, "_build_where_parts", return_value=([], [])), \
             mock.patch.object(client, "_execute", return_value=[{"cnt": 4}]):
            count = client.count_rows_with_filters("users")
        self.assertEqual(count, 4)

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id"]), \
             mock.patch.object(client, "_build_select_from_clause", return_value=sql.SQL(' FROM "users"')), \
             mock.patch.object(client, "_build_where_parts", return_value=([], [])), \
             mock.patch.object(client, "_execute", return_value=[{"?column?": 1}]):
            self.assertTrue(client.exists_with_filters("users"))

        with mock.patch.object(client, "get_rows_with_filters", return_value=([{"id": 1}], 1)):
            row = client.get_optional_one("users")
        self.assertEqual(row, {"id": 1})

        with mock.patch.object(client, "get_rows_with_filters", return_value=([], 0)):
            row2 = client.get_optional_one("users")
        self.assertIsNone(row2)

        with mock.patch.object(client, "count_rows_with_filters", return_value=0):
            with self.assertRaisesRegex(LookupError, "No rows"):
                client.get_one("users")

        with mock.patch.object(client, "count_rows_with_filters", return_value=2):
            with self.assertRaisesRegex(ValueError, "exactly one row"):
                client.get_one("users")

        with mock.patch.object(client, "count_rows_with_filters", return_value=1), \
             mock.patch.object(client, "get_optional_one", return_value={"id": 1}):
            row3 = client.get_one("users")
        self.assertEqual(row3, {"id": 1})

    def test_get_or_create_and_update_or_create(self):
        client = make_client()

        with self.assertRaisesRegex(ValueError, "non-empty dictionary"):
            client.get_or_create("users", {})

        with self.assertRaisesRegex(ValueError, "overlap"):
            client.get_or_create("users", {"id": 1}, defaults={"id": 2})

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id", "name", "active"]), \
             mock.patch.object(client, "get_optional_one", return_value={"id": 1, "name": "A"}) as get_one_mock, \
             mock.patch.object(client, "insert_row") as insert_mock:
            row, created = client.get_or_create("users", {"id": 1}, defaults={"name": "A"})
        self.assertFalse(created)
        self.assertEqual(row, {"id": 1, "name": "A"})
        get_one_mock.assert_called_once()
        insert_mock.assert_not_called()

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id", "name"]), \
             mock.patch.object(client, "get_optional_one", return_value=None), \
             mock.patch.object(client, "insert_row", return_value={"id": 2, "name": "B"}) as insert_mock2:
            row2, created2 = client.get_or_create("users", {"id": 2}, defaults={"name": "B"})
        self.assertTrue(created2)
        self.assertEqual(row2, {"id": 2, "name": "B"})
        insert_mock2.assert_called_once_with("users", {"id": 2, "name": "B"})

        with self.assertRaisesRegex(ValueError, "non-empty dictionary"):
            client.update_or_create("users", {})

        with self.assertRaisesRegex(ValueError, "overlap"):
            client.update_or_create("users", {"id": 1}, {"id": 2})

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id", "name"]), \
             mock.patch.object(client, "get_optional_one", side_effect=[{"id": 1, "name": "Old"}, {"id": 1, "name": "New"}]), \
             mock.patch.object(client, "update_rows_with_equalities") as update_mock:
            row3, created3 = client.update_or_create("users", {"id": 1}, {"name": "New"})
        self.assertFalse(created3)
        self.assertEqual(row3, {"id": 1, "name": "New"})
        update_mock.assert_called_once_with("users", {"name": "New"}, {"id": 1})

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id", "name"]), \
             mock.patch.object(client, "get_optional_one", return_value=None), \
             mock.patch.object(client, "insert_row", return_value={"id": 3, "name": "C"}) as insert_mock3:
            row4, created4 = client.update_or_create("users", {"id": 3}, {"name": "C"})
        self.assertTrue(created4)
        self.assertEqual(row4, {"id": 3, "name": "C"})
        insert_mock3.assert_called_once_with("users", {"id": 3, "name": "C"})

    def test_paged_execute_validation_and_sql(self):
        client = make_client()

        with self.assertRaisesRegex(ValueError, "positive integer"):
            client._paged_execute("SELECT 1", page_limit=0)

        with self.assertRaisesRegex(ValueError, "non-negative integer"):
            client._paged_execute("SELECT 1", page_num=-1)

        conn = FakeConnection()
        with mock.patch.object(client, "_get_conn", return_value=conn), mock.patch.object(client, "_put_conn") as put_conn:
            with self.assertRaisesRegex(ValueError, "SELECT/CTE"):
                client._paged_execute("DELETE FROM users")
        put_conn.assert_called_once_with(conn)

        conn2 = FakeConnection()
        with mock.patch.object(client, "_get_conn", return_value=conn2), \
             mock.patch.object(client, "_put_conn") as put_conn2, \
             mock.patch.object(client, "_execute_on_conn", return_value=[{"id": 1}]) as exec_on_conn:
            rows = client._paged_execute(
                "SELECT * FROM users",
                params=[99],
                page_limit=10,
                page_num=2,
                order_by="created_at",
                order_dir="DESC",
                tiebreaker="id",
                base_qualifier=sql.Identifier("users"),
            )
        self.assertEqual(rows, [{"id": 1}])
        put_conn2.assert_called_once_with(conn2)

        called_conn, final_sql, final_params = exec_on_conn.call_args.args
        self.assertIs(called_conn, conn2)
        self.assertIn('ORDER BY "users"."created_at" DESC, "users"."id" ASC', final_sql)
        self.assertIn("LIMIT %s OFFSET %s;", final_sql)
        self.assertEqual(final_params, [99, 10, 20])

    def test_get_rows_with_filters(self):
        client = make_client()

        with self.assertRaisesRegex(ValueError, "positive integer"):
            client.get_rows_with_filters("users", page_limit=0)

        with self.assertRaisesRegex(ValueError, "non-negative integer"):
            client.get_rows_with_filters("users", page_num=-1)

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id", "name"]):
            with self.assertRaisesRegex(ValueError, "Invalid columns"):
                client.get_rows_with_filters("users", equalities={"missing": 1})
            with self.assertRaisesRegex(ValueError, "Unknown order_by"):
                client.get_rows_with_filters("users", order_by="missing")

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id", "name"]), \
             mock.patch.object(client, "_build_select_from_clause", return_value=sql.SQL(' FROM "users"')), \
             mock.patch.object(client, "_build_where_parts", return_value=([], [])), \
             mock.patch.object(client, "_execute", return_value=[{"cnt": 0}]), \
             mock.patch.object(client, "_paged_execute") as paged_execute:
            rows, pages = client.get_rows_with_filters("users")
        self.assertEqual(rows, [])
        self.assertEqual(pages, 0)
        paged_execute.assert_not_called()

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id", "name"]), \
             mock.patch.object(client, "_build_select_from_clause", return_value=sql.SQL(' FROM "users"')), \
             mock.patch.object(client, "_build_where_parts", return_value=([sql.SQL('"name" = %s')], ["Alice"])), \
             mock.patch.object(client, "_execute", return_value=[{"cnt": 13}]) as execute_mock, \
             mock.patch.object(client, "_paged_execute", return_value=[{"id": 1, "name": "Alice"}]) as paged_execute2:
            rows2, pages2 = client.get_rows_with_filters("users", equalities={"name": "Alice"}, page_limit=5, page_num=1)
        self.assertEqual(rows2, [{"id": 1, "name": "Alice"}])
        self.assertEqual(pages2, 3)
        self.assertIn("SELECT COUNT(*) AS cnt", render(execute_mock.call_args.args[0]))
        self.assertEqual(paged_execute2.call_args.kwargs["order_by"], "id")
        self.assertEqual(paged_execute2.call_args.kwargs["order_dir"], "ASC")
        self.assertIsNone(paged_execute2.call_args.kwargs["tiebreaker"])

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id", "name"]), \
             mock.patch.object(client, "_build_select_from_clause", return_value=sql.SQL(' FROM "users"')), \
             mock.patch.object(client, "_build_where_parts", return_value=([], [])), \
             mock.patch.object(client, "_execute", return_value=[{"cnt": 1}]), \
             mock.patch.object(client, "_paged_execute", return_value=[{"id": 1, "name": "A"}]) as paged_execute3:
            client.get_rows_with_filters("users", order_by="name", order_dir="desc")
        self.assertEqual(paged_execute3.call_args.kwargs["order_by"], "name")
        self.assertEqual(paged_execute3.call_args.kwargs["order_dir"], "DESC")
        self.assertEqual(paged_execute3.call_args.kwargs["tiebreaker"], "id")

    def test_delete_and_update_paths(self):
        client = make_client()

        with self.assertRaisesRegex(ValueError, "Provide at least one"):
            client.delete_rows_with_filters("users")

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id"]), \
             mock.patch.object(client, "_build_relation_clause", return_value=(sql.SQL(""), [])), \
             mock.patch.object(client, "_build_where_parts", return_value=([], [])):
            with self.assertRaisesRegex(ValueError, "Refusing to delete"):
                client.delete_rows_with_filters("users", joins=[("JOIN", "t2", "t2.id = users.id")])

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id"]), \
             mock.patch.object(client, "_build_relation_clause", return_value=(sql.SQL(" USING roles r"), [sql.SQL("r.id = users.role_id")])), \
             mock.patch.object(client, "_build_where_parts", return_value=([sql.SQL('"id" = %s'), sql.SQL("r.id = users.role_id")], [1])), \
             mock.patch.object(client, "_execute", return_value=[{"?column?": 1}, {"?column?": 1}]):
            count = client.delete_rows_with_filters("users", equalities={"id": 1}, joins=[("JOIN", "roles r", "r.id = users.role_id")])
        self.assertEqual(count, 2)

        with self.assertRaisesRegex(ValueError, "Updates dictionary is empty"):
            client.update_rows_with_equalities("users", {}, {"id": 1})

        with self.assertRaisesRegex(ValueError, "Conditions dictionary is empty"):
            client.update_rows_with_equalities("users", {"name": "N"}, {})

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id", "name", "active"]), \
             mock.patch.object(client, "_execute", return_value=[{"?column?": 1}, {"?column?": 1}]) as execute_mock:
            count2 = client.update_rows_with_equalities("users", {"name": "Bob"}, {"active": True})
        self.assertEqual(count2, 2)
        self.assertEqual(execute_mock.call_args.args[1], ["Bob", True])

        with self.assertRaisesRegex(ValueError, "Updates dictionary is empty"):
            client.update_rows_with_filters("users", {})

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id", "name", "active"]), \
             mock.patch.object(client, "_build_relation_clause", return_value=(sql.SQL(""), [])), \
             mock.patch.object(client, "_build_where_parts", return_value=([], [])):
            with self.assertRaisesRegex(ValueError, "Refusing to update"):
                client.update_rows_with_filters("users", {"name": "x"})

        with mock.patch.object(client, "_require_existing_table_columns", return_value=["id", "name", "active"]), \
             mock.patch.object(client, "_build_relation_clause", return_value=(sql.SQL(" FROM roles r"), [sql.SQL("r.id = users.role_id")])), \
             mock.patch.object(client, "_build_where_parts", return_value=([sql.SQL('"active" = %s'), sql.SQL("r.id = users.role_id")], [True])), \
             mock.patch.object(client, "_execute", return_value=[{"?column?": 1}]) as execute_mock2:
            count3 = client.update_rows_with_filters(
                "users",
                {"name": "Updated"},
                equalities={"active": True},
                joins=[("JOIN", "roles r", "r.id = users.role_id")],
            )
        self.assertEqual(count3, 1)
        self.assertEqual(execute_mock2.call_args.args[1], ["Updated", True])


if __name__ == "__main__":
    unittest.main()
