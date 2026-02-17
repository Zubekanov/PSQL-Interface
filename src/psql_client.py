import logging
import re
from contextlib import contextmanager
from dataclasses import dataclass
from math import ceil
from threading import RLock
from typing import Any, Iterable, Optional, Sequence
from psycopg2 import sql
from psycopg2.pool import ThreadedConnectionPool

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class JoinSpec:
	"""
	Structured join definition used by filter APIs.
	"""
	join_type: str
	table_expr: str
	on_clause: str


@dataclass(frozen=True)
class RawCondition:
	"""
	Structured WHERE fragment with its own bound parameters.
	"""
	expression: str
	params: tuple[Any, ...] = ()


class PSQLClient:
	"""
	Thread-safe PostgreSQL client with a connection pool and convenience helpers.

	Create directly:
		client = PSQLClient(database="WebsiteDev", user="postgres", password="...", host="localhost", port=5432)

	Or reuse an existing pool by DSN via the cache:
		client = PSQLClient.get(database="WebsiteDev", user="postgres", host="localhost")

	Call `close()` when you're done with a specific client instance, or `PSQLClient.closeall()` to close all cached pools.
	"""

	_cache: dict[tuple, "PSQLClient"] = {}
	_cache_lock = RLock()
	_JOIN_RE = re.compile(
		r"^\s*(?P<join_type>(?:INNER|LEFT(?:\s+OUTER)?|RIGHT(?:\s+OUTER)?|FULL(?:\s+OUTER)?|CROSS|NATURAL)?\s*JOIN)\s+(?P<table_expr>.+?)\s+ON\s+(?P<on_clause>.+)\s*$",
		flags=re.IGNORECASE
	)

	@staticmethod
	def _freeze_conn_kwargs(conn_kwargs: dict[str, Any]) -> tuple[tuple[str, Any], ...] | None:
		"""
		Build a deterministic, hashable representation of extra connect kwargs.
		"""
		if not conn_kwargs:
			return None
		frozen: list[tuple[str, Any]] = []
		for key, value in sorted(conn_kwargs.items()):
			try:
				hash(value)
				frozen.append((key, value))
			except TypeError:
				# Fall back to repr for non-hashable kwargs (e.g., dict/list options).
				frozen.append((key, repr(value)))
		return tuple(frozen)

	@classmethod
	def get(
		cls,
		*,
		database: str = "postgres",
		user: str = "postgres",
		password: Optional[str] = None,
		host: Optional[str] = None,
		port: Optional[int] = None,
		minconn: int = 1,
		maxconn: int = 10,
		**conn_kwargs
	) -> "PSQLClient":
		"""
		Return a cached client for the same connection parameters, creating it if needed.
		Extra psycopg2 connection kwargs can be passed via **conn_kwargs (e.g., sslmode="require", options="...").
		"""
		key = (
			host, port, database, user, password,
			cls._freeze_conn_kwargs(conn_kwargs),
			minconn, maxconn
		)
		with cls._cache_lock:
			client = cls._cache.get(key)
			if client is None or client._closed:
				client = cls(
					database=database, user=user, password=password,
					host=host, port=port, minconn=minconn, maxconn=maxconn, **conn_kwargs
				)
				client._cache_key = key
				cls._cache[key] = client
				logger.debug("Created new cached PSQLClient for key: %s", key)
			else:
				logger.debug("Reusing cached PSQLClient for key: %s", key)
			return client

	@classmethod
	def closeall(cls) -> None:
		"""Close all cached connection pools and clear the cache."""
		with cls._cache_lock:
			clients = list(cls._cache.values())
			cls._cache.clear()
		for client in clients:
			try:
				client.close()
			except Exception:
				logger.exception("Error closing pooled client")

	def __init__(
		self,
		*,
		database: str = "postgres",
		user: str = "postgres",
		password: Optional[str] = None,
		host: Optional[str] = None,
		port: Optional[int] = None,
		minconn: int = 1,
		maxconn: int = 10,
		**conn_kwargs
	):
		# Store connect params for __repr__ / debugging
		self.database = database
		self.user = user
		self.host = host
		self.port = port
		self._closed = False
		self._state_lock = RLock()
		self._cache_key: tuple | None = None
		self._conn_kwargs = dict(conn_kwargs)
		if password is not None:
			self._conn_kwargs["password"] = password
		if host is not None:
			self._conn_kwargs["host"] = host
		if port is not None:
			self._conn_kwargs["port"] = port

		logger.debug("Creating PSQLClient for %s@%s:%s/%s", user, host or "", port or "", database)

		# Create pool
		self.pool = ThreadedConnectionPool(
			minconn, maxconn,
			database=self.database,
			user=self.user,
			**self._conn_kwargs
		)

	def __repr__(self) -> str:
		host = self.host or ""
		port = f":{self.port}" if self.port else ""
		return f"<PSQLClient {self.user}@{host}{port}/{self.database} pool={getattr(self.pool, 'minconn', '?')}-{getattr(self.pool, 'maxconn', '?')}>"

	# ---------- Pool plumbing ----------
	def close(self) -> None:
		"""Close this client's pool."""
		with self._state_lock:
			if self._closed:
				return
			self._closed = True
		try:
			self.pool.closeall()
		except Exception:
			logger.exception("Error closing connection pool")
		finally:
			cache_key = self._cache_key
			if cache_key is not None:
				with self.__class__._cache_lock:
					cached = self.__class__._cache.get(cache_key)
					if cached is self:
						self.__class__._cache.pop(cache_key, None)

	def _get_conn(self):
		with self._state_lock:
			if self._closed:
				raise RuntimeError("PSQLClient is closed.")
		return self.pool.getconn()

	def _put_conn(self, conn):
		try:
			self.pool.putconn(conn)
		except Exception:
			# If the pool is already closed while returning a connection, suppress.
			with self._state_lock:
				if not self._closed:
					raise

	class _Transaction:
		"""
		Transaction-scoped executor that reuses one checked-out connection.
		"""
		def __init__(self, client: "PSQLClient", conn):
			self._client = client
			self._conn = conn

		def execute(self, query, params: Optional[Iterable] = None) -> list[dict] | None:
			query_text = self._client._normalize_query(self._conn, query)
			with self._conn.cursor() as cur:
				cur.execute(query_text, list(params or []))
				return self._client._rows_from_cursor(cur)

	@contextmanager
	def transaction(self):
		"""
		Provide a transaction context that commits once on success and rolls back on error.

		Usage:
			with client.transaction() as tx:
				tx.execute("INSERT ...", [...])
				tx.execute("UPDATE ...", [...])
		"""
		conn = self._get_conn()
		prev_autocommit = getattr(conn, "autocommit", False)
		try:
			conn.autocommit = False
			yield self._Transaction(self, conn)
			conn.commit()
		except Exception:
			conn.rollback()
			raise
		finally:
			conn.autocommit = prev_autocommit
			self._put_conn(conn)

	# ---------- Execution helpers ----------
	@staticmethod
	def _normalize_query(conn, query) -> str:
		if isinstance(query, str):
			return query
		return query.as_string(conn)

	@staticmethod
	def _rows_from_cursor(cur) -> list[dict] | None:
		if cur.description is None:
			return None
		colnames = [d[0] for d in cur.description]
		rows = cur.fetchall()
		return [dict(zip(colnames, r)) for r in rows]

	def _execute_on_conn(
		self,
		conn,
		query,
		params: Optional[Iterable] = None,
		*,
		autocommit: bool = False,
	) -> list[dict] | None:
		"""
		Core execution helper that can run using an already-acquired connection.
		"""
		prev_autocommit = getattr(conn, "autocommit", False)
		try:
			if autocommit:
				conn.autocommit = True
			query_text = self._normalize_query(conn, query)
			with conn.cursor() as cur:
				cur.execute(query_text, list(params or []))
				rows = self._rows_from_cursor(cur)
			if not autocommit:
				conn.commit()
			return rows
		except Exception:
			if not autocommit:
				conn.rollback()
			raise
		finally:
			if autocommit:
				conn.autocommit = prev_autocommit

	def _execute(self, query, params: Optional[Iterable] = None) -> list[dict] | None:
		"""
		Executes SQL (string or psycopg2.sql Composable).
		Returns list[dict] for result sets, otherwise None.
		Commits on success; rolls back on exception.
		"""
		conn = self._get_conn()
		try:
			return self._execute_on_conn(conn, query, params, autocommit=False)
		finally:
			self._put_conn(conn)

	def execute_query(self, query, params: Optional[Iterable] = None) -> list[dict] | None:
		"""
		Public wrapper for executing raw SQL (read or write).
		"""
		return self._execute(query, params)

	def _execute_autocommit(self, query, params: Optional[Iterable] = None) -> list[dict] | None:
		"""
		Execute a statement that must run outside a transaction (e.g., CREATE/DROP DATABASE).
		Returns None or list[dict] like _execute.
		"""
		conn = self._get_conn()
		try:
			return self._execute_on_conn(conn, query, params, autocommit=True)
		finally:
			self._put_conn(conn)

	# ---------- Database-level helpers (autocommit required) ----------
	def database_exists(self, db_name: str) -> bool:
		q = "SELECT 1 FROM pg_database WHERE datname = %s;"
		return bool(self._execute(q, [db_name]))

	def create_database(self, db_name: str, exists_ok: bool = True) -> bool:
		if exists_ok and self.database_exists(db_name):
			return False
		q = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name))
		self._execute_autocommit(q)
		return True

	def drop_database(self, db_name: str, missing_ok: bool = True) -> bool:
		if not missing_ok and not self.database_exists(db_name):
			raise ValueError(f"Database not found: {db_name}")
		q = sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name))
		self._execute_autocommit(q)
		return True

	# ---------- Schema helpers ----------
	def schema_exists(self, schema: str) -> bool:
		q = "SELECT 1 FROM pg_namespace WHERE nspname = %s;"
		return bool(self._execute(q, [schema]))

	def list_schemas(self, exclude_system: bool = True) -> list[str]:
		if exclude_system:
			q = """
				SELECT schema_name
				FROM information_schema.schemata
				WHERE schema_name NOT IN ('information_schema','pg_catalog')
				AND schema_name NOT LIKE 'pg_toast%%'
				AND schema_name NOT LIKE 'pg_temp%%'
				ORDER BY schema_name;
			"""
			return [r["schema_name"] for r in self._execute(q)]
		else:
			q = "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name;"
			return [r["schema_name"] for r in self._execute(q)]

	def create_schema(self, schema: str, exists_ok: bool = True) -> None:
		if exists_ok:
			q = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
		else:
			q = sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))
		self._execute(q)

	def drop_schema(self, schema: str, cascade: bool = False, missing_ok: bool = True) -> None:
		q = sql.SQL("DROP SCHEMA {} {} {}").format(
			sql.SQL("IF EXISTS") if missing_ok else sql.SQL(""),
			sql.Identifier(schema),
			sql.SQL("CASCADE") if cascade else sql.SQL("")
		)
		self._execute(q)

	def ensure_schema(self, schema: str) -> None:
		self.create_schema(schema, exists_ok=True)

	# ---------- Table & column helpers ----------
	def table_exists(self, schema: str, table: str) -> bool:
		q = """
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = %s AND table_name = %s;
		"""
		return bool(self._execute(q, [schema, table]))

	def list_tables(self, schema: str = "public") -> list[str]:
		q = """
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = %s AND table_type = 'BASE TABLE'
			ORDER BY table_name;
		"""
		return [r["table_name"] for r in self._execute(q, [schema])]

	def _fetch_table_columns(self, schema: str | None, table: str) -> list[str]:
		if schema:
			q = """
				SELECT column_name
				FROM information_schema.columns
				WHERE table_schema = %s AND table_name = %s
				ORDER BY ordinal_position;
			"""
			rows = self._execute(q, [schema, table]) or []
		else:
			q = """
				SELECT column_name
				FROM information_schema.columns
				WHERE table_name = %s
				ORDER BY ordinal_position;
			"""
			rows = self._execute(q, [table]) or []
		return [r["column_name"] for r in rows]

	def get_table_columns(self, schema: str, table: str) -> list[str]:
		return self._fetch_table_columns(schema, table)

	def column_exists(self, schema: str, table: str, column: str) -> bool:
		q = """
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = %s AND table_name = %s AND column_name = %s;
		"""
		return bool(self._execute(q, [schema, table, column]))

	def _require_existing_table_columns(self, table: str) -> list[str]:
		valid_columns = self._get_table_columns(table)
		if not valid_columns:
			raise ValueError(f"Table '{table}' does not exist.")
		return valid_columns

	@staticmethod
	def _validate_known_columns(valid_columns: Sequence[str], columns: Iterable[str], label: str) -> None:
		invalid = [c for c in columns if c not in valid_columns]
		if invalid:
			raise ValueError(f"Invalid columns for {label}: {invalid}")

	def create_table(
		self,
		schema: str,
		table: str,
		columns: dict[str, str],
		constraints: list[str] | None = None,
		if_not_exists: bool = True,
		temporary: bool = False
	) -> None:
		if not columns:
			raise ValueError("columns must be a non-empty dict of {name: SQL type/constraint}.")
		col_bits = []
		for name, type_sql in columns.items():
			if not isinstance(type_sql, str) or not type_sql.strip():
				raise ValueError(f"Invalid type/constraint for column '{name}'.")
			col_bits.append(sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(type_sql)))
		if constraints:
			col_bits.extend(sql.SQL(c) for c in constraints)

		q = sql.SQL("CREATE {temp} TABLE {ine} {sch}.{tbl} ({cols})").format(
			temp=sql.SQL("TEMP") if temporary else sql.SQL(""),
			ine=sql.SQL("IF NOT EXISTS") if if_not_exists else sql.SQL(""),
			sch=sql.Identifier(schema),
			tbl=sql.Identifier(table),
			cols=sql.SQL(", ").join(col_bits)
		)
		self._execute(q)

	def drop_table(self, schema: str, table: str, cascade: bool = False, missing_ok: bool = True) -> None:
		q = sql.SQL("DROP TABLE {} {}.{} {}").format(
			sql.SQL("IF EXISTS") if missing_ok else sql.SQL(""),
			sql.Identifier(schema),
			sql.Identifier(table),
			sql.SQL("CASCADE") if cascade else sql.SQL("")
		)
		self._execute(q)

	def ensure_table(self, schema: str, table: str, columns: dict[str, str], constraints: list[str] | None = None) -> None:
		self.create_table(schema, table, columns, constraints, if_not_exists=True)

	# ---------- Index helpers ----------
	def index_exists(self, schema: str, index_name: str) -> bool:
		q = """
			SELECT 1
			FROM pg_indexes
			WHERE schemaname = %s AND indexname = %s;
		"""
		return bool(self._execute(q, [schema, index_name]))

	def create_index(
		self,
		schema: str,
		table: str,
		index_name: str,
		columns: list[str],
		unique: bool = False,
		if_not_exists: bool = True
	) -> None:
		if not columns:
			raise ValueError("columns must be a non-empty list of column names.")
		prefix = sql.SQL("CREATE {} INDEX {} {}").format(
			sql.SQL("UNIQUE") if unique else sql.SQL(""),
			sql.SQL("IF NOT EXISTS") if if_not_exists else sql.SQL(""),
			sql.Identifier(index_name),
		)
		cols = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
		q = prefix + sql.SQL(" ON {}.{} ({})").format(sql.Identifier(schema), sql.Identifier(table), cols)
		self._execute(q)

	def drop_index(self, schema: str, index_name: str, missing_ok: bool = True) -> None:
		q = sql.SQL("DROP INDEX {} {}.{}").format(
			sql.SQL("IF EXISTS") if missing_ok else sql.SQL(""),
			sql.Identifier(schema),
			sql.Identifier(index_name),
		)
		self._execute(q)
	
	def drop_column(
		self,
		schema: str,
		table: str,
		column: str,
		*,
		cascade: bool = False,
		missing_ok: bool = True,
	) -> None:
		q = sql.SQL("ALTER TABLE {}.{} DROP COLUMN {} {}").format(
			sql.Identifier(schema),
			sql.Identifier(table),
			sql.SQL("IF EXISTS") if missing_ok else sql.SQL(""),
			sql.Identifier(column),
		)
		if cascade:
			q = q + sql.SQL(" CASCADE")
		self._execute(q)

	def alter_column_type(
		self,
		schema: str,
		table: str,
		column: str,
		type_sql: str,
		*,
		using: str | None = None,
	) -> None:
		q = sql.SQL("ALTER TABLE {}.{} ALTER COLUMN {} TYPE {}").format(
			sql.Identifier(schema),
			sql.Identifier(table),
			sql.Identifier(column),
			sql.SQL(type_sql),
		)
		if using:
			q = q + sql.SQL(" USING ") + sql.SQL(using)
		self._execute(q)

	def alter_column_nullability(
		self,
		schema: str,
		table: str,
		column: str,
		*,
		nullable: bool,
	) -> None:
		q = sql.SQL("ALTER TABLE {}.{} ALTER COLUMN {} {}").format(
			sql.Identifier(schema),
			sql.Identifier(table),
			sql.Identifier(column),
			sql.SQL("DROP NOT NULL") if nullable else sql.SQL("SET NOT NULL"),
		)
		self._execute(q)

	def alter_column_default(
		self,
		schema: str,
		table: str,
		column: str,
		*,
		default_sql: str | None = None,
		drop: bool = False,
	) -> None:
		if drop:
			q = sql.SQL("ALTER TABLE {}.{} ALTER COLUMN {} DROP DEFAULT").format(
				sql.Identifier(schema),
				sql.Identifier(table),
				sql.Identifier(column),
			)
		else:
			q = sql.SQL("ALTER TABLE {}.{} ALTER COLUMN {} SET DEFAULT {}").format(
				sql.Identifier(schema),
				sql.Identifier(table),
				sql.Identifier(column),
				sql.SQL(default_sql if default_sql is not None else "NULL"),
			)
		self._execute(q)

	def drop_constraint(
		self,
		schema: str,
		table: str,
		constraint_name: str,
		*,
		missing_ok: bool = True,
	) -> None:
		q = sql.SQL("ALTER TABLE {}.{} DROP CONSTRAINT {}{}").format(
			sql.Identifier(schema),
			sql.Identifier(table),
			sql.SQL("IF EXISTS ") if missing_ok else sql.SQL(""),
			sql.Identifier(constraint_name),
		)
		self._execute(q)

	# ---------- Name helpers ----------
	def _split_qualified(self, qname) -> tuple[Optional[str], str]:
		"""
		Accepts 'schema.table', 'table', or ('schema','table').
		Returns (schema_or_None, table).
		"""
		if isinstance(qname, (tuple, list)):
			if len(qname) == 2:
				return (str(qname[0]), str(qname[1]))
			if len(qname) == 1:
				return (None, str(qname[0]))
			raise ValueError("qname tuple/list must be length 1 or 2")
		s = str(qname).strip()
		if "." in s:
			schema, table = s.split(".", 1)
			return (schema.strip('"'), table.strip('"'))
		return (None, s.strip('"'))

	def _ident_qualified(self, qname) -> sql.Composed:
		"""
		Build a properly quoted identifier for an optional schema-qualified name.
		"""
		schema, table = self._split_qualified(qname)
		if schema:
			return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))
		return sql.Identifier(table)

	def _normalize_join_spec(self, join_entry) -> JoinSpec:
		"""
		Normalize join inputs into JoinSpec.
		Accepted forms: JoinSpec, 3-tuple, or SQL string like 'LEFT JOIN x ON ...'.
		"""
		if isinstance(join_entry, JoinSpec):
			join_type = join_entry.join_type
			table_expr = join_entry.table_expr
			on_clause = join_entry.on_clause
		elif isinstance(join_entry, (tuple, list)):
			if len(join_entry) != 3:
				raise ValueError("joins tuple/list entries must have 3 items: (join_type, table_expr, on_clause).")
			join_type, table_expr, on_clause = join_entry
		elif isinstance(join_entry, str):
			match = self._JOIN_RE.match(join_entry.strip())
			if not match:
				raise ValueError(
					"String join entries must follow '<JOIN TYPE> JOIN <table_expr> ON <on_clause>' "
					"or use a 3-tuple / JoinSpec."
				)
			join_type = match.group("join_type")
			table_expr = match.group("table_expr")
			on_clause = match.group("on_clause")
		else:
			raise ValueError("Unsupported join entry type.")

		join_type = str(join_type).strip().upper()
		if not join_type:
			join_type = "JOIN"
		elif "JOIN" not in join_type:
			join_type = f"{join_type} JOIN"

		table_expr = str(table_expr).strip()
		on_clause = str(on_clause).strip()
		if not table_expr or not on_clause:
			raise ValueError("joins entries require non-empty table_expr and on_clause.")

		return JoinSpec(join_type=join_type, table_expr=table_expr, on_clause=on_clause)

	def _normalize_joins(self, joins) -> list[JoinSpec]:
		return [self._normalize_join_spec(j) for j in (joins or [])]

	def _build_select_from_clause(self, table: str, joins) -> sql.Composable:
		from_clause = sql.SQL(" FROM ") + self._ident_qualified(table)
		for j in self._normalize_joins(joins):
			from_clause += sql.SQL(" {} {} ON {}").format(
				sql.SQL(j.join_type),
				sql.SQL(j.table_expr),
				sql.SQL(j.on_clause),
			)
		return from_clause

	def _build_relation_clause(self, joins, keyword: str) -> tuple[sql.Composable, list[sql.Composable]]:
		normalized = self._normalize_joins(joins)
		if not normalized:
			return sql.SQL(""), []
		clause = sql.SQL(f" {keyword} ") + sql.SQL(", ").join(sql.SQL(j.table_expr) for j in normalized)
		on_parts = [sql.SQL(j.on_clause) for j in normalized]
		return clause, on_parts

	@staticmethod
	def _normalize_raw_conditions(
		raw_conditions,
		raw_params: list | tuple | None = None,
	) -> tuple[list[sql.Composable], list]:
		fragments: list[sql.Composable] = []
		params: list = []

		if raw_conditions is None:
			if raw_params:
				raise ValueError("raw_params was provided without raw_conditions.")
			return fragments, params

		if isinstance(raw_conditions, (str, RawCondition, sql.Composable)):
			condition_items = [raw_conditions]
		else:
			condition_items = list(raw_conditions)
		if not condition_items:
			if raw_params:
				raise ValueError("raw_params was provided without usable raw_conditions.")
			return fragments, params

		for condition in condition_items:
			if isinstance(condition, RawCondition):
				expr = condition.expression.strip()
				if not expr:
					raise ValueError("RawCondition.expression cannot be empty.")
				fragments.append(sql.SQL(expr))
				params.extend(list(condition.params))
			elif isinstance(condition, sql.Composable):
				fragments.append(condition)
			else:
				expr = str(condition).strip()
				if not expr:
					raise ValueError("raw_conditions contains an empty SQL fragment.")
				fragments.append(sql.SQL(expr))

		if raw_params:
			params.extend(list(raw_params))

		return fragments, params

	def _build_where_parts(
		self,
		*,
		equalities: dict | None = None,
		raw_conditions=None,
		raw_params: list | tuple | None = None,
		extra_conditions: list[sql.Composable] | None = None,
	) -> tuple[list[sql.Composable], list]:
		where_parts: list[sql.Composable] = []
		params: list = []

		if equalities:
			items = list(equalities.items())
			where_parts.extend(
				sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
				for k, _ in items
			)
			params.extend(v for _, v in items)

		if extra_conditions:
			where_parts.extend(extra_conditions)

		raw_parts, raw_values = self._normalize_raw_conditions(raw_conditions, raw_params)
		where_parts.extend(raw_parts)
		params.extend(raw_values)
		return where_parts, params

	@staticmethod
	def _prepare_bulk_rows(rows: Sequence[dict]) -> tuple[list[str], list[list[Any]]]:
		if not rows:
			raise ValueError("rows must be a non-empty sequence of dictionaries.")
		first = rows[0]
		if not isinstance(first, dict) or not first:
			raise ValueError("rows[0] must be a non-empty dictionary.")

		columns = list(first.keys())
		expected = set(columns)
		value_rows: list[list[Any]] = []
		for idx, row in enumerate(rows):
			if not isinstance(row, dict) or not row:
				raise ValueError(f"rows[{idx}] must be a non-empty dictionary.")
			if set(row.keys()) != expected:
				raise ValueError("All rows must contain the same keys.")
			value_rows.append([row[c] for c in columns])

		return columns, value_rows

	# ---------- Simple INSERT ----------
	def insert_row(self, table: str, data: dict) -> dict | None:
		if not data:
			raise ValueError("Data dictionary is empty.")
		columns = list(data.keys())
		values = list(data.values())

		query = sql.SQL(
			"INSERT INTO {tbl} ({fields}) VALUES ({placeholders}) RETURNING *"
		).format(
			tbl=self._ident_qualified(table),
			fields=sql.SQL(', ').join(sql.Identifier(c) for c in columns),
			placeholders=sql.SQL(', ').join(sql.Placeholder() for _ in columns),
		)
		result = self._execute(query, values)
		return result[0] if result else None

	def bulk_insert(
		self,
		table: str,
		rows: Sequence[dict],
		*,
		chunk_size: int = 500,
		returning: bool = False,
		do_nothing: bool = False,
		conflict_columns: Sequence[str] | None = None,
	) -> list[dict] | int:
		"""
		Insert many rows in chunks.

		When returning=False, returns affected row count.
		When returning=True, returns returned rows.
		"""
		if not isinstance(chunk_size, int) or chunk_size <= 0:
			raise ValueError("chunk_size must be a positive integer.")
		if conflict_columns and not do_nothing:
			raise ValueError("conflict_columns requires do_nothing=True in bulk_insert.")

		columns, value_rows = self._prepare_bulk_rows(rows)

		valid_columns = self._require_existing_table_columns(table)
		self._validate_known_columns(valid_columns, columns, "insert")
		if conflict_columns:
			self._validate_known_columns(valid_columns, conflict_columns, "conflict")

		row_placeholders = sql.SQL("(") + sql.SQL(", ").join(sql.Placeholder() for _ in columns) + sql.SQL(")")
		inserted_rows: list[dict] = []
		affected = 0

		for start in range(0, len(value_rows), chunk_size):
			chunk = value_rows[start:start + chunk_size]
			values_clause = sql.SQL(", ").join(row_placeholders for _ in chunk)
			query = sql.SQL("INSERT INTO {tbl} ({fields}) VALUES {values}").format(
				tbl=self._ident_qualified(table),
				fields=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
				values=values_clause,
			)

			if do_nothing:
				if conflict_columns:
					query += sql.SQL(" ON CONFLICT ({}) DO NOTHING").format(
						sql.SQL(", ").join(sql.Identifier(c) for c in conflict_columns)
					)
				else:
					query += sql.SQL(" ON CONFLICT DO NOTHING")

			if returning:
				query += sql.SQL(" RETURNING *;")
			else:
				query += sql.SQL(" RETURNING 1;")

			params = [value for row_values in chunk for value in row_values]
			result = self._execute(query, params) or []

			if returning:
				inserted_rows.extend(result)
			else:
				affected += len(result)

		return inserted_rows if returning else affected

	def upsert_row(
		self,
		table: str,
		data: dict,
		*,
		conflict_columns: Sequence[str],
		update_columns: Sequence[str] | None = None,
		do_nothing: bool = False,
		returning: bool = True,
	) -> dict | None:
		"""
		Insert one row and resolve conflicts via DO UPDATE or DO NOTHING.
		"""
		if not data:
			raise ValueError("Data dictionary is empty.")
		if not conflict_columns:
			raise ValueError("conflict_columns must be a non-empty sequence.")
		if do_nothing and update_columns is not None:
			raise ValueError("update_columns cannot be used when do_nothing=True.")

		columns = list(data.keys())
		values = [data[c] for c in columns]
		valid_columns = self._require_existing_table_columns(table)
		self._validate_known_columns(valid_columns, columns, "insert")
		self._validate_known_columns(valid_columns, conflict_columns, "conflict")

		if update_columns is None:
			update_cols = [c for c in columns if c not in set(conflict_columns)]
		else:
			update_cols = list(update_columns)
			self._validate_known_columns(valid_columns, update_cols, "update")
			missing = [c for c in update_cols if c not in data]
			if missing:
				raise ValueError(f"update_columns must exist in input data: {missing}")

		query = sql.SQL(
			"INSERT INTO {tbl} ({fields}) VALUES ({placeholders}) ON CONFLICT ({conflicts})"
		).format(
			tbl=self._ident_qualified(table),
			fields=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
			placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in columns),
			conflicts=sql.SQL(", ").join(sql.Identifier(c) for c in conflict_columns),
		)

		if do_nothing or not update_cols:
			query += sql.SQL(" DO NOTHING")
		else:
			set_sql = sql.SQL(", ").join(
				sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
				for c in update_cols
			)
			query += sql.SQL(" DO UPDATE SET ") + set_sql

		if returning:
			query += sql.SQL(" RETURNING *;")
			result = self._execute(query, values)
			return result[0] if result else None

		self._execute(query, values)
		return None

	def upsert_rows(
		self,
		table: str,
		rows: Sequence[dict],
		*,
		conflict_columns: Sequence[str],
		update_columns: Sequence[str] | None = None,
		do_nothing: bool = False,
		chunk_size: int = 500,
		returning: bool = False,
	) -> list[dict] | int:
		"""
		Batch upsert with configurable conflict behavior.
		"""
		if not conflict_columns:
			raise ValueError("conflict_columns must be a non-empty sequence.")
		if not isinstance(chunk_size, int) or chunk_size <= 0:
			raise ValueError("chunk_size must be a positive integer.")
		if do_nothing and update_columns is not None:
			raise ValueError("update_columns cannot be used when do_nothing=True.")

		columns, value_rows = self._prepare_bulk_rows(rows)

		valid_columns = self._require_existing_table_columns(table)
		self._validate_known_columns(valid_columns, columns, "insert")
		self._validate_known_columns(valid_columns, conflict_columns, "conflict")

		if update_columns is None:
			update_cols = [c for c in columns if c not in set(conflict_columns)]
		else:
			update_cols = list(update_columns)
			self._validate_known_columns(valid_columns, update_cols, "update")
			missing = [c for c in update_cols if c not in columns]
			if missing:
				raise ValueError(f"update_columns must exist in row payloads: {missing}")

		row_placeholders = sql.SQL("(") + sql.SQL(", ").join(sql.Placeholder() for _ in columns) + sql.SQL(")")
		all_rows: list[dict] = []
		affected = 0

		for start in range(0, len(value_rows), chunk_size):
			chunk = value_rows[start:start + chunk_size]
			values_clause = sql.SQL(", ").join(row_placeholders for _ in chunk)
			query = sql.SQL(
				"INSERT INTO {tbl} ({fields}) VALUES {values} ON CONFLICT ({conflicts})"
			).format(
				tbl=self._ident_qualified(table),
				fields=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
				values=values_clause,
				conflicts=sql.SQL(", ").join(sql.Identifier(c) for c in conflict_columns),
			)

			if do_nothing or not update_cols:
				query += sql.SQL(" DO NOTHING")
			else:
				set_sql = sql.SQL(", ").join(
					sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
					for c in update_cols
				)
				query += sql.SQL(" DO UPDATE SET ") + set_sql

			if returning:
				query += sql.SQL(" RETURNING *;")
			else:
				query += sql.SQL(" RETURNING 1;")

			params = [value for row_values in chunk for value in row_values]
			result = self._execute(query, params) or []

			if returning:
				all_rows.extend(result)
			else:
				affected += len(result)

		return all_rows if returning else affected

	def get_or_create(self, table: str, lookup: dict, defaults: dict | None = None) -> tuple[dict, bool]:
		"""
		Fetch one row by equality lookup or create it.
		Returns (row, created).
		"""
		if not lookup:
			raise ValueError("lookup must be a non-empty dictionary.")
		defaults = defaults or {}
		overlap = sorted(set(lookup.keys()) & set(defaults.keys()))
		if overlap:
			raise ValueError(f"lookup and defaults overlap on keys: {overlap}")

		valid_columns = self._require_existing_table_columns(table)
		self._validate_known_columns(valid_columns, lookup.keys(), "lookup")
		if defaults:
			self._validate_known_columns(valid_columns, defaults.keys(), "defaults")

		existing = self.get_optional_one(table, equalities=lookup)
		if existing is not None:
			return existing, False

		payload = dict(lookup)
		payload.update(defaults)
		created = self.insert_row(table, payload)
		if created is None:
			refetched = self.get_optional_one(table, equalities=payload)
			if refetched is None:
				raise RuntimeError("insert_row did not return data and refetch failed.")
			return refetched, True
		return created, True

	def update_or_create(self, table: str, lookup: dict, updates: dict | None = None) -> tuple[dict, bool]:
		"""
		Fetch one row by lookup and update it, or create when absent.
		Returns (row, created).
		"""
		if not lookup:
			raise ValueError("lookup must be a non-empty dictionary.")
		updates = updates or {}
		overlap = sorted(set(lookup.keys()) & set(updates.keys()))
		if overlap:
			raise ValueError(f"lookup and updates overlap on keys: {overlap}")

		valid_columns = self._require_existing_table_columns(table)
		self._validate_known_columns(valid_columns, lookup.keys(), "lookup")
		if updates:
			self._validate_known_columns(valid_columns, updates.keys(), "update")

		existing = self.get_optional_one(table, equalities=lookup)
		if existing is not None:
			if updates:
				self.update_rows_with_equalities(table, updates, lookup)
				refetched = self.get_optional_one(table, equalities=lookup)
				if refetched is not None:
					return refetched, False
			return existing, False

		payload = dict(lookup)
		payload.update(updates)
		created = self.insert_row(table, payload)
		if created is None:
			refetched = self.get_optional_one(table, equalities=payload)
			if refetched is None:
				raise RuntimeError("insert_row did not return data and refetch failed.")
			return refetched, True
		return created, True

	# ---------- Pagination core ----------
	def _paged_execute(
		self, query, params: list | None = None, page_limit: int = 50, page_num: int = 0,
		order_by: str | None = None, order_dir: str = "ASC", tiebreaker: str | None = None,
		base_qualifier: str | sql.Composable | None = None,
	) -> list[dict]:
		if not isinstance(page_limit, int) or page_limit <= 0:
			raise ValueError("page_limit must be a positive integer.")
		if not isinstance(page_num, int) or page_num < 0:
			raise ValueError("page_num must be a non-negative integer.")

		params = list(params or [])
		limit = page_limit
		offset = page_limit * page_num

		conn = self._get_conn()
		try:
			base = self._normalize_query(conn, query).strip().rstrip(';')

			head = base.lstrip().split(None, 1)[0].upper() if base else ""
			if head not in {"SELECT", "WITH"}:
				raise ValueError(f"_paged_execute expects a SELECT/CTE, got: {head or 'EMPTY'}")

			qual_str = None
			if base_qualifier:
				if isinstance(base_qualifier, sql.Composable):
					qual_str = base_qualifier.as_string(conn)
				else:
					qual_str = str(base_qualifier)

			order_sql = ""
			if order_by:
				dir_up = str(order_dir).upper()
				if dir_up not in {"ASC", "DESC"}:
					raise ValueError("order_dir must be 'ASC' or 'DESC'")
				ob = sql.Identifier(order_by).as_string(conn)
				ob_qual = f"{qual_str}.{ob}" if qual_str else ob
				order_sql = f" ORDER BY {ob_qual} {dir_up}"
				if tiebreaker and tiebreaker != order_by:
					tb = sql.Identifier(tiebreaker).as_string(conn)
					tb_qual = f"{qual_str}.{tb}" if qual_str else tb
					order_sql += f", {tb_qual} ASC"

			final_sql = f"{base}{order_sql} LIMIT %s OFFSET %s;"
			return self._execute_on_conn(conn, final_sql, params + [limit, offset], autocommit=False) or []
		finally:
			self._put_conn(conn)

	# ---------- Column discovery for base table ----------
	def _get_table_columns(self, table: str) -> list[str]:
		schema, tbl = self._split_qualified(table)
		return self._fetch_table_columns(schema, tbl)

	# ---------- Unified SELECT with filters/joins/paging ----------
	def get_rows_with_filters(
		self,
		table: str,
		equalities: dict | None = None,
		raw_conditions: RawCondition | sql.Composable | str | list[RawCondition | sql.Composable | str] | None = None,
		raw_params: list | None = None,
		joins: list[JoinSpec | tuple[str, str, str] | str] | None = None,
		page_limit: int = 50,
		page_num: int = 0,
		order_by: str | None = None,
		order_dir: str = "ASC"
	) -> tuple[list[dict], int]:
		
		if not isinstance(page_limit, int) or page_limit <= 0:
			raise ValueError("page_limit must be a positive integer.")
		if not isinstance(page_num, int) or page_num < 0:
			raise ValueError("page_num must be a non-negative integer.")

		valid_columns = self._require_existing_table_columns(table)
		if equalities:
			self._validate_known_columns(valid_columns, equalities.keys(), "condition")

		if order_by is not None:
			if order_by not in valid_columns:
				raise ValueError(f"Unknown order_by column: {order_by}")
			order_col = order_by
		else:
			order_col = "id" if "id" in valid_columns else valid_columns[0]

		dir_up = str(order_dir).upper()
		if dir_up not in {"ASC", "DESC"}:
			raise ValueError("order_dir must be 'ASC' or 'DESC'")

		tiebreaker = "id" if ("id" in valid_columns and order_col != "id") else None

		from_clause = self._build_select_from_clause(table, joins)
		where_parts, params = self._build_where_parts(
			equalities=equalities,
			raw_conditions=raw_conditions,
			raw_params=raw_params,
		)

		where_sql = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_parts) if where_parts else sql.SQL("")

		q_count = sql.SQL("SELECT COUNT(*) AS cnt") + from_clause + where_sql + sql.SQL(";")
		count_result = self._execute(q_count, params)
		total = int(count_result[0]["cnt"]) if count_result else 0
		if total == 0:
			return [], 0

		total_pages = ceil(total / page_limit)

		q_select = sql.SQL("SELECT *") + from_clause + where_sql
		rows = self._paged_execute(
			q_select,
			params,
			page_limit=page_limit,
			page_num=page_num,
			order_by=order_col,
			order_dir=dir_up,
			tiebreaker=tiebreaker,
			base_qualifier=self._ident_qualified(table),
		) or []

		return rows, total_pages

	def count_rows_with_filters(
		self,
		table: str,
		equalities: dict | None = None,
		raw_conditions: RawCondition | sql.Composable | str | list[RawCondition | sql.Composable | str] | None = None,
		raw_params: list | None = None,
		joins: list[JoinSpec | tuple[str, str, str] | str] | None = None,
	) -> int:
		"""
		Return the number of rows matching the provided filter conditions.
		"""
		valid_columns = self._require_existing_table_columns(table)
		if equalities:
			self._validate_known_columns(valid_columns, equalities.keys(), "condition")

		from_clause = self._build_select_from_clause(table, joins)
		where_parts, params = self._build_where_parts(
			equalities=equalities,
			raw_conditions=raw_conditions,
			raw_params=raw_params,
		)
		where_sql = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_parts) if where_parts else sql.SQL("")

		q = sql.SQL("SELECT COUNT(*) AS cnt") + from_clause + where_sql + sql.SQL(";")
		result = self._execute(q, params)
		return int(result[0]["cnt"]) if result else 0

	def exists_with_filters(
		self,
		table: str,
		equalities: dict | None = None,
		raw_conditions: RawCondition | sql.Composable | str | list[RawCondition | sql.Composable | str] | None = None,
		raw_params: list | None = None,
		joins: list[JoinSpec | tuple[str, str, str] | str] | None = None,
	) -> bool:
		"""
		Return True if at least one row matches filters.
		"""
		valid_columns = self._require_existing_table_columns(table)
		if equalities:
			self._validate_known_columns(valid_columns, equalities.keys(), "condition")

		from_clause = self._build_select_from_clause(table, joins)
		where_parts, params = self._build_where_parts(
			equalities=equalities,
			raw_conditions=raw_conditions,
			raw_params=raw_params,
		)
		where_sql = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_parts) if where_parts else sql.SQL("")
		q = sql.SQL("SELECT 1") + from_clause + where_sql + sql.SQL(" LIMIT 1;")
		return bool(self._execute(q, params))

	def get_optional_one(
		self,
		table: str,
		equalities: dict | None = None,
		raw_conditions: RawCondition | sql.Composable | str | list[RawCondition | sql.Composable | str] | None = None,
		raw_params: list | None = None,
		joins: list[JoinSpec | tuple[str, str, str] | str] | None = None,
		order_by: str | None = None,
		order_dir: str = "ASC",
	) -> dict | None:
		"""
		Return one row or None.
		"""
		rows, _ = self.get_rows_with_filters(
			table,
			equalities=equalities,
			raw_conditions=raw_conditions,
			raw_params=raw_params,
			joins=joins,
			page_limit=1,
			page_num=0,
			order_by=order_by,
			order_dir=order_dir,
		)
		return rows[0] if rows else None

	def get_one(
		self,
		table: str,
		equalities: dict | None = None,
		raw_conditions: RawCondition | sql.Composable | str | list[RawCondition | sql.Composable | str] | None = None,
		raw_params: list | None = None,
		joins: list[JoinSpec | tuple[str, str, str] | str] | None = None,
		order_by: str | None = None,
		order_dir: str = "ASC",
	) -> dict:
		"""
		Return exactly one row, raising when none or multiple rows match.
		"""
		total = self.count_rows_with_filters(
			table,
			equalities=equalities,
			raw_conditions=raw_conditions,
			raw_params=raw_params,
			joins=joins,
		)
		if total == 0:
			raise LookupError("No rows matched filters.")
		if total > 1:
			raise ValueError(f"Expected exactly one row, found {total}.")

		row = self.get_optional_one(
			table,
			equalities=equalities,
			raw_conditions=raw_conditions,
			raw_params=raw_params,
			joins=joins,
			order_by=order_by,
			order_dir=order_dir,
		)
		if row is None:
			raise LookupError("No rows matched filters.")
		return row

	# ---------- DELETE with filters/joins ----------
	def delete_rows_with_filters(
		self,
		table: str,
		equalities: dict | None = None,
		raw_conditions: RawCondition | sql.Composable | str | list[RawCondition | sql.Composable | str] | None = None,
		raw_params: list | None = None,
		joins: list[JoinSpec | tuple[str, str, str] | str] | None = None
	) -> int:
		if not equalities and not raw_conditions and not joins:
			raise ValueError("Provide at least one of 'equalities', 'raw_conditions', or 'joins'.")

		valid_columns = self._require_existing_table_columns(table)
		if equalities:
			self._validate_known_columns(valid_columns, equalities.keys(), "condition")

		using_clause, on_parts = self._build_relation_clause(joins, keyword="USING")
		where_parts, params = self._build_where_parts(
			equalities=equalities,
			raw_conditions=raw_conditions,
			raw_params=raw_params,
			extra_conditions=on_parts,
		)

		if not where_parts:
			raise ValueError("No WHERE predicates built. Refusing to delete without filters.")

		where_sql = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_parts)

		q = sql.SQL("DELETE FROM ") + self._ident_qualified(table)
		q = q + using_clause + where_sql + sql.SQL(" RETURNING 1;")

		result = self._execute(q, params)
		return len(result) if result else 0

	# ---------- UPDATE (equalities only) ----------
	def update_rows_with_equalities(self, table: str, updates: dict, equalities: dict) -> int:
		if not updates:
			raise ValueError("Updates dictionary is empty.")
		if not equalities:
			raise ValueError("Conditions dictionary is empty.")

		valid_columns = self._require_existing_table_columns(table)
		self._validate_known_columns(valid_columns, updates.keys(), "update")
		self._validate_known_columns(valid_columns, equalities.keys(), "condition")

		update_items = list(updates.items())

		set_clauses = [
			sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
			for k, _ in update_items
		]

		set_sql = sql.SQL(", ").join(set_clauses)
		set_params = [v for _, v in update_items]
		where_parts, where_params = self._build_where_parts(equalities=equalities)
		where_sql = sql.SQL(" AND ").join(where_parts)

		query = sql.SQL("UPDATE {tbl} SET {sets} WHERE {conds} RETURNING 1;").format(
			tbl=self._ident_qualified(table),
			sets=set_sql,
			conds=where_sql
		)

		result = self._execute(query, set_params + where_params)
		return len(result) if result else 0

	# ---------- UPDATE with filters/joins ----------
	def update_rows_with_filters(
		self,
		table: str,
		updates: dict,
		equalities: dict | None = None,
		raw_conditions: RawCondition | sql.Composable | str | list[RawCondition | sql.Composable | str] | None = None,
		raw_params: list | None = None,
		joins: list[JoinSpec | tuple[str, str, str] | str] | None = None
	) -> int:
		if not updates:
			raise ValueError("Updates dictionary is empty.")

		valid_columns = self._require_existing_table_columns(table)
		self._validate_known_columns(valid_columns, updates.keys(), "update")

		if equalities:
			self._validate_known_columns(valid_columns, equalities.keys(), "condition")

		update_items = list(updates.items())
		set_clauses = [
			sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
			for k, _ in update_items
		]
		set_sql = sql.SQL(", ").join(set_clauses)
		set_params = [v for _, v in update_items]

		from_clause, on_parts = self._build_relation_clause(joins, keyword="FROM")
		where_parts, params = self._build_where_parts(
			equalities=equalities,
			raw_conditions=raw_conditions,
			raw_params=raw_params,
			extra_conditions=on_parts,
		)

		if not where_parts:
			raise ValueError("No WHERE predicates built. Refusing to update without filters.")

		where_sql = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_parts)

		q = sql.SQL("UPDATE {tbl} SET {sets}").format(
			tbl=self._ident_qualified(table),
			sets=set_sql
		)
		q = q + from_clause + where_sql + sql.SQL(" RETURNING 1;")

		result = self._execute(q, set_params + params)
		return len(result) if result else 0
	
	def get_column_info(self, schema: str, table: str) -> dict[str, dict]:
		"""
		Return dict keyed by column_name with basic metadata.
		"""
		q = """
			SELECT
				column_name,
				data_type,
				udt_name,
				is_nullable,
				column_default,
				character_maximum_length,
				numeric_precision,
				numeric_scale
			FROM information_schema.columns
			WHERE table_schema = %s AND table_name = %s
			ORDER BY ordinal_position;
		"""
		rows = self._execute(q, [schema, table]) or []
		out = {}
		for r in rows:
			out[r["column_name"]] = r
		return out

	def constraint_exists(self, schema: str, table: str, constraint_name: str) -> bool:
		q = """
			SELECT 1
			FROM information_schema.table_constraints
			WHERE constraint_schema = %s
			AND table_name = %s
			AND constraint_name = %s;
		"""
		return bool(self._execute(q, [schema, table, constraint_name]))

	def add_column(self, schema: str, table: str, column: str, type_sql: str) -> None:
		q = sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
			sql.Identifier(schema),
			sql.Identifier(table),
			sql.Identifier(column),
			sql.SQL(type_sql)
		)
		self._execute(q)

	def add_constraint(self, schema: str, table: str, constraint_sql: str) -> None:
		"""
		constraint_sql should be the body after 'ADD', e.g.
		'CONSTRAINT users_email_key UNIQUE (email)'
		"""
		q = sql.SQL("ALTER TABLE {}.{} ADD {}").format(
			sql.Identifier(schema),
			sql.Identifier(table),
			sql.SQL(constraint_sql)
		)
		self._execute(q)

	def list_indexes(self, schema: str, table: str) -> list[dict]:
		q = """
			SELECT indexname, indexdef
			FROM pg_indexes
			WHERE schemaname = %s AND tablename = %s
			ORDER BY indexname;
		"""
		return self._execute(q, [schema, table]) or []

	def list_constraints(self, schema: str, table: str) -> list[dict]:
		q = """
			SELECT constraint_name, constraint_type
			FROM information_schema.table_constraints
			WHERE constraint_schema = %s AND table_name = %s
			ORDER BY constraint_name;
		"""
		return self._execute(q, [schema, table]) or []

	def get_primary_key_columns(self, schema: str, table: str) -> list[str]:
		q = """
			SELECT kcu.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
				ON tc.constraint_name = kcu.constraint_name
				AND tc.constraint_schema = kcu.constraint_schema
			WHERE tc.constraint_schema = %s
			AND tc.table_name = %s
			AND tc.constraint_type = 'PRIMARY KEY'
			ORDER BY kcu.ordinal_position;
		"""
		rows = self._execute(q, [schema, table]) or []
		return [r["column_name"] for r in rows]

	def get_constraint_columns(self, schema: str, table: str, constraint_name: str) -> list[str]:
		q = """
			SELECT kcu.column_name
			FROM information_schema.key_column_usage kcu
			WHERE kcu.constraint_schema = %s
			AND kcu.table_name = %s
			AND kcu.constraint_name = %s
			ORDER BY kcu.ordinal_position;
		"""
		rows = self._execute(q, [schema, table, constraint_name]) or []
		return [r["column_name"] for r in rows]

	def list_constraint_indexes(self, schema: str, table: str) -> list[str]:
		q = """
			SELECT i.relname AS indexname
			FROM pg_constraint c
			JOIN pg_class t ON t.oid = c.conrelid
			JOIN pg_namespace n ON n.oid = t.relnamespace
			JOIN pg_class i ON i.oid = c.conindid
			WHERE n.nspname = %s AND t.relname = %s AND c.conindid <> 0;
		"""
		rows = self._execute(q, [schema, table]) or []
		return [r["indexname"] for r in rows]
