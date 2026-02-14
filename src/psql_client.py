import logging
from math import ceil
from typing import Optional, Iterable
from psycopg2 import sql
from psycopg2.pool import ThreadedConnectionPool

logger = logging.getLogger(__name__)

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
			tuple(sorted(conn_kwargs.items())) if conn_kwargs else None,
			minconn, maxconn
		)
		if key not in cls._cache:
			cls._cache[key] = cls(
				database=database, user=user, password=password,
				host=host, port=port, minconn=minconn, maxconn=maxconn, **conn_kwargs
			)
			logger.debug("Created new cached PSQLClient for key: %s", key)
		else:
			logger.debug("Reusing cached PSQLClient for key: %s", key)
		return cls._cache[key]

	@classmethod
	def closeall(cls) -> None:
		"""Close all cached connection pools and clear the cache."""
		items = list(cls._cache.items())
		cls._cache.clear()
		for _, client in items:
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
		try:
			self.pool.closeall()
		except Exception:
			logger.exception("Error closing connection pool")

	def _get_conn(self):
		return self.pool.getconn()

	def _put_conn(self, conn):
		self.pool.putconn(conn)

	# ---------- Execution helpers ----------
	def _execute(self, query, params: Optional[Iterable] = None) -> list[dict] | None:
		"""
		Executes SQL (string or psycopg2.sql Composable).
		Returns list[dict] for result sets, otherwise None.
		Commits on success; rolls back on exception.
		"""
		conn = self._get_conn()
		try:
			if not isinstance(query, str):
				query = query.as_string(conn)
			with conn.cursor() as cur:
				cur.execute(query, list(params or []))
				has_result = cur.description is not None
				if has_result:
					colnames = [d[0] for d in cur.description]
					rows = cur.fetchall()
					# If it's DML with RETURNING, this commit covers the write
					conn.commit()
					return [dict(zip(colnames, r)) for r in rows]
				else:
					conn.commit()
					return None
		except Exception:
			conn.rollback()
			raise
		finally:
			self._put_conn(conn)

	def execute_query(self, query, params: Optional[Iterable] = None) -> list[dict] | None:
		"""
		Public wrapper for executing raw SQL (read or write).
		"""
		return self._execute(query, params)

	def _execute_autocommit(self, query, params: Optional[Iterable] = None):
		"""
		Execute a statement that must run outside a transaction (e.g., CREATE/DROP DATABASE).
		Returns None or list[dict] like _execute.
		"""
		conn = self._get_conn()
		prev = getattr(conn, "autocommit", False)
		try:
			if not isinstance(query, str):
				query = query.as_string(conn)
			conn.autocommit = True
			with conn.cursor() as cur:
				cur.execute(query, list(params or []))
				if cur.description:
					names = [d[0] for d in cur.description]
					rows = cur.fetchall()
					return [dict(zip(names, r)) for r in rows]
				return None
		finally:
			conn.autocommit = prev
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

	def get_table_columns(self, schema: str, table: str) -> list[str]:
		q = """
			SELECT column_name
			FROM information_schema.columns
			WHERE table_schema = %s AND table_name = %s
			ORDER BY ordinal_position;
		"""
		rows = self._execute(q, [schema, table]) or []
		return [r["column_name"] for r in rows]

	def column_exists(self, schema: str, table: str, column: str) -> bool:
		q = """
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = %s AND table_name = %s AND column_name = %s;
		"""
		return bool(self._execute(q, [schema, table, column]))

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
			if isinstance(query, str):
				base = query.strip().rstrip(';')
			else:
				base = query.as_string(conn).strip().rstrip(';')

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
		finally:
			self._put_conn(conn)

		return self._execute(final_sql, params + [limit, offset]) or []

	# ---------- Column discovery for base table ----------
	def _get_table_columns(self, table: str) -> list[str]:
		schema, tbl = self._split_qualified(table)
		if schema:
			q = """
				SELECT column_name
				FROM information_schema.columns
				WHERE table_schema = %s AND table_name = %s
				ORDER BY ordinal_position;
			"""
			rows = self._execute(q, [schema, tbl]) or []
		else:
			q = """
				SELECT column_name
				FROM information_schema.columns
				WHERE table_name = %s
				ORDER BY ordinal_position;
			"""
			rows = self._execute(q, [tbl]) or []
		return [r["column_name"] for r in rows]

	# ---------- Unified SELECT with filters/joins/paging ----------
	def get_rows_with_filters(
		self,
		table: str,
		equalities: dict | None = None,
		raw_conditions: str | list[str] | None = None,
		raw_params: list | None = None,
		joins: list[tuple[str, str, str]] | None = None,
		page_limit: int = 50,
		page_num: int = 0,
		order_by: str | None = None,
		order_dir: str = "ASC"
	) -> tuple[list[dict], int]:
		
		if not isinstance(page_limit, int) or page_limit <= 0:
			raise ValueError("page_limit must be a positive integer.")
		if not isinstance(page_num, int) or page_num < 0:
			raise ValueError("page_num must be a non-negative integer.")

		valid_columns = self._get_table_columns(table)
		if not valid_columns:
			raise ValueError(f"Table '{table}' does not exist.")

		if equalities:
			invalid = [k for k in equalities if k not in valid_columns]
			if invalid:
				raise ValueError(f"Invalid columns for condition: {invalid}")

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

		from_clause = sql.SQL(" FROM ") + self._ident_qualified(table)
		if joins:
			for j in joins:
				if isinstance(j, str):
					from_clause += sql.SQL(" ") + sql.SQL(j)
				else:
					try:
						join_type, table_expr, on_clause = j
					except Exception:
						raise ValueError("joins entries must be either raw JOIN strings or 3-tuples (join_type, table_expr, on_clause).")
					from_clause += sql.SQL(" {} {} ON {}").format(
						sql.SQL(join_type),
						sql.SQL(table_expr),
						sql.SQL(on_clause)
					)

		where_parts = []
		params = []

		if equalities:
			items = list(equalities.items())
			where_parts.extend(
				sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
				for k, _ in items
			)
			params.extend(v for _, v in items)

		if raw_conditions:
			if isinstance(raw_conditions, str):
				where_parts.append(sql.SQL(raw_conditions))
			else:
				for frag in raw_conditions:
					where_parts.append(sql.SQL(frag))
			if raw_params:
				params.extend(list(raw_params))

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

	# ---------- DELETE with filters/joins ----------
	def delete_rows_with_filters(
		self,
		table: str,
		equalities: dict | None = None,
		raw_conditions: str | list[str] | None = None,
		raw_params: list | None = None,
		joins: list[tuple[str, str, str]] | None = None
	) -> int:
		if not equalities and not raw_conditions:
			raise ValueError("Provide at least one of 'equalities' or 'raw_conditions'.")

		valid_columns = self._get_table_columns(table)
		if not valid_columns:
			raise ValueError(f"Table '{table}' does not exist.")
		if equalities:
			invalid = [k for k in equalities if k not in valid_columns]
			if invalid:
				raise ValueError(f"Invalid columns for condition: {invalid}")

		using_clause = sql.SQL("")
		on_parts = []
		if joins:
			using_bits = []
			for _join_type, table_expr, on_clause in joins:
				using_bits.append(sql.SQL(table_expr))
				on_parts.append(sql.SQL(on_clause))
			if using_bits:
				using_clause = sql.SQL(" USING ") + sql.SQL(", ").join(using_bits)

		where_parts = []
		params = []

		if equalities:
			items = list(equalities.items())
			where_parts.extend(
				sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
				for k, _ in items
			)
			params.extend(v for _, v in items)

		where_parts.extend(on_parts)

		if raw_conditions:
			if isinstance(raw_conditions, str):
				where_parts.append(sql.SQL(raw_conditions))
			else:
				where_parts.extend(sql.SQL(frag) for frag in raw_conditions)
			if raw_params:
				params.extend(list(raw_params))

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

		valid_columns = self._get_table_columns(table)
		if not valid_columns:
			raise ValueError(f"Table '{table}' does not exist.")

		invalid_updates = [k for k in updates if k not in valid_columns]
		if invalid_updates:
			raise ValueError(f"Invalid columns for update: {invalid_updates}")

		invalid_conditions = [k for k in equalities if k not in valid_columns]
		if invalid_conditions:
			raise ValueError(f"Invalid columns for condition: {invalid_conditions}")

		update_items = list(updates.items())
		where_items = list(equalities.items())

		set_clauses = [
			sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
			for k, _ in update_items
		]
		where_clauses = [
			sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
			for k, _ in where_items
		]

		set_sql = sql.SQL(", ").join(set_clauses)
		where_sql = sql.SQL(" AND ").join(where_clauses)

		set_params = [v for _, v in update_items]
		where_params = [v for _, v in where_items]

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
		raw_conditions: str | list[str] | None = None,
		raw_params: list | None = None,
		joins: list[tuple[str, str, str]] | None = None
	) -> int:
		if not updates:
			raise ValueError("Updates dictionary is empty.")

		valid_columns = self._get_table_columns(table)
		if not valid_columns:
			raise ValueError(f"Table '{table}' does not exist.")

		invalid_updates = [k for k in updates if k not in valid_columns]
		if invalid_updates:
			raise ValueError(f"Invalid columns for update: {invalid_updates}")

		if equalities:
			invalid_conds = [k for k in equalities if k not in valid_columns]
			if invalid_conds:
				raise ValueError(f"Invalid columns for condition: {invalid_conds}")

		update_items = list(updates.items())
		set_clauses = [
			sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
			for k, _ in update_items
		]
		set_sql = sql.SQL(", ").join(set_clauses)
		set_params = [v for _, v in update_items]

		from_clause = sql.SQL("")
		on_parts = []
		if joins:
			from_bits = []
			for _join_type, table_expr, on_clause in joins:
				from_bits.append(sql.SQL(table_expr))
				on_parts.append(sql.SQL(on_clause))
			if from_bits:
				from_clause = sql.SQL(" FROM ") + sql.SQL(", ").join(from_bits)

		where_parts = []
		params = []

		if equalities:
			where_items = list(equalities.items())
			where_parts.extend(
				sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
				for k, _ in where_items
			)
			params.extend(v for _, v in where_items)

		where_parts.extend(on_parts)

		if raw_conditions:
			if isinstance(raw_conditions, str):
				where_parts.append(sql.SQL(raw_conditions))
			else:
				where_parts.extend(sql.SQL(frag) for frag in raw_conditions)
			if raw_params:
				params.extend(list(raw_params))

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
