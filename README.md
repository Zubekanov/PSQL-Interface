# PSQL Interface

`src/psql_client.py` is a pooled PostgreSQL utility layer built on `psycopg2` and `ThreadedConnectionPool`.

It provides:
- safe-ish dynamic SQL composition through `psycopg2.sql`
- connection pooling + cached client reuse
- database/schema/table/index/constraint helpers
- higher-level CRUD helpers with validated filters, joins, and pagination
- typed filter primitives (`JoinSpec`, `RawCondition`)

## What's New In The Refactor

- Added `JoinSpec` and `RawCondition` for structured joins and raw filter fragments.
- Added cache locking and safer client lifecycle handling (`close()` is idempotent and evicts cached instance).
- Unified execution internals through `_execute_on_conn(...)`.
- Consolidated repeated filter/join builder logic into shared internal helpers.
- Unified column lookup internals with `_fetch_table_columns(...)`.

## Quick Start

```python
from src.psql_client import PSQLClient, JoinSpec, RawCondition

client = PSQLClient.get(
    database="mydb",
    user="postgres",
    password="secret",
    host="localhost",
    port=5432,
)

rows, total_pages = client.get_rows_with_filters(
    "public.users",
    equalities={"active": True},
    joins=[JoinSpec("LEFT JOIN", "public.roles r", "r.id = users.role_id")],
    raw_conditions=[RawCondition("users.created_at >= %s", ("2025-01-01",))],
    page_limit=25,
    page_num=0,
    order_by="id",
)

PSQLClient.closeall()
```

## Data Structures

### `JoinSpec`

```python
JoinSpec(join_type: str, table_expr: str, on_clause: str)
```

Represents one SQL join segment used by filter APIs.
- `join_type`: e.g. `"JOIN"`, `"LEFT JOIN"`, `"INNER JOIN"`
- `table_expr`: table expression after `JOIN`
- `on_clause`: SQL condition after `ON`

### `RawCondition`

```python
RawCondition(expression: str, params: tuple[Any, ...] = ())
```

Represents one raw SQL predicate and its bound values.
- `expression`: SQL fragment to insert into `WHERE`
- `params`: positional values consumed by placeholders in `expression`

## API Reference

Private methods are included because they are part of the current implementation, but their signatures are less stable than public methods.

### Class: `PSQLClient`

#### Class methods

`get(cls, *, database: str = "postgres", user: str = "postgres", password: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None, minconn: int = 1, maxconn: int = 10, **conn_kwargs) -> "PSQLClient"`  
Returns a cached client for the same connection/pool configuration. If cached instance is closed or missing, creates a fresh one.

`closeall(cls) -> None`  
Closes all cached pools and clears the class-level cache.

#### Lifecycle and pool plumbing

`__init__(self, *, database: str = "postgres", user: str = "postgres", password: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None, minconn: int = 1, maxconn: int = 10, **conn_kwargs)`  
Creates a `ThreadedConnectionPool`, stores connection metadata for debugging, and initializes close/cache state.

`__repr__(self) -> str`  
Returns a concise debug representation including connection target and pool min/max sizing.

`close(self) -> None`  
Closes this instance's pool once, marks the instance closed, and removes it from cache if it came from `get(...)`.

`_get_conn(self)`  
Internal pool checkout. Raises if the client has already been closed.

`_put_conn(self, conn)`  
Internal pool return. Suppresses pool-return errors only when closure race conditions are expected.

#### Execution internals

`_freeze_conn_kwargs(conn_kwargs: dict[str, Any]) -> tuple[tuple[str, Any], ...] | None`  
Converts extra connection kwargs to a deterministic hashable form for cache keys.

`_normalize_query(conn, query) -> str`  
Normalizes query input to SQL text (`str` or `psycopg2.sql` composable).

`_rows_from_cursor(cur) -> list[dict] | None`  
Maps cursor results into `list[dict]` keyed by column name, or `None` when no result set exists.

`_execute_on_conn(self, conn, query, params: Optional[Iterable] = None, *, autocommit: bool = False) -> list[dict] | None`  
Core executor used by all SQL paths. Handles autocommit mode, commits/rollbacks, and row materialization.

`_execute(self, query, params: Optional[Iterable] = None) -> list[dict] | None`  
Transactional executor: commit on success, rollback on failure.

`execute_query(self, query, params: Optional[Iterable] = None) -> list[dict] | None`  
Public generic SQL entrypoint that delegates to `_execute(...)`.

`_execute_autocommit(self, query, params: Optional[Iterable] = None) -> list[dict] | None`  
Autocommit executor for statements that must run outside transaction blocks.

#### Database helpers

`database_exists(self, db_name: str) -> bool`  
Checks whether a database name exists in `pg_database`.

`create_database(self, db_name: str, exists_ok: bool = True) -> bool`  
Creates a database. Returns `False` when it already exists and `exists_ok=True`; otherwise returns `True`.

`drop_database(self, db_name: str, missing_ok: bool = True) -> bool`  
Drops a database (using `DROP DATABASE IF EXISTS`). Can enforce existence when `missing_ok=False`.

#### Schema helpers

`schema_exists(self, schema: str) -> bool`  
Checks whether a schema exists in `pg_namespace`.

`list_schemas(self, exclude_system: bool = True) -> list[str]`  
Lists schema names, optionally excluding system/internal schemas.

`create_schema(self, schema: str, exists_ok: bool = True) -> None`  
Creates a schema with optional `IF NOT EXISTS` behavior.

`drop_schema(self, schema: str, cascade: bool = False, missing_ok: bool = True) -> None`  
Drops a schema with configurable `CASCADE` and missing-schema tolerance.

`ensure_schema(self, schema: str) -> None`  
Idempotent convenience wrapper around `create_schema(..., exists_ok=True)`.

#### Table/column helpers

`table_exists(self, schema: str, table: str) -> bool`  
Checks whether a base table exists in a specific schema.

`list_tables(self, schema: str = "public") -> list[str]`  
Lists base tables in a schema ordered by name.

`_fetch_table_columns(self, schema: str | None, table: str) -> list[str]`  
Internal column discovery with optional schema scope.

`get_table_columns(self, schema: str, table: str) -> list[str]`  
Returns ordered column names for an explicit `schema.table`.

`column_exists(self, schema: str, table: str, column: str) -> bool`  
Checks whether a specific column exists in a table.

`_require_existing_table_columns(self, table: str) -> list[str]`  
Internal guard for qualified/unqualified table names; raises if table is missing.

`_validate_known_columns(valid_columns: Sequence[str], columns: Iterable[str], label: str) -> None`  
Internal validator to reject unknown columns in update/filter inputs.

`create_table(self, schema: str, table: str, columns: dict[str, str], constraints: list[str] | None = None, if_not_exists: bool = True, temporary: bool = False) -> None`  
Creates a table from column definitions + optional table constraints. Supports `TEMP` and `IF NOT EXISTS`.

`drop_table(self, schema: str, table: str, cascade: bool = False, missing_ok: bool = True) -> None`  
Drops a table with optional `CASCADE` and missing-table tolerance.

`ensure_table(self, schema: str, table: str, columns: dict[str, str], constraints: list[str] | None = None) -> None`  
Creates a table only if needed (`IF NOT EXISTS`).

`drop_column(self, schema: str, table: str, column: str, *, cascade: bool = False, missing_ok: bool = True) -> None`  
Drops a column with optional `CASCADE` and missing-column tolerance.

`alter_column_type(self, schema: str, table: str, column: str, type_sql: str, *, using: str | None = None) -> None`  
Alters column type and optionally applies a `USING` conversion expression.

`alter_column_nullability(self, schema: str, table: str, column: str, *, nullable: bool) -> None`  
Sets nullability (`DROP NOT NULL` when `nullable=True`; otherwise `SET NOT NULL`).

`alter_column_default(self, schema: str, table: str, column: str, *, default_sql: str | None = None, drop: bool = False) -> None`  
Sets or removes column default expression.

`add_column(self, schema: str, table: str, column: str, type_sql: str) -> None`  
Adds a new column with provided SQL type/expression.

#### Index/constraint helpers

`index_exists(self, schema: str, index_name: str) -> bool`  
Checks whether an index exists in a schema.

`create_index(self, schema: str, table: str, index_name: str, columns: list[str], unique: bool = False, if_not_exists: bool = True) -> None`  
Creates an index on one or more columns with optional uniqueness and existence guard.

`drop_index(self, schema: str, index_name: str, missing_ok: bool = True) -> None`  
Drops an index with optional missing-index tolerance.

`drop_constraint(self, schema: str, table: str, constraint_name: str, *, missing_ok: bool = True) -> None`  
Drops a named table constraint with optional `IF EXISTS`.

`constraint_exists(self, schema: str, table: str, constraint_name: str) -> bool`  
Checks whether a named constraint exists on a table.

`add_constraint(self, schema: str, table: str, constraint_sql: str) -> None`  
Adds a constraint using raw SQL body (the fragment after `ADD`).

`list_indexes(self, schema: str, table: str) -> list[dict]`  
Returns index names and definitions from `pg_indexes`.

`list_constraints(self, schema: str, table: str) -> list[dict]`  
Returns table constraints and their types.

`get_primary_key_columns(self, schema: str, table: str) -> list[str]`  
Returns ordered primary key columns.

`get_constraint_columns(self, schema: str, table: str, constraint_name: str) -> list[str]`  
Returns ordered columns belonging to a specific constraint.

`list_constraint_indexes(self, schema: str, table: str) -> list[str]`  
Lists index objects that back constraints.

#### Name and SQL composition helpers

`_split_qualified(self, qname) -> tuple[Optional[str], str]`  
Parses `schema.table`, `table`, or tuple/list forms into `(schema_or_none, table)`.

`_ident_qualified(self, qname) -> sql.Composed`  
Builds a correctly quoted identifier for optional schema-qualified names.

`_normalize_join_spec(self, join_entry) -> JoinSpec`  
Normalizes join definitions from `JoinSpec`, tuple/list, or SQL string (`... JOIN ... ON ...`) into one `JoinSpec`.

`_normalize_joins(self, joins) -> list[JoinSpec]`  
Normalizes join collection input for downstream builders.

`_build_select_from_clause(self, table: str, joins) -> sql.Composable`  
Builds `FROM` + explicit `JOIN` clauses for SELECT-like queries.

`_build_relation_clause(self, joins, keyword: str) -> tuple[sql.Composable, list[sql.Composable]]`  
Builds relation clause (`FROM` or `USING`) and returns associated join `ON` predicates separately.

`_normalize_raw_conditions(raw_conditions, raw_params: list | tuple | None = None) -> tuple[list[sql.Composable], list]`  
Normalizes raw conditions from `str`, `RawCondition`, or composables into SQL fragments + param list.

`_build_where_parts(self, *, equalities: dict | None = None, raw_conditions=None, raw_params: list | tuple | None = None, extra_conditions: list[sql.Composable] | None = None) -> tuple[list[sql.Composable], list]`  
Builds combined WHERE predicates and positional params from equality filters, raw fragments, and join predicates.

`_get_table_columns(self, table: str) -> list[str]`  
Column discovery wrapper that accepts qualified or unqualified table references.

#### Data access methods

`insert_row(self, table: str, data: dict) -> dict | None`  
Inserts one record and returns inserted row (`RETURNING *`).

`_paged_execute(self, query, params: list | None = None, page_limit: int = 50, page_num: int = 0, order_by: str | None = None, order_dir: str = "ASC", tiebreaker: str | None = None, base_qualifier: str | sql.Composable | None = None) -> list[dict]`  
Executes a `SELECT`/`WITH` query with validated `ORDER BY`, deterministic optional tiebreaker, and `LIMIT/OFFSET`.

`get_rows_with_filters(self, table: str, equalities: dict | None = None, raw_conditions: RawCondition | sql.Composable | str | list[RawCondition | sql.Composable | str] | None = None, raw_params: list | None = None, joins: list[JoinSpec | tuple[str, str, str] | str] | None = None, page_limit: int = 50, page_num: int = 0, order_by: str | None = None, order_dir: str = "ASC") -> tuple[list[dict], int]`  
Performs filtered SELECT with optional joins + raw predicates. Returns `(rows, total_pages)` using a separate count query.

`delete_rows_with_filters(self, table: str, equalities: dict | None = None, raw_conditions: RawCondition | sql.Composable | str | list[RawCondition | sql.Composable | str] | None = None, raw_params: list | None = None, joins: list[JoinSpec | tuple[str, str, str] | str] | None = None) -> int`  
Deletes rows with equality filters, raw conditions, and/or join predicates. Refuses to run if no effective WHERE predicates are built.

`update_rows_with_equalities(self, table: str, updates: dict, equalities: dict) -> int`  
Updates rows with strict equality predicates only. Returns number of affected rows.

`update_rows_with_filters(self, table: str, updates: dict, equalities: dict | None = None, raw_conditions: RawCondition | sql.Composable | str | list[RawCondition | sql.Composable | str] | None = None, raw_params: list | None = None, joins: list[JoinSpec | tuple[str, str, str] | str] | None = None) -> int`  
Flexible update path supporting equality filters, raw predicates, and join-based predicates. Returns affected row count.

`get_column_info(self, schema: str, table: str) -> dict[str, dict]`  
Returns detailed column metadata keyed by `column_name` from `information_schema.columns`.

