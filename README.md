# PSQL Interface Overview

`src/psql_client.py` provides a `PSQLClient` class: a thread-safe PostgreSQL helper built on `psycopg2` and `ThreadedConnectionPool`.

## High-Level Functionality

- Manages pooled PostgreSQL connections for concurrent use.
- Supports cached client reuse with `PSQLClient.get(...)` and global cleanup with `PSQLClient.closeall()`.
- Executes SQL with transaction handling (commit on success, rollback on failure).
- Runs database-level statements that require autocommit (for example, `CREATE DATABASE` / `DROP DATABASE`).
- Uses `psycopg2.sql` composition for safe identifier handling when building SQL dynamically.

## Core Capabilities

- Database helpers: check, create, and drop databases.
- Schema helpers: check, list, create, drop, and ensure schemas.
- Table/column helpers: check/list tables and columns, create/drop tables, add/drop columns, and alter column type/nullability/defaults.
- Index and constraint helpers: create/drop/check indexes, add/drop/check constraints, and inspect constraint-related metadata.
- Metadata inspection: column info, primary key columns, constraint columns, and index/constraint listings.

## API Reference (High-Level)

### Classes

- `class PSQLClient`: Thread-safe PostgreSQL client that wraps a `ThreadedConnectionPool` and provides schema/table/query utilities.

### Class Methods

- `get(cls, *, database: str = "postgres", user: str = "postgres", password: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None, minconn: int = 1, maxconn: int = 10, **conn_kwargs) -> "PSQLClient"`: Returns a cached client for matching connection parameters, creating one if needed.
- `closeall(cls) -> None`: Closes all cached client pools and clears the cache.

### Lifecycle and Pool Methods

- `__init__(self, *, database: str = "postgres", user: str = "postgres", password: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None, minconn: int = 1, maxconn: int = 10, **conn_kwargs)`: Stores connection settings and creates the connection pool.
- `__repr__(self) -> str`: Returns a debug-friendly string showing user/host/database and pool sizing.
- `close(self) -> None`: Closes this instance's connection pool.
- `_get_conn(self)`: Retrieves a single connection from the pool.
- `_put_conn(self, conn)`: Returns a connection to the pool.

### Execution Methods

- `_execute(self, query, params: Optional[Iterable] = None) -> list[dict] | None`: Executes SQL in a transaction and returns rows as `list[dict]` when a result set exists.
- `execute_query(self, query, params: Optional[Iterable] = None) -> list[dict] | None`: Public wrapper for `_execute` for raw SQL reads/writes.
- `_execute_autocommit(self, query, params: Optional[Iterable] = None) -> list[dict] | None`: Executes SQL with autocommit enabled for statements that cannot run inside a transaction.

### Database Helpers

- `database_exists(self, db_name: str) -> bool`: Checks whether a database exists.
- `create_database(self, db_name: str, exists_ok: bool = True) -> bool`: Creates a database and optionally no-ops if it already exists.
- `drop_database(self, db_name: str, missing_ok: bool = True) -> bool`: Drops a database, optionally validating existence first.

### Schema Helpers

- `schema_exists(self, schema: str) -> bool`: Checks whether a schema exists.
- `list_schemas(self, exclude_system: bool = True) -> list[str]`: Lists schema names, optionally excluding system schemas.
- `create_schema(self, schema: str, exists_ok: bool = True) -> None`: Creates a schema with optional `IF NOT EXISTS` behavior.
- `drop_schema(self, schema: str, cascade: bool = False, missing_ok: bool = True) -> None`: Drops a schema with optional cascade and missing guard.
- `ensure_schema(self, schema: str) -> None`: Ensures a schema exists (idempotent create).

### Table and Column Helpers

- `table_exists(self, schema: str, table: str) -> bool`: Checks whether a table exists in a schema.
- `list_tables(self, schema: str = "public") -> list[str]`: Lists base tables in a schema.
- `get_table_columns(self, schema: str, table: str) -> list[str]`: Lists ordered column names for a table.
- `column_exists(self, schema: str, table: str, column: str) -> bool`: Checks whether a specific column exists.
- `create_table(self, schema: str, table: str, columns: dict[str, str], constraints: list[str] | None = None, if_not_exists: bool = True, temporary: bool = False) -> None`: Creates a table from column definitions and optional table constraints.
- `drop_table(self, schema: str, table: str, cascade: bool = False, missing_ok: bool = True) -> None`: Drops a table with optional cascade and missing guard.
- `ensure_table(self, schema: str, table: str, columns: dict[str, str], constraints: list[str] | None = None) -> None`: Ensures a table exists by creating it with `IF NOT EXISTS`.
- `drop_column(self, schema: str, table: str, column: str, *, cascade: bool = False, missing_ok: bool = True) -> None`: Drops a column with optional cascade and missing guard.
- `alter_column_type(self, schema: str, table: str, column: str, type_sql: str, *, using: str | None = None) -> None`: Changes a column type with optional `USING` expression.
- `alter_column_nullability(self, schema: str, table: str, column: str, *, nullable: bool) -> None`: Sets or drops `NOT NULL` on a column.
- `alter_column_default(self, schema: str, table: str, column: str, *, default_sql: str | None = None, drop: bool = False) -> None`: Sets or drops a column default.
- `add_column(self, schema: str, table: str, column: str, type_sql: str) -> None`: Adds a new column to a table.

### Index and Constraint Helpers

- `index_exists(self, schema: str, index_name: str) -> bool`: Checks whether an index exists.
- `create_index(self, schema: str, table: str, index_name: str, columns: list[str], unique: bool = False, if_not_exists: bool = True) -> None`: Creates an index on one or more columns.
- `drop_index(self, schema: str, index_name: str, missing_ok: bool = True) -> None`: Drops an index with optional missing guard.
- `drop_constraint(self, schema: str, table: str, constraint_name: str, *, missing_ok: bool = True) -> None`: Drops a table constraint.
- `constraint_exists(self, schema: str, table: str, constraint_name: str) -> bool`: Checks whether a constraint exists.
- `add_constraint(self, schema: str, table: str, constraint_sql: str) -> None`: Adds a table constraint from raw SQL fragment.
- `list_indexes(self, schema: str, table: str) -> list[dict]`: Lists indexes and definitions for a table.
- `list_constraints(self, schema: str, table: str) -> list[dict]`: Lists constraints and types for a table.
- `get_primary_key_columns(self, schema: str, table: str) -> list[str]`: Returns ordered primary key columns.
- `get_constraint_columns(self, schema: str, table: str, constraint_name: str) -> list[str]`: Returns ordered columns for a named constraint.
- `list_constraint_indexes(self, schema: str, table: str) -> list[str]`: Lists index names backing table constraints.

### Name and SQL-Building Helpers (Internal)

- `_split_qualified(self, qname) -> tuple[Optional[str], str]`: Parses `schema.table`, `table`, or tuple input into `(schema_or_none, table)`.
- `_ident_qualified(self, qname) -> sql.Composed`: Builds a properly quoted optional schema-qualified identifier.
- `_get_table_columns(self, table: str) -> list[str]`: Internal column lookup for qualified/unqualified table names.
- `_paged_execute(self, query, params: list | None = None, page_limit: int = 50, page_num: int = 0, order_by: str | None = None, order_dir: str = "ASC", tiebreaker: str | None = None, base_qualifier: str | sql.Composable | None = None) -> list[dict]`: Applies validated ordering and `LIMIT/OFFSET` pagination to `SELECT`/`WITH` queries.

### Data Access Methods

- `insert_row(self, table: str, data: dict) -> dict | None`: Inserts one row and returns the inserted record.
- `get_rows_with_filters(self, table: str, equalities: dict | None = None, raw_conditions: str | list[str] | None = None, raw_params: list | None = None, joins: list[tuple[str, str, str]] | None = None, page_limit: int = 50, page_num: int = 0, order_by: str | None = None, order_dir: str = "ASC") -> tuple[list[dict], int]`: Executes filtered/paginated reads and returns `(rows, total_pages)`.
- `delete_rows_with_filters(self, table: str, equalities: dict | None = None, raw_conditions: str | list[str] | None = None, raw_params: list | None = None, joins: list[tuple[str, str, str]] | None = None) -> int`: Deletes rows matching filters and returns affected row count.
- `update_rows_with_equalities(self, table: str, updates: dict, equalities: dict) -> int`: Updates rows using equality predicates only; returns affected row count.
- `update_rows_with_filters(self, table: str, updates: dict, equalities: dict | None = None, raw_conditions: str | list[str] | None = None, raw_params: list | None = None, joins: list[tuple[str, str, str]] | None = None) -> int`: Updates rows using flexible predicates/joins; returns affected row count.
- `get_column_info(self, schema: str, table: str) -> dict[str, dict]`: Returns per-column metadata keyed by column name.
