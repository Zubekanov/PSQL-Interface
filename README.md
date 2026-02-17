## Data Structures

### `JoinSpec`
```python
JoinSpec(join_type: str, table_expr: str, on_clause: str)
```
Short description:
Structured join definition used by filter APIs.

Parameters:
- `join_type`: Join keyword, e.g. `"JOIN"`, `"LEFT JOIN"`, `"INNER JOIN"`.
- `table_expr`: Table expression after join keyword (can include alias).
- `on_clause`: Raw SQL condition after `ON`.

Returns:
- Dataclass instance describing one join segment.

### `RawCondition`
```python
RawCondition(expression: str, params: tuple[Any, ...] = ())
```
Short description:
Structured raw SQL `WHERE` fragment with bound parameters.

Parameters:
- `expression`: SQL fragment, e.g. `"users.created_at >= %s"`.
- `params`: Positional values used by placeholders in `expression`.

Returns:
- Dataclass instance describing one raw predicate.

## `PSQLClient` API

### Class Methods

#### `get(...)`
```python
PSQLClient.get(
    *,
    database: str = "postgres",
    user: str = "postgres",
    password: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    minconn: int = 1,
    maxconn: int = 10,
    **conn_kwargs,
) -> PSQLClient
```
Short description:
Get a cached pooled client for the same connection settings (or create a new one).

Parameters:
- `database`: Target database name.
- `user`: Database user.
- `password`: User password.
- `host`: Hostname/address.
- `port`: Port number.
- `minconn`: Minimum pool size.
- `maxconn`: Maximum pool size.
- `conn_kwargs`: Extra psycopg2 connection kwargs (`sslmode`, `options`, etc.).

Returns:
- `PSQLClient`: Reusable client instance.

#### `closeall()`
```python
PSQLClient.closeall() -> None
```
Short description:
Close all cached connection pools and clear client cache.

Parameters:
- None.

Returns:
- `None`.

### Lifecycle and Execution

#### `close()`
```python
close() -> None
```
Short description:
Close this client pool and mark instance closed.

Parameters:
- None.

Returns:
- `None`.

#### `transaction()`
```python
transaction() -> context manager
```
Short description:
Open a transaction scope on one pooled connection.

Parameters:
- None.

Returns:
- Context manager yielding `tx` with:
  - `tx.execute(query, params=None) -> list[dict] | None`

#### `execute_query(...)`
```python
execute_query(query, params: Optional[Iterable] = None) -> list[dict] | None
```
Short description:
Execute raw SQL (read/write) using standard commit/rollback behavior.

Parameters:
- `query`: SQL string or psycopg2 composable query.
- `params`: Positional bind parameters.

Returns:
- `list[dict] | None`: Rows for row-returning SQL; otherwise `None`.

### Read Operations

#### `get_rows_with_filters(...)`
```python
get_rows_with_filters(
    table: str,
    equalities: dict | None = None,
    raw_conditions: RawCondition | sql.Composable | str | list[RawCondition | sql.Composable | str] | None = None,
    raw_params: list | None = None,
    joins: list[JoinSpec | tuple[str, str, str] | str] | None = None,
    page_limit: int = 50,
    page_num: int = 0,
    order_by: str | None = None,
    order_dir: str = "ASC",
) -> tuple[list[dict], int]
```
Short description:
Run filtered, joined, paginated `SELECT *` query.

Parameters:
- `table`: Base table name. Format: `"table"` or `"schema.table"`.
- `equalities`: Exact-match filters. Format example: `{"active": True, "role_id": 2}`.
- `raw_conditions`: Extra SQL predicates as `RawCondition`, SQL string/composable, or list of them.
- `raw_params`: Extra bind params appended after params from `raw_conditions`.
- `joins`: Joins as `JoinSpec`, 3-tuples `(join_type, table_expr, on_clause)`, or join SQL strings.
- `page_limit`: Rows per page; positive integer.
- `page_num`: Zero-based page index.
- `order_by`: Column name for sort.
- `order_dir`: `"ASC"` or `"DESC"`.

Returns:
- `tuple[list[dict], int]`: `(rows, total_pages)`.

#### `count_rows_with_filters(...)`
```python
count_rows_with_filters(
    table: str,
    equalities: dict | None = None,
    raw_conditions: RawCondition | sql.Composable | str | list[RawCondition | sql.Composable | str] | None = None,
    raw_params: list | None = None,
    joins: list[JoinSpec | tuple[str, str, str] | str] | None = None,
) -> int
```
Short description:
Count rows matching filter/join criteria.

Parameters:
- Same filter arguments as `get_rows_with_filters(...)`.

Returns:
- `int`: Matching row count.

#### `exists_with_filters(...)`
```python
exists_with_filters(
    table: str,
    equalities: dict | None = None,
    raw_conditions: RawCondition | sql.Composable | str | list[RawCondition | sql.Composable | str] | None = None,
    raw_params: list | None = None,
    joins: list[JoinSpec | tuple[str, str, str] | str] | None = None,
) -> bool
```
Short description:
Check if at least one row matches filters.

Parameters:
- Same filter arguments as `get_rows_with_filters(...)`.

Returns:
- `bool`: `True` if one or more matches exist.

#### `get_optional_one(...)`
```python
get_optional_one(
    table: str,
    equalities: dict | None = None,
    raw_conditions: RawCondition | sql.Composable | str | list[RawCondition | sql.Composable | str] | None = None,
    raw_params: list | None = None,
    joins: list[JoinSpec | tuple[str, str, str] | str] | None = None,
    order_by: str | None = None,
    order_dir: str = "ASC",
) -> dict | None
```
Short description:
Return first matching row or `None`.

Parameters:
- Same filter/sort arguments as `get_rows_with_filters(...)`.

Returns:
- `dict | None`: First row as dict if found, else `None`.

#### `get_one(...)`
```python
get_one(
    table: str,
    equalities: dict | None = None,
    raw_conditions: RawCondition | sql.Composable | str | list[RawCondition | sql.Composable | str] | None = None,
    raw_params: list | None = None,
    joins: list[JoinSpec | tuple[str, str, str] | str] | None = None,
    order_by: str | None = None,
    order_dir: str = "ASC",
) -> dict
```
Short description:
Require exactly one matching row.

Parameters:
- Same filter/sort arguments as `get_rows_with_filters(...)`.

Returns:
- `dict`: The single matching row.

Notes:
- Raises when zero or multiple rows match.

### Write Operations

#### `insert_row(...)`
```python
insert_row(table: str, data: dict) -> dict | None
```
Short description:
Insert one row with `RETURNING *`.

Parameters:
- `table`: Target table (`"table"` or `"schema.table"`).
- `data`: Column-value map. Format example: `{"email": "a@x.com", "active": True}`.

Returns:
- `dict | None`: Inserted row if returned, else `None`.

#### `bulk_insert(...)`
```python
bulk_insert(
    table: str,
    rows: Sequence[dict],
    *,
    chunk_size: int = 500,
    returning: bool = False,
    do_nothing: bool = False,
    conflict_columns: Sequence[str] | None = None,
) -> list[dict] | int
```
Short description:
Insert multiple rows in chunks.

Parameters:
- `table`: Target table.
- `rows`: Sequence of row dicts; all rows must share identical keys.
- `chunk_size`: Rows per SQL statement.
- `returning`: If `True`, returns inserted rows; otherwise returns affected count.
- `do_nothing`: If `True`, uses `ON CONFLICT DO NOTHING`.
- `conflict_columns`: Optional conflict target columns for `DO NOTHING`.

Returns:
- `list[dict] | int`: Inserted rows (when `returning=True`) or affected row count.

#### `upsert_row(...)`
```python
upsert_row(
    table: str,
    data: dict,
    *,
    conflict_columns: Sequence[str],
    update_columns: Sequence[str] | None = None,
    do_nothing: bool = False,
    returning: bool = True,
) -> dict | None
```
Short description:
Insert one row with `ON CONFLICT DO UPDATE` or `DO NOTHING`.

Parameters:
- `table`: Target table.
- `data`: Input row dict.
- `conflict_columns`: Conflict target columns.
- `update_columns`: Columns to update on conflict; default is all non-conflict columns.
- `do_nothing`: If `True`, conflict path is `DO NOTHING`.
- `returning`: If `True`, returns row data.

Returns:
- `dict | None`: Returned row when available, else `None`.

#### `upsert_rows(...)`
```python
upsert_rows(
    table: str,
    rows: Sequence[dict],
    *,
    conflict_columns: Sequence[str],
    update_columns: Sequence[str] | None = None,
    do_nothing: bool = False,
    chunk_size: int = 500,
    returning: bool = False,
) -> list[dict] | int
```
Short description:
Batch upsert with chunking.

Parameters:
- `table`: Target table.
- `rows`: Sequence of row dicts with identical keys.
- `conflict_columns`: Conflict target columns.
- `update_columns`: Columns updated on conflict.
- `do_nothing`: Use `DO NOTHING` on conflict.
- `chunk_size`: Rows per statement.
- `returning`: If `True`, returns row dicts.

Returns:
- `list[dict] | int`: Returned rows or affected count.

#### `update_rows_with_equalities(...)`
```python
update_rows_with_equalities(table: str, updates: dict, equalities: dict) -> int
```
Short description:
Update rows using only equality conditions.

Parameters:
- `table`: Target table.
- `updates`: Column-value map for `SET` clause.
- `equalities`: Column-value map for exact-match `WHERE` clause.

Returns:
- `int`: Number of updated rows.

#### `update_rows_with_filters(...)`
```python
update_rows_with_filters(
    table: str,
    updates: dict,
    equalities: dict | None = None,
    raw_conditions: RawCondition | sql.Composable | str | list[RawCondition | sql.Composable | str] | None = None,
    raw_params: list | None = None,
    joins: list[JoinSpec | tuple[str, str, str] | str] | None = None,
) -> int
```
Short description:
Update rows using equality filters, raw predicates, and optional join predicates.

Parameters:
- `table`: Target table.
- `updates`: Column-value map for `SET`.
- `equalities`: Exact-match `WHERE` filters.
- `raw_conditions`: Additional raw SQL conditions.
- `raw_params`: Positional params for raw conditions.
- `joins`: Join definitions used to build `FROM` + join predicates.

Returns:
- `int`: Number of updated rows.

#### `delete_rows_with_filters(...)`
```python
delete_rows_with_filters(
    table: str,
    equalities: dict | None = None,
    raw_conditions: RawCondition | sql.Composable | str | list[RawCondition | sql.Composable | str] | None = None,
    raw_params: list | None = None,
    joins: list[JoinSpec | tuple[str, str, str] | str] | None = None,
) -> int
```
Short description:
Delete rows with required filters/joins.

Parameters:
- `table`: Target table.
- `equalities`: Exact-match conditions.
- `raw_conditions`: Additional raw SQL conditions.
- `raw_params`: Params for raw conditions.
- `joins`: Join definitions used as delete relation predicates.

Returns:
- `int`: Number of deleted rows.

### Convenience Flows

#### `get_or_create(...)`
```python
get_or_create(table: str, lookup: dict, defaults: dict | None = None) -> tuple[dict, bool]
```
Short description:
Fetch one row by lookup or create it.

Parameters:
- `table`: Target table.
- `lookup`: Required equality keys used to find existing row.
- `defaults`: Additional fields used only on create path.

Returns:
- `tuple[dict, bool]`: `(row, created)`.

#### `update_or_create(...)`
```python
update_or_create(table: str, lookup: dict, updates: dict | None = None) -> tuple[dict, bool]
```
Short description:
Fetch one row by lookup and update it, or create when missing.

Parameters:
- `table`: Target table.
- `lookup`: Required equality keys for lookup.
- `updates`: Update fields for existing row, also included in create payload.

Returns:
- `tuple[dict, bool]`: `(row, created)`.

### Database and Schema Helpers

#### `database_exists(...)`
```python
database_exists(db_name: str) -> bool
```
Short description:
Check whether database exists.

Parameters:
- `db_name`: Database name.

Returns:
- `bool`.

#### `create_database(...)`
```python
create_database(db_name: str, exists_ok: bool = True) -> bool
```
Short description:
Create database.

Parameters:
- `db_name`: Database name.
- `exists_ok`: If `True`, skip create when already exists.

Returns:
- `bool`: `True` if created, `False` if skipped due to existing DB.

#### `drop_database(...)`
```python
drop_database(db_name: str, missing_ok: bool = True) -> bool
```
Short description:
Drop database.

Parameters:
- `db_name`: Database name.
- `missing_ok`: If `False`, raise when DB does not exist.

Returns:
- `bool`: `True` on successful drop command path.

#### `schema_exists(...)`
```python
schema_exists(schema: str) -> bool
```
Short description:
Check whether schema exists.

Parameters:
- `schema`: Schema name.

Returns:
- `bool`.

#### `list_schemas(...)`
```python
list_schemas(exclude_system: bool = True) -> list[str]
```
Short description:
List schemas.

Parameters:
- `exclude_system`: Exclude system/internal schemas.

Returns:
- `list[str]`: Schema names.

#### `create_schema(...)`
```python
create_schema(schema: str, exists_ok: bool = True) -> None
```
Short description:
Create schema.

Parameters:
- `schema`: Schema name.
- `exists_ok`: Use idempotent create behavior.

Returns:
- `None`.

#### `drop_schema(...)`
```python
drop_schema(schema: str, cascade: bool = False, missing_ok: bool = True) -> None
```
Short description:
Drop schema.

Parameters:
- `schema`: Schema name.
- `cascade`: Drop dependent objects.
- `missing_ok`: Ignore missing schema if `True`.

Returns:
- `None`.

#### `ensure_schema(...)`
```python
ensure_schema(schema: str) -> None
```
Short description:
Create schema if missing.

Parameters:
- `schema`: Schema name.

Returns:
- `None`.

### Table and Column Helpers

#### `table_exists(...)`
```python
table_exists(schema: str, table: str) -> bool
```
Short description:
Check whether table exists in schema.

Parameters:
- `schema`: Schema name.
- `table`: Table name.

Returns:
- `bool`.

#### `list_tables(...)`
```python
list_tables(schema: str = "public") -> list[str]
```
Short description:
List base tables in schema.

Parameters:
- `schema`: Schema name.

Returns:
- `list[str]`: Table names.

#### `get_table_columns(...)`
```python
get_table_columns(schema: str, table: str) -> list[str]
```
Short description:
Get ordered column names for a table.

Parameters:
- `schema`: Schema name.
- `table`: Table name.

Returns:
- `list[str]`: Column names in ordinal order.

#### `column_exists(...)`
```python
column_exists(schema: str, table: str, column: str) -> bool
```
Short description:
Check whether a column exists in a table.

Parameters:
- `schema`: Schema name.
- `table`: Table name.
- `column`: Column name.

Returns:
- `bool`.

#### `create_table(...)`
```python
create_table(
    schema: str,
    table: str,
    columns: dict[str, str],
    constraints: list[str] | None = None,
    if_not_exists: bool = True,
    temporary: bool = False,
) -> None
```
Short description:
Create table from column SQL definitions and optional constraints.

Parameters:
- `schema`: Schema name.
- `table`: Table name.
- `columns`: Format `{column_name: "SQL type/definition"}`.
- `constraints`: Optional table-level SQL constraint fragments.
- `if_not_exists`: Use idempotent create.
- `temporary`: Create temp table when `True`.

Returns:
- `None`.

#### `drop_table(...)`
```python
drop_table(schema: str, table: str, cascade: bool = False, missing_ok: bool = True) -> None
```
Short description:
Drop table.

Parameters:
- `schema`: Schema name.
- `table`: Table name.
- `cascade`: Drop dependent objects.
- `missing_ok`: Ignore missing table if `True`.

Returns:
- `None`.

#### `ensure_table(...)`
```python
ensure_table(schema: str, table: str, columns: dict[str, str], constraints: list[str] | None = None) -> None
```
Short description:
Create table if missing.

Parameters:
- `schema`: Schema name.
- `table`: Table name.
- `columns`: Column SQL definitions.
- `constraints`: Optional constraints list.

Returns:
- `None`.

#### `add_column(...)`
```python
add_column(schema: str, table: str, column: str, type_sql: str) -> None
```
Short description:
Add column to table.

Parameters:
- `schema`: Schema name.
- `table`: Table name.
- `column`: Column name.
- `type_sql`: SQL type/definition.

Returns:
- `None`.

#### `drop_column(...)`
```python
drop_column(schema: str, table: str, column: str, *, cascade: bool = False, missing_ok: bool = True) -> None
```
Short description:
Drop column from table.

Parameters:
- `schema`: Schema name.
- `table`: Table name.
- `column`: Column name.
- `cascade`: Drop dependents.
- `missing_ok`: Ignore missing column.

Returns:
- `None`.

#### `alter_column_type(...)`
```python
alter_column_type(schema: str, table: str, column: str, type_sql: str, *, using: str | None = None) -> None
```
Short description:
Change column type.

Parameters:
- `schema`: Schema name.
- `table`: Table name.
- `column`: Column name.
- `type_sql`: Target SQL type expression.
- `using`: Optional SQL cast expression for conversion.

Returns:
- `None`.

#### `alter_column_nullability(...)`
```python
alter_column_nullability(schema: str, table: str, column: str, *, nullable: bool) -> None
```
Short description:
Set or unset `NOT NULL`.

Parameters:
- `schema`: Schema name.
- `table`: Table name.
- `column`: Column name.
- `nullable`: `True` => drop not-null, `False` => set not-null.

Returns:
- `None`.

#### `alter_column_default(...)`
```python
alter_column_default(schema: str, table: str, column: str, *, default_sql: str | None = None, drop: bool = False) -> None
```
Short description:
Set or remove column default.

Parameters:
- `schema`: Schema name.
- `table`: Table name.
- `column`: Column name.
- `default_sql`: SQL expression for default value.
- `drop`: If `True`, drop default instead of setting one.

Returns:
- `None`.

#### `get_column_info(...)`
```python
get_column_info(schema: str, table: str) -> dict[str, dict]
```
Short description:
Return metadata for all table columns.

Parameters:
- `schema`: Schema name.
- `table`: Table name.

Returns:
- `dict[str, dict]`: Keyed by `column_name`, each value contains metadata fields from `information_schema.columns`.

### Index and Constraint Helpers

#### `index_exists(...)`
```python
index_exists(schema: str, index_name: str) -> bool
```
Short description:
Check whether index exists.

Parameters:
- `schema`: Schema name.
- `index_name`: Index name.

Returns:
- `bool`.

#### `create_index(...)`
```python
create_index(
    schema: str,
    table: str,
    index_name: str,
    columns: list[str],
    unique: bool = False,
    if_not_exists: bool = True,
) -> None
```
Short description:
Create index on table columns.

Parameters:
- `schema`: Schema name.
- `table`: Table name.
- `index_name`: Index name.
- `columns`: Column names for index key.
- `unique`: Create unique index.
- `if_not_exists`: Idempotent create behavior.

Returns:
- `None`.

#### `drop_index(...)`
```python
drop_index(schema: str, index_name: str, missing_ok: bool = True) -> None
```
Short description:
Drop index.

Parameters:
- `schema`: Schema name.
- `index_name`: Index name.
- `missing_ok`: Ignore missing index.

Returns:
- `None`.

#### `list_indexes(...)`
```python
list_indexes(schema: str, table: str) -> list[dict]
```
Short description:
List indexes for table.

Parameters:
- `schema`: Schema name.
- `table`: Table name.

Returns:
- `list[dict]`: Index rows (includes `indexname`, `indexdef`).

#### `constraint_exists(...)`
```python
constraint_exists(schema: str, table: str, constraint_name: str) -> bool
```
Short description:
Check whether named constraint exists on table.

Parameters:
- `schema`: Schema name.
- `table`: Table name.
- `constraint_name`: Constraint name.

Returns:
- `bool`.

#### `add_constraint(...)`
```python
add_constraint(schema: str, table: str, constraint_sql: str) -> None
```
Short description:
Add table constraint from raw SQL fragment.

Parameters:
- `schema`: Schema name.
- `table`: Table name.
- `constraint_sql`: SQL fragment after `ADD` (e.g. `CONSTRAINT ... UNIQUE (...)`).

Returns:
- `None`.

#### `drop_constraint(...)`
```python
drop_constraint(schema: str, table: str, constraint_name: str, *, missing_ok: bool = True) -> None
```
Short description:
Drop table constraint.

Parameters:
- `schema`: Schema name.
- `table`: Table name.
- `constraint_name`: Constraint name.
- `missing_ok`: Ignore missing constraint.

Returns:
- `None`.

#### `list_constraints(...)`
```python
list_constraints(schema: str, table: str) -> list[dict]
```
Short description:
List table constraints.

Parameters:
- `schema`: Schema name.
- `table`: Table name.

Returns:
- `list[dict]`: Constraint rows (includes `constraint_name`, `constraint_type`).

#### `get_primary_key_columns(...)`
```python
get_primary_key_columns(schema: str, table: str) -> list[str]
```
Short description:
Get ordered primary key column names.

Parameters:
- `schema`: Schema name.
- `table`: Table name.

Returns:
- `list[str]`.

#### `get_constraint_columns(...)`
```python
get_constraint_columns(schema: str, table: str, constraint_name: str) -> list[str]
```
Short description:
Get ordered column names for a specific constraint.

Parameters:
- `schema`: Schema name.
- `table`: Table name.
- `constraint_name`: Constraint name.

Returns:
- `list[str]`.

#### `list_constraint_indexes(...)`
```python
list_constraint_indexes(schema: str, table: str) -> list[str]
```
Short description:
List index names that back table constraints.

Parameters:
- `schema`: Schema name.
- `table`: Table name.

Returns:
- `list[str]`.

## `TableEnforcer` API

### `enforce(...)`
```python
TableEnforcer.enforce(
    *,
    table: str | None = None,
    config_dir: str | Path | None = None,
    config_files: Sequence[str | Path] | None = None,
    config_payloads: Sequence[dict | list] | None = None,
    safe_mode: bool = True,
    drop_tables_not_in_config: bool = False,
    cleanup_not_null_rows: bool = True,
    plan_only: bool = False,
) -> set[tuple[str, str]] | tuple[set[tuple[str, str]], list[str]]
```
Short description:
Load table config sources and enforce schema state.

Parameters:
- `table`: Optional target filter. Format `"table"` or `"schema.table"`.
- `config_dir`: Directory containing `*.json` config files.
- `config_files`: Explicit config file paths.
- `config_payloads`: In-memory config dict/list payloads.
- `safe_mode`: Additive-only changes when `True`; forceful reconciliation when `False`.
- `drop_tables_not_in_config`: Drop existing tables not present in config (schema-scoped).
- `cleanup_not_null_rows`: In forceful mode, delete rows violating future `NOT NULL`.
- `plan_only`: Dry-run mode; collect actions without applying DB changes.

Returns:
- `set[tuple[str, str]]` when `plan_only=False`.
- `tuple[set[tuple[str, str]], list[str]]` when `plan_only=True`.

### `last_plan`
```python
TableEnforcer.last_plan -> list[str]
```
Short description:
Get most recently collected planned action list.

Parameters:
- None.

Returns:
- `list[str]`: Planned DDL/DML actions.

### `validate_config(...)`
```python
TableEnforcer.validate_config(cfg: dict | list, *, source_name: str = "<config>") -> list[dict]
```
Short description:
Validate config payload structure and normalize to list-of-table configs.

Parameters:
- `cfg`: Table config payload (single table object, list, or object containing `"tables"`).
- `source_name`: Source label used in validation errors.

Returns:
- `list[dict]`: Normalized list of table config objects.

### `enforce_tables(...)`
```python
enforce_tables(client: PSQLClient, **kwargs) -> set[tuple[str, str]] | tuple[set[tuple[str, str]], list[str]]
```
Short description:
Convenience wrapper for `TableEnforcer(client).enforce(**kwargs)`.

Parameters:
- `client`: `PSQLClient` instance.
- `kwargs`: Same keyword arguments as `TableEnforcer.enforce(...)`.

Returns:
- Same return contract as `TableEnforcer.enforce(...)`.

## Config Format Notes

Per table config object:
- Required: `table_name`, `columns`
- Optional: `schema` (default `"public"`), `indexes`

Column config commonly uses:
- `name`, `type` or `raw_type`
- `nullable`, `default`
- `primary_key`, `unique`
- `foreign_key` (`table`, `column`, optional `on_delete`, `on_update`)
- `enum` (list of allowed values)

## Testing

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```
