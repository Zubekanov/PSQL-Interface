# PSQL Interface

User-focused PostgreSQL utilities for Python:
- `PSQLClient`: pooled DB access + common read/write/DDL workflows
- `TableEnforcer`: enforce table schemas from JSON configs

This project is designed for app/service code that needs:
- safe parameterized SQL execution
- convenient CRUD with filters/joins/pagination
- upsert + bulk insert helpers
- transaction support
- schema reconciliation from config files

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

## Core Workflows

### 1) Read Rows

- `get_rows_with_filters(...)` for paginated list queries
- `count_rows_with_filters(...)` for counts
- `exists_with_filters(...)` for boolean checks
- `get_optional_one(...)` for one-or-none
- `get_one(...)` when exactly one row must match

```python
rows, pages = client.get_rows_with_filters(
    "public.users",
    equalities={"active": True},
    page_limit=50,
    page_num=0,
)

user = client.get_optional_one("public.users", equalities={"id": 10})
exists = client.exists_with_filters("public.users", equalities={"email": "a@b.com"})
total = client.count_rows_with_filters("public.users", equalities={"active": True})
```

### 2) Write Rows

- `insert_row(table, data)`
- `bulk_insert(table, rows, chunk_size=..., returning=..., do_nothing=..., conflict_columns=...)`
- `upsert_row(table, data, conflict_columns=[...], update_columns=..., do_nothing=...)`
- `upsert_rows(table, rows, conflict_columns=[...], update_columns=..., chunk_size=...)`
- `update_rows_with_equalities(...)`
- `update_rows_with_filters(...)`
- `delete_rows_with_filters(...)`

```python
created = client.insert_row("public.users", {"email": "x@y.com", "active": True})

affected = client.bulk_insert(
    "public.users",
    [{"id": 1, "email": "a@x.com"}, {"id": 2, "email": "b@x.com"}],
    chunk_size=500,
)

upserted = client.upsert_row(
    "public.users",
    {"id": 1, "email": "new@x.com", "active": True},
    conflict_columns=["id"],
)
```

### 3) Get-Or-Create Patterns

- `get_or_create(table, lookup, defaults=None) -> (row, created)`
- `update_or_create(table, lookup, updates=None) -> (row, created)`

```python
row, created = client.get_or_create(
    "public.users",
    lookup={"email": "first@x.com"},
    defaults={"active": True},
)

row2, created2 = client.update_or_create(
    "public.users",
    lookup={"id": 42},
    updates={"active": False},
)
```

### 4) Transactions

Use one connection and one commit boundary for multi-step operations.

```python
with client.transaction() as tx:
    tx.execute("INSERT INTO public.audit_log (event_type) VALUES (%s)", ["USER_IMPORT"])
    tx.execute("UPDATE public.users SET active = %s WHERE id = %s", [True, 42])
```

### 5) DDL and Metadata

Common helpers include:
- Database/schema: `create_database`, `drop_database`, `create_schema`, `ensure_schema`
- Tables/columns: `create_table`, `drop_table`, `add_column`, `drop_column`, `alter_column_*`
- Index/constraint: `create_index`, `drop_index`, `add_constraint`, `drop_constraint`
- Introspection: `list_tables`, `get_table_columns`, `get_column_info`, `list_indexes`, `list_constraints`

## TableEnforcer (Schema From JSON)

`TableEnforcer` applies JSON table definitions to PostgreSQL.

### Config Shapes

Accepted inputs:
- one table object (`{"table_name": "...", ...}`)
- object with `"tables": [...]`
- list of table objects

Minimal example:

```json
{
  "schema": "public",
  "table_name": "users",
  "columns": [
    { "name": "id", "type": "integer", "primary_key": true, "nullable": false },
    { "name": "email", "type": "varchar", "length": 320, "unique": true, "nullable": false },
    { "name": "active", "type": "boolean", "default": true, "nullable": false }
  ],
  "indexes": [
    { "name": "users_active_idx", "columns": ["active"] }
  ]
}
```

### Enforce Modes

- `safe_mode=True`: additive changes only
- `safe_mode=False`: forceful reconciliation (drop/alter extras to match config)
- `plan_only=True`: dry run; returns planned actions instead of mutating DB

```python
from src.table_enforcer import enforce_tables

configured, plan = enforce_tables(
    client,
    config_dir="tables",
    safe_mode=False,
    plan_only=True,
)
```

### Validation

Validate config payloads before changing the database:

```python
from src.table_enforcer import TableEnforcer

enforcer = TableEnforcer(client)
enforcer.validate_config(payload, source_name="users.json")
```

## Lifecycle Notes

- Use `PSQLClient.get(...)` to reuse pooled clients.
- Use `client.close()` when done with one client instance.
- Use `PSQLClient.closeall()` on shutdown to close all cached pools.

## Tests

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```
