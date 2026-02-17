from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Iterable, Sequence

from src.psql_client import PSQLClient

logger = logging.getLogger(__name__)


class TableEnforcer:
    """
    Enforce PostgreSQL table schemas from JSON config(s).

    Supported config source shapes:
    - dict with a single table config (contains "table_name")
    - dict with "tables": [...]
    - list of table configs
    """

    def __init__(self, client: PSQLClient):
        self.client = client
        self._plan_only = False
        self._planned_actions: list[str] = []

    def enforce(
        self,
        *,
        table: str | None = None,
        config_dir: str | Path | None = None,
        config_files: Sequence[str | Path] | None = None,
        config_payloads: Sequence[dict | list] | None = None,
        safe_mode: bool = True,
        drop_tables_not_in_config: bool = False,
        cleanup_not_null_rows: bool = True,
        plan_only: bool = False,
    ) -> set[tuple[str, str]] | tuple[set[tuple[str, str]], list[str]]:
        """
        Enforce schema from one or more config sources.

        Args:
            table:
                Optional table filter. Accepts "table" or "schema.table".
            config_dir:
                Directory containing *.json table config files.
            config_files:
                Explicit list of JSON config files.
            config_payloads:
                In-memory config payload(s) already parsed from JSON.
            safe_mode:
                True => additive changes only.
                False => forceful reconciliation (drops/changes extras).
            drop_tables_not_in_config:
                Drop DB tables not present in the loaded configs for configured schemas.
            cleanup_not_null_rows:
                In forceful mode, delete NULL rows before enforcing NOT NULL.
            plan_only:
                True => return planned actions without applying DDL/DML changes.

        Returns:
            If plan_only=False: set of (schema, table) entries that were enforced.
            If plan_only=True: tuple of (configured_tables, planned_actions).
        """
        self._plan_only = bool(plan_only)
        self._planned_actions = []
        try:
            target_schema, target_table = self._parse_table_target(table)
            sources = self._load_sources(
                config_dir=config_dir,
                config_files=config_files,
                config_payloads=config_payloads,
            )

            configured: set[tuple[str, str]] = set()
            for source_name, cfg in sources:
                tables = self.validate_config(cfg, source_name=source_name)
                for t in tables:
                    schema = t.get("schema", "public")
                    name = t.get("table_name")
                    if not name:
                        raise ValueError(f"Config source '{source_name}' has entry missing 'table_name'.")

                    if target_table and name != target_table:
                        continue
                    if target_schema and schema != target_schema:
                        continue

                    self._verify_one_table(
                        t,
                        safe_mode=safe_mode,
                        cleanup_not_null_rows=cleanup_not_null_rows,
                    )
                    configured.add((schema, name))

            if table and not configured:
                raise ValueError(f"Target table '{table}' was not found in provided config sources.")

            if drop_tables_not_in_config:
                if table:
                    raise ValueError(
                        "drop_tables_not_in_config cannot be used with a single table filter. "
                        "Run without 'table' to perform schema-level cleanup."
                    )
                self._drop_unconfigured_tables(configured)

            if self._plan_only:
                return configured, list(self._planned_actions)
            return configured
        finally:
            self._plan_only = False

    @property
    def last_plan(self) -> list[str]:
        return list(self._planned_actions)

    def _record_action(self, action: str) -> None:
        self._planned_actions.append(action)

    def _apply_action(self, action: str, fn, *args, **kwargs):
        self._record_action(action)
        if self._plan_only:
            return None
        return fn(*args, **kwargs)

    def validate_config(self, cfg: dict | list, *, source_name: str = "<config>") -> list[dict]:
        """
        Validate a config payload and return normalized table entries.
        Raises ValueError when schema is invalid.
        """
        tables = self._normalise_tables_config(cfg)
        if not tables:
            raise ValueError(f"Config source '{source_name}' does not define any tables.")

        for i, t in enumerate(tables):
            if not isinstance(t, dict):
                raise ValueError(f"Config source '{source_name}' table entry #{i} must be an object.")

            table_name = t.get("table_name")
            if not isinstance(table_name, str) or not table_name.strip():
                raise ValueError(f"Config source '{source_name}' table entry #{i} has invalid 'table_name'.")

            schema = t.get("schema", "public")
            if not isinstance(schema, str) or not schema.strip():
                raise ValueError(f"Config source '{source_name}' table '{table_name}' has invalid 'schema'.")

            columns_cfg = t.get("columns")
            if not isinstance(columns_cfg, list) or not columns_cfg:
                raise ValueError(f"Config source '{source_name}' table '{table_name}' must have non-empty 'columns'.")

            seen_colnames: set[str] = set()
            for col_idx, c in enumerate(columns_cfg):
                if not isinstance(c, dict):
                    raise ValueError(
                        f"Config source '{source_name}' table '{table_name}' column #{col_idx} must be an object."
                    )
                col_name = c.get("name")
                if not isinstance(col_name, str) or not col_name.strip():
                    raise ValueError(
                        f"Config source '{source_name}' table '{table_name}' column #{col_idx} has invalid 'name'."
                    )
                if col_name in seen_colnames:
                    raise ValueError(
                        f"Config source '{source_name}' table '{table_name}' has duplicate column '{col_name}'."
                    )
                seen_colnames.add(col_name)

                if "type" not in c and not c.get("raw_type"):
                    raise ValueError(
                        f"Config source '{source_name}' table '{table_name}' column '{col_name}' "
                        "must define 'type' or 'raw_type'."
                    )
                self._column_type_sql(c)

                fk = c.get("foreign_key")
                if fk is not None:
                    if not isinstance(fk, dict):
                        raise ValueError(
                            f"Config source '{source_name}' table '{table_name}' column '{col_name}' "
                            "foreign_key must be an object."
                        )
                    fk_table = fk.get("table")
                    fk_column = fk.get("column")
                    if not isinstance(fk_table, str) or not fk_table.strip():
                        raise ValueError(
                            f"Config source '{source_name}' table '{table_name}' column '{col_name}' "
                            "foreign_key.table must be a non-empty string."
                        )
                    if not isinstance(fk_column, str) or not fk_column.strip():
                        raise ValueError(
                            f"Config source '{source_name}' table '{table_name}' column '{col_name}' "
                            "foreign_key.column must be a non-empty string."
                        )

                enum_vals = c.get("enum")
                if enum_vals is not None:
                    if not isinstance(enum_vals, list) or not enum_vals:
                        raise ValueError(
                            f"Config source '{source_name}' table '{table_name}' column '{col_name}' "
                            "enum must be a non-empty list."
                        )

            indexes_cfg = t.get("indexes", [])
            if not isinstance(indexes_cfg, list):
                raise ValueError(f"Config source '{source_name}' table '{table_name}' has invalid 'indexes'.")
            seen_indexes: set[str] = set()
            for idx in indexes_cfg:
                if not isinstance(idx, dict):
                    raise ValueError(f"Config source '{source_name}' table '{table_name}' index entries must be objects.")
                idx_name = idx.get("name")
                if not isinstance(idx_name, str) or not idx_name.strip():
                    raise ValueError(
                        f"Config source '{source_name}' table '{table_name}' has index with invalid 'name'."
                    )
                if idx_name in seen_indexes:
                    raise ValueError(
                        f"Config source '{source_name}' table '{table_name}' has duplicate index name '{idx_name}'."
                    )
                seen_indexes.add(idx_name)
                idx_cols = idx.get("columns")
                if not isinstance(idx_cols, list) or not idx_cols:
                    raise ValueError(
                        f"Config source '{source_name}' table '{table_name}' index '{idx_name}' "
                        "must have non-empty 'columns'."
                    )
                missing_cols = [c for c in idx_cols if c not in seen_colnames]
                if missing_cols:
                    raise ValueError(
                        f"Config source '{source_name}' table '{table_name}' index '{idx_name}' "
                        f"references unknown columns: {missing_cols}"
                    )

        return tables

    def _parse_table_target(self, table: str | None) -> tuple[str | None, str | None]:
        if not table:
            return None, None
        table_name = table.strip()
        if not table_name:
            return None, None
        if "." not in table_name:
            return None, table_name.strip('"')
        schema, name = table_name.split(".", 1)
        return schema.strip('"'), name.strip('"')

    def _load_sources(
        self,
        *,
        config_dir: str | Path | None,
        config_files: Sequence[str | Path] | None,
        config_payloads: Sequence[dict | list] | None,
    ) -> list[tuple[str, dict | list]]:
        sources: list[tuple[str, dict | list]] = []

        if config_dir is not None:
            directory = Path(config_dir)
            if not directory.exists() or not directory.is_dir():
                raise ValueError(f"config_dir does not exist or is not a directory: {directory}")
            for json_path in sorted(directory.glob("*.json")):
                sources.append((str(json_path), self._load_json_file(json_path)))

        if config_files:
            for raw_path in config_files:
                json_path = Path(raw_path)
                if not json_path.exists() or not json_path.is_file():
                    raise ValueError(f"Config file not found: {json_path}")
                sources.append((str(json_path), self._load_json_file(json_path)))

        if config_payloads:
            for i, payload in enumerate(config_payloads):
                sources.append((f"<payload:{i}>", payload))

        if not sources:
            raise ValueError(
                "At least one config source is required. Provide config_dir, config_files, or config_payloads."
            )

        return sources

    def _load_json_file(self, json_path: Path) -> dict | list:
        try:
            return json.loads(json_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise ValueError(f"Failed to parse JSON config: {json_path}") from exc

    def _drop_unconfigured_tables(self, configured: set[tuple[str, str]]) -> None:
        schemas = {schema for schema, _ in configured}
        for schema in schemas:
            try:
                existing = set(self.client.list_tables(schema))
            except Exception:
                logger.exception("Failed to list tables for schema %s", schema)
                continue
            allowed = {name for s, name in configured if s == schema}
            for table in sorted(existing - allowed):
                try:
                    self._apply_action(
                        f"DROP TABLE {schema}.{table} CASCADE",
                        self.client.drop_table,
                        schema,
                        table,
                        cascade=True,
                        missing_ok=True,
                    )
                    logger.warning("Dropped table not in config: %s.%s", schema, table)
                except Exception:
                    logger.exception("Failed to drop table %s.%s", schema, table)

    def _normalise_tables_config(self, cfg: dict | list) -> list[dict]:
        if cfg is None:
            raise ValueError("No table config loaded.")

        if isinstance(cfg, list):
            return cfg

        if isinstance(cfg, dict) and "tables" in cfg and isinstance(cfg["tables"], list):
            return cfg["tables"]

        if isinstance(cfg, dict) and "table_name" in cfg:
            return [cfg]

        raise ValueError("Unsupported table config structure.")

    def _verify_one_table(self, t: dict, *, safe_mode: bool, cleanup_not_null_rows: bool) -> None:
        schema = t.get("schema", "public")
        table = t["table_name"]
        columns_cfg = t.get("columns", [])
        indexes_cfg = t.get("indexes", [])

        self._apply_action(
            f"CREATE SCHEMA IF NOT EXISTS {schema}",
            self.client.ensure_schema,
            schema,
        )

        if not self.client.table_exists(schema, table):
            self._create_table_from_config(schema, table, columns_cfg, indexes_cfg)
            logger.info("Created table %s.%s", schema, table)
            return

        if safe_mode:
            has_changes = self._alter_table_additive(schema, table, columns_cfg, indexes_cfg, safe_mode=safe_mode)
        else:
            has_changes = self._alter_table_forceful(
                schema,
                table,
                columns_cfg,
                indexes_cfg,
                cleanup_not_null_rows=cleanup_not_null_rows,
            )
        if not has_changes:
            logger.debug("Verified table %s.%s (no changes)", schema, table)
        else:
            logger.info("Verified table %s.%s (changes applied)", schema, table)

    def _create_table_from_config(
        self,
        schema: str,
        table: str,
        columns_cfg: list[dict],
        indexes_cfg: list[dict],
    ) -> None:
        columns: dict[str, str] = {}
        constraints: list[str] = []

        for c in columns_cfg:
            col_name = c["name"]
            col_sql = self._column_type_sql(c)

            mod_bits = []
            if not c.get("nullable", True):
                mod_bits.append("NOT NULL")
            if "default" in c and c["default"] is not None:
                mod_bits.append(f"DEFAULT {self._default_sql(c['default'])}")

            if c.get("primary_key", False):
                mod_bits.append("PRIMARY KEY")

            columns[col_name] = (col_sql + (" " + " ".join(mod_bits) if mod_bits else "")).strip()

        for c in columns_cfg:
            if c.get("unique", False) and not c.get("primary_key", False):
                con_name = f"{table}_{c['name']}_key"
                constraints.append(f'CONSTRAINT "{con_name}" UNIQUE ("{c["name"]}")')

        for c in columns_cfg:
            fk = c.get("foreign_key")
            if fk:
                con_name = f"{table}_{c['name']}_fkey"
                on_delete = fk.get("on_delete")
                on_update = fk.get("on_update")
                frag = (
                    f'CONSTRAINT "{con_name}" FOREIGN KEY ("{c["name"]}") '
                    f'REFERENCES "{fk["table"]}" ("{fk["column"]}")'
                )
                if on_delete:
                    frag += f" ON DELETE {on_delete.upper()}"
                if on_update:
                    frag += f" ON UPDATE {on_update.upper()}"
                constraints.append(frag)
            enum_vals = c.get("enum")
            if enum_vals:
                con_name = f"{table}_{c['name']}_enum_check"
                enum_sql = self._enum_values_sql(enum_vals)
                constraints.append(f'CONSTRAINT "{con_name}" CHECK ("{c["name"]}" IN ({enum_sql}))')

        self._apply_action(
            f"CREATE TABLE {schema}.{table}",
            self.client.create_table,
            schema,
            table,
            columns,
            constraints=constraints,
            if_not_exists=True,
        )
        self._ensure_indexes(schema, table, indexes_cfg, columns_cfg)

    def _alter_table_additive(
        self,
        schema: str,
        table: str,
        columns_cfg: list[dict],
        indexes_cfg: list[dict],
        *,
        safe_mode: bool,
    ) -> bool:
        existing_cols = self.client.get_column_info(schema, table)
        existing_colnames = set(existing_cols.keys())
        changes_detected = False

        for c in columns_cfg:
            name = c["name"]
            if name in existing_colnames:
                continue

            type_sql = self._column_type_sql(c)
            mod_bits = []
            if "default" in c and c["default"] is not None:
                mod_bits.append(f"DEFAULT {self._default_sql(c['default'])}")

            if not c.get("nullable", True):
                if safe_mode and "default" not in c:
                    logger.warning(
                        "Adding column %s.%s.%s as NULLABLE (safe_mode). Config wants NOT NULL but no default was provided.",
                        schema,
                        table,
                        name,
                    )
                else:
                    mod_bits.append("NOT NULL")

            col_def = (type_sql + (" " + " ".join(mod_bits) if mod_bits else "")).strip()
            self._apply_action(
                f"ALTER TABLE {schema}.{table} ADD COLUMN {name}",
                self.client.add_column,
                schema,
                table,
                name,
                col_def,
            )
            logger.info("Added column %s.%s.%s", schema, table, name)
            changes_detected = True

        for c in columns_cfg:
            if c.get("unique", False) and not c.get("primary_key", False):
                con_name = f"{table}_{c['name']}_key"
                if not self.client.constraint_exists(schema, table, con_name):
                    self._apply_action(
                        f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {con_name}",
                        self.client.add_constraint,
                        schema,
                        table,
                        f'CONSTRAINT "{con_name}" UNIQUE ("{c["name"]}")',
                    )
                    logger.info("Added UNIQUE constraint %s on %s.%s(%s)", con_name, schema, table, c["name"])
                    changes_detected = True

            fk = c.get("foreign_key")
            if fk:
                con_name = f"{table}_{c['name']}_fkey"
                if not self.client.constraint_exists(schema, table, con_name):
                    on_delete = fk.get("on_delete")
                    on_update = fk.get("on_update")
                    frag = (
                        f'CONSTRAINT "{con_name}" FOREIGN KEY ("{c["name"]}") '
                        f'REFERENCES "{fk["table"]}" ("{fk["column"]}")'
                    )
                    if on_delete:
                        frag += f" ON DELETE {on_delete.upper()}"
                    if on_update:
                        frag += f" ON UPDATE {on_update.upper()}"
                    self._apply_action(
                        f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {con_name}",
                        self.client.add_constraint,
                        schema,
                        table,
                        frag,
                    )
                    logger.info("Added FK constraint %s on %s.%s(%s)", con_name, schema, table, c["name"])
                    changes_detected = True
            enum_vals = c.get("enum")
            if enum_vals:
                con_name = f"{table}_{c['name']}_enum_check"
                if not self.client.constraint_exists(schema, table, con_name):
                    enum_sql = self._enum_values_sql(enum_vals)
                    self._apply_action(
                        f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {con_name}",
                        self.client.add_constraint,
                        schema,
                        table,
                        f'CONSTRAINT "{con_name}" CHECK ("{c["name"]}" IN ({enum_sql}))',
                    )
                    logger.info("Added CHECK constraint %s on %s.%s(%s)", con_name, schema, table, c["name"])
                    changes_detected = True

        if self._ensure_indexes(schema, table, indexes_cfg, columns_cfg):
            changes_detected = True

        config_cols = {c["name"] for c in columns_cfg}
        extras = sorted(existing_colnames - config_cols)
        if extras:
            logger.warning("Table %s.%s has extra columns not in config: %s", schema, table, extras)

        return changes_detected

    def _ensure_indexes(
        self,
        schema: str,
        table: str,
        indexes_cfg: list[dict],
        columns_cfg: list[dict],
    ) -> bool:
        existing = {r["indexname"] for r in self.client.list_indexes(schema, table)}
        changed = False

        for c in columns_cfg:
            if c.get("index", False):
                idx_name = f"{table}_{c['name']}_idx"
                if idx_name not in existing:
                    self._apply_action(
                        f"CREATE INDEX {idx_name} ON {schema}.{table}",
                        self.client.create_index,
                        schema,
                        table,
                        idx_name,
                        [c["name"]],
                        unique=False,
                        if_not_exists=True,
                    )
                    logger.info("Created index %s on %s.%s(%s)", idx_name, schema, table, c["name"])
                    changed = True

        for idx in indexes_cfg:
            name = idx["name"]
            if name in existing:
                continue
            cols = idx["columns"]
            unique = bool(idx.get("unique", False))
            self._apply_action(
                f"CREATE {'UNIQUE ' if unique else ''}INDEX {name} ON {schema}.{table}",
                self.client.create_index,
                schema,
                table,
                name,
                cols,
                unique=unique,
                if_not_exists=True,
            )
            logger.info("Created index %s on %s.%s(%s)", name, schema, table, ", ".join(cols))
            changed = True

        return changed

    def _alter_table_forceful(
        self,
        schema: str,
        table: str,
        columns_cfg: list[dict],
        indexes_cfg: list[dict],
        *,
        cleanup_not_null_rows: bool,
    ) -> bool:
        changed = False
        existing_cols = self.client.get_column_info(schema, table)
        existing_colnames = set(existing_cols.keys())
        config_cols = {c["name"] for c in columns_cfg}
        not_null_targets: list[str] = []

        for c in columns_cfg:
            name = c["name"]
            if name in existing_colnames:
                continue
            type_sql = self._column_type_sql(c)
            mod_bits = []
            if not c.get("nullable", True):
                not_null_targets.append(name)
            if "default" in c and c["default"] is not None:
                mod_bits.append(f"DEFAULT {self._default_sql(c['default'])}")
            col_def = (type_sql + (" " + " ".join(mod_bits) if mod_bits else "")).strip()
            self._apply_action(
                f"ALTER TABLE {schema}.{table} ADD COLUMN {name}",
                self.client.add_column,
                schema,
                table,
                name,
                col_def,
            )
            logger.info("Added column %s.%s.%s", schema, table, name)
            changed = True

        for c in columns_cfg:
            name = c["name"]
            if name not in existing_cols:
                continue
            info = existing_cols[name]
            expected_sig = self._expected_type_signature(c)
            existing_sig = self._existing_type_signature(info)
            if expected_sig != existing_sig:
                type_sql = self._column_type_sql(c)
                self._apply_action(
                    f"ALTER TABLE {schema}.{table} ALTER COLUMN {name} TYPE {type_sql}",
                    self.client.alter_column_type,
                    schema,
                    table,
                    name,
                    type_sql,
                )
                logger.info("Altered type of %s.%s.%s to %s", schema, table, name, type_sql)
                changed = True

            nullable_expected = bool(c.get("nullable", True))
            nullable_current = (str(info.get("is_nullable", "YES")).upper() == "YES")
            if nullable_expected != nullable_current:
                if not nullable_expected and cleanup_not_null_rows:
                    self._cleanup_nulls(schema, table, name)
                self._apply_action(
                    f"ALTER TABLE {schema}.{table} ALTER COLUMN {name} {'DROP NOT NULL' if nullable_expected else 'SET NOT NULL'}",
                    self.client.alter_column_nullability,
                    schema,
                    table,
                    name,
                    nullable=nullable_expected,
                )
                logger.info(
                    "Altered nullability of %s.%s.%s to %s",
                    schema,
                    table,
                    name,
                    "NULL" if nullable_expected else "NOT NULL",
                )
                changed = True

            has_default = ("default" in c and c["default"] is not None)
            current_default = info.get("column_default")
            if has_default:
                expected_default = self._normalize_default(self._default_sql(c["default"]))
                current_norm = self._normalize_default(current_default)
                if expected_default != current_norm:
                    self._apply_action(
                        f"ALTER TABLE {schema}.{table} ALTER COLUMN {name} SET DEFAULT",
                        self.client.alter_column_default,
                        schema,
                        table,
                        name,
                        default_sql=self._default_sql(c["default"]),
                    )
                    logger.info("Altered default of %s.%s.%s", schema, table, name)
                    changed = True
            else:
                if current_default is not None:
                    self._apply_action(
                        f"ALTER TABLE {schema}.{table} ALTER COLUMN {name} DROP DEFAULT",
                        self.client.alter_column_default,
                        schema,
                        table,
                        name,
                        drop=True,
                    )
                    logger.info("Dropped default of %s.%s.%s", schema, table, name)
                    changed = True

        for name in not_null_targets:
            try:
                if cleanup_not_null_rows:
                    self._cleanup_nulls(schema, table, name)
                self._apply_action(
                    f"ALTER TABLE {schema}.{table} ALTER COLUMN {name} SET NOT NULL",
                    self.client.alter_column_nullability,
                    schema,
                    table,
                    name,
                    nullable=False,
                )
                logger.info("Enforced NOT NULL on %s.%s.%s after cleanup", schema, table, name)
                changed = True
            except Exception:
                logger.exception("Failed to enforce NOT NULL on %s.%s.%s", schema, table, name)

        extras = sorted(existing_colnames - config_cols)
        for col in extras:
            self._apply_action(
                f"ALTER TABLE {schema}.{table} DROP COLUMN {col} CASCADE",
                self.client.drop_column,
                schema,
                table,
                col,
                cascade=True,
                missing_ok=True,
            )
            logger.info("Dropped extra column %s.%s.%s", schema, table, col)
            changed = True

        expected_constraints = set()
        pk_columns = [c["name"] for c in columns_cfg if c.get("primary_key", False)]
        if pk_columns:
            expected_constraints.add(f"{table}_pkey")

        for c in columns_cfg:
            if c.get("unique", False) and not c.get("primary_key", False):
                expected_constraints.add(f"{table}_{c['name']}_key")
            if c.get("foreign_key"):
                expected_constraints.add(f"{table}_{c['name']}_fkey")
            if c.get("enum"):
                expected_constraints.add(f"{table}_{c['name']}_enum_check")

        existing_constraints = self.client.list_constraints(schema, table)
        for con in existing_constraints:
            if con["constraint_type"] not in {"PRIMARY KEY", "UNIQUE", "FOREIGN KEY", "CHECK"}:
                continue
            con_name = con["constraint_name"]
            if con_name.endswith("_not_null"):
                continue
            if con_name not in expected_constraints:
                self._apply_action(
                    f"ALTER TABLE {schema}.{table} DROP CONSTRAINT {con_name}",
                    self.client.drop_constraint,
                    schema,
                    table,
                    con_name,
                    missing_ok=True,
                )
                logger.info("Dropped constraint %s on %s.%s", con_name, schema, table)
                changed = True

        if pk_columns:
            pk_name = f"{table}_pkey"
            if not self.client.constraint_exists(schema, table, pk_name):
                col_list = ", ".join(f'"{c}"' for c in pk_columns)
                self._apply_action(
                    f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {pk_name}",
                    self.client.add_constraint,
                    schema,
                    table,
                    f'CONSTRAINT "{pk_name}" PRIMARY KEY ({col_list})',
                )
                logger.info("Added PRIMARY KEY %s on %s.%s(%s)", pk_name, schema, table, ", ".join(pk_columns))
                changed = True
            else:
                current_pk_cols = self.client.get_constraint_columns(schema, table, pk_name)
                if set(current_pk_cols) != set(pk_columns):
                    self._apply_action(
                        f"ALTER TABLE {schema}.{table} DROP CONSTRAINT {pk_name}",
                        self.client.drop_constraint,
                        schema,
                        table,
                        pk_name,
                        missing_ok=True,
                    )
                    col_list = ", ".join(f'"{c}"' for c in pk_columns)
                    self._apply_action(
                        f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {pk_name}",
                        self.client.add_constraint,
                        schema,
                        table,
                        f'CONSTRAINT "{pk_name}" PRIMARY KEY ({col_list})',
                    )
                    logger.info("Rebuilt PRIMARY KEY %s on %s.%s(%s)", pk_name, schema, table, ", ".join(pk_columns))
                    changed = True

        for c in columns_cfg:
            if c.get("unique", False) and not c.get("primary_key", False):
                con_name = f"{table}_{c['name']}_key"
                if not self.client.constraint_exists(schema, table, con_name):
                    self._apply_action(
                        f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {con_name}",
                        self.client.add_constraint,
                        schema,
                        table,
                        f'CONSTRAINT "{con_name}" UNIQUE ("{c["name"]}")',
                    )
                    logger.info("Added UNIQUE constraint %s on %s.%s(%s)", con_name, schema, table, c["name"])
                    changed = True

            fk = c.get("foreign_key")
            if fk:
                con_name = f"{table}_{c['name']}_fkey"
                if not self.client.constraint_exists(schema, table, con_name):
                    on_delete = fk.get("on_delete")
                    on_update = fk.get("on_update")
                    frag = (
                        f'CONSTRAINT "{con_name}" FOREIGN KEY ("{c["name"]}") '
                        f'REFERENCES "{fk["table"]}" ("{fk["column"]}")'
                    )
                    if on_delete:
                        frag += f" ON DELETE {on_delete.upper()}"
                    if on_update:
                        frag += f" ON UPDATE {on_update.upper()}"
                    self._apply_action(
                        f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {con_name}",
                        self.client.add_constraint,
                        schema,
                        table,
                        frag,
                    )
                    logger.info("Added FK constraint %s on %s.%s(%s)", con_name, schema, table, c["name"])
                    changed = True
            enum_vals = c.get("enum")
            if enum_vals:
                con_name = f"{table}_{c['name']}_enum_check"
                if not self.client.constraint_exists(schema, table, con_name):
                    enum_sql = self._enum_values_sql(enum_vals)
                    self._apply_action(
                        f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {con_name}",
                        self.client.add_constraint,
                        schema,
                        table,
                        f'CONSTRAINT "{con_name}" CHECK ("{c["name"]}" IN ({enum_sql}))',
                    )
                    logger.info("Added CHECK constraint %s on %s.%s(%s)", con_name, schema, table, c["name"])
                    changed = True

        if self._ensure_indexes(schema, table, indexes_cfg, columns_cfg):
            changed = True

        expected_indexes = set()
        for c in columns_cfg:
            if c.get("index", False):
                expected_indexes.add(f"{table}_{c['name']}_idx")
        for idx in indexes_cfg:
            expected_indexes.add(idx["name"])

        existing_indexes = {r["indexname"] for r in self.client.list_indexes(schema, table)}
        constraint_indexes = set(self.client.list_constraint_indexes(schema, table))
        for idx_name in sorted(existing_indexes - expected_indexes - constraint_indexes):
            self._apply_action(
                f"DROP INDEX {schema}.{idx_name}",
                self.client.drop_index,
                schema,
                idx_name,
                missing_ok=True,
            )
            logger.info("Dropped index %s on %s.%s", idx_name, schema, table)
            changed = True

        return changed

    def _default_sql(self, value: Any) -> str:
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if value is None:
            return "NULL"
        return str(value)

    def _normalize_default(self, value: Any) -> str | None:
        if value is None:
            return None
        s = str(value).strip()
        if not s:
            return None
        if "::" in s:
            s = s.split("::", 1)[0]
        return s.strip().lower()

    def _cleanup_nulls(self, schema: str, table: str, column: str) -> None:
        if self._plan_only:
            self._record_action(f'DELETE FROM {schema}.{table} WHERE "{column}" IS NULL')
            return
        try:
            deleted = self.client.delete_rows_with_filters(
                f"{schema}.{table}",
                raw_conditions=f'"{column}" IS NULL',
            )
            if deleted <= 0:
                return
            logger.warning(
                "Deleted %s rows with NULL %s.%s.%s to satisfy NOT NULL.",
                deleted,
                schema,
                table,
                column,
            )
        except Exception:
            logger.exception("Failed to cleanup NULLs for %s.%s.%s", schema, table, column)

    def _enum_values_sql(self, values: list) -> str:
        escaped = []
        for v in values:
            s = str(v)
            escaped.append("'" + s.replace("'", "''") + "'")
        return ", ".join(escaped)

    def _expected_type_signature(self, c: dict) -> tuple:
        if "type" not in c or c["type"] is None:
            if c.get("raw_type"):
                return (str(c["raw_type"]).lower(),)
            return ("",)
        t = str(c["type"]).lower()
        if t in {"varchar", "character varying"}:
            return ("varchar", int(c.get("length", 0)))
        if t in {"char", "character"}:
            return ("char", int(c.get("length", 0)))
        if t in {"numeric", "decimal"}:
            prec = c.get("precision")
            scale = c.get("scale")
            return ("numeric", int(prec) if prec is not None else None, int(scale) if scale is not None else None)
        if t in {"int", "integer"}:
            return ("integer",)
        return (t,)

    def _existing_type_signature(self, info: dict) -> tuple:
        udt = (info.get("udt_name") or "").lower()
        data_type = (info.get("data_type") or "").lower()

        if udt in {"varchar", "bpchar"}:
            base = "varchar" if udt == "varchar" else "char"
            return (base, int(info.get("character_maximum_length") or 0))
        if udt in {"numeric"}:
            return ("numeric", info.get("numeric_precision"), info.get("numeric_scale"))
        if udt in {"int4"}:
            return ("integer",)
        if udt in {"int8"}:
            return ("bigint",)
        if udt in {"bool"}:
            return ("boolean",)
        if udt in {"timestamptz"}:
            return ("timestamptz",)
        if udt in {"timestamp"}:
            return ("timestamp",)
        if udt:
            return (udt,)
        return (data_type or "",)

    def _column_type_sql(self, c: dict) -> str:
        if "type" not in c or c["type"] is None:
            if c.get("raw_type"):
                return str(c["raw_type"])
            raise ValueError(f"Unsupported column type: {c.get('type')} (column {c.get('name')})")

        t = str(c["type"]).lower()

        if t in {"uuid", "text", "boolean", "timestamp", "timestamptz", "date", "json", "jsonb"}:
            return t

        if t in {"varchar", "character varying"}:
            n = c.get("length")
            if not n:
                raise ValueError(f"varchar column '{c['name']}' missing length")
            return f"varchar({int(n)})"

        if t in {"char", "character"}:
            n = c.get("length")
            if not n:
                raise ValueError(f"char column '{c['name']}' missing length")
            return f"char({int(n)})"

        if t in {"int", "integer"}:
            return "integer"

        if t in {"bigint"}:
            return "bigint"

        if t in {"numeric", "decimal"}:
            prec = c.get("precision")
            scale = c.get("scale")
            if prec is not None and scale is not None:
                return f"numeric({int(prec)},{int(scale)})"
            if prec is not None:
                return f"numeric({int(prec)})"
            return "numeric"

        raise ValueError(f"Unsupported column type: {c['type']} (column {c.get('name')})")


def enforce_tables(
    client: PSQLClient,
    **kwargs,
) -> set[tuple[str, str]] | tuple[set[tuple[str, str]], list[str]]:
    """
    Convenience wrapper:

        enforce_tables(client, config_dir="tables", safe_mode=True)
    """
    return TableEnforcer(client).enforce(**kwargs)
