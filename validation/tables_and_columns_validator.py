import json
import os
from typing import List, Dict, Tuple, Union
import sqlglot
from sqlglot.optimizer.scope import build_scope, Scope
from config.file_path import CONFIG_PATH, SEMANTIC_LAYER_DIR

def load_json_file(path: str) -> dict:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}

error_msgs = load_json_file(os.path.join(CONFIG_PATH, "error_msgs.json"))
schema = load_json_file(os.path.join(SEMANTIC_LAYER_DIR, "schema_context.json"))

def extract_columns_with_tables(sql: Union[str, sqlglot.exp.Expression]) -> List[Dict[str, str]]:
    results = []
    seen = set()

    try:
        statement = sqlglot.parse_one(sql) if isinstance(sql, str) else sql
    except Exception:
        return results

    try:
        root = build_scope(statement)
        if not root:
            return results
    except Exception:
        return results

    try:
        for scope in root.traverse():
            alias_map = {}
            for name, source in scope.sources.items():
                if isinstance(source, sqlglot.exp.Table):
                    alias_map[name] = source.name
                elif isinstance(source, Scope):
                    alias_map[name] = f"<subquery:{name}>"

            default_table = list(alias_map.values())[0] if len(alias_map) == 1 else "unknown"

            for col in scope.columns:
                col_name = getattr(col, "name", None)
                table_alias = getattr(col, "table", None)
                if not col_name:
                    continue

                real_table = alias_map.get(table_alias, table_alias or default_table)

                key = (col_name, real_table)
                if key not in seen:
                    seen.add(key)
                    results.append({"column": col_name, "table": real_table})
    except Exception:
        return results

    return results

def validate_columns_and_tables(columns_list: List[Dict[str, str]]) -> Tuple[bool, str]:
    if not isinstance(columns_list, list) or not all(isinstance(c, dict) for c in columns_list):
        return False, "Invalid input columns_list"

    try:
        tables_schema = schema.get("tables", {})
        for item in columns_list:
            table = item.get("table")
            column = item.get("column")

            if not table or not column:
                continue  # skip invalid entries

            if "subquery" in table:
                if table in tables_schema:
                    if column not in tables_schema.get(table, []):
                        return False, error_msgs.get("column_not_defined_error", f"Column '{column}' not defined")
                else:
                    return False, error_msgs.get("table_not_defined_error", f"Table '{table}' not defined")
    except Exception:
        return False, "Schema validation failed"

    return True, error_msgs.get("tables_and_columns_defined_msg", "All columns and tables are valid")