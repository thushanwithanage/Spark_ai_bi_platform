import json
import os
import sqlglot
from sqlglot.optimizer.scope import build_scope, Scope
from config.file_path import CONFIG_PATH, SEMANTIC_LAYER_DIR

with open(os.path.join(CONFIG_PATH, "error_msgs.json"), "r") as f:
    error_msgs = json.load(f)

with open(os.path.join(SEMANTIC_LAYER_DIR, "schema_context.json"), "r") as f:
        schema = json.load(f)

def extract_columns_with_tables(sql: str | sqlglot.exp.Expression) -> list[dict]:
    statement = sqlglot.parse_one(sql) if isinstance(sql, str) else sql

    seen = set()
    results = []

    root = build_scope(statement)
    if root is None:
        return results

    for scope in root.traverse():
        # Build alias map
        alias_map = {}
        for name, source in scope.sources.items():
            if isinstance(source, sqlglot.exp.Table):
                alias_map[name] = source.name
            elif isinstance(source, Scope):
                alias_map[name] = f"<subquery:{name}>"

        default_table = list(alias_map.values())[0] if len(alias_map) == 1 else "unknown"

        for col in scope.columns:
            col_name = col.name
            table_alias = col.table
            real_table = alias_map.get(table_alias, table_alias or default_table)

            key = (col_name, real_table)
            if key not in seen:
                seen.add(key)
                results.append({"column": col_name, "table": real_table})

    return results

def validate_columns_and_tables(columns_list : list[dict]) -> tuple[bool, str]:
    for item in columns_list:
        if item["table"] != "<subquery:sub>":
            if item["table"] in schema["tables"].keys():
                if item["column"] not in schema["tables"].get(item["table"]):
                    return False, error_msgs["column_not_defined_error"]
            else:
                return False, error_msgs["table_not_defined_error"]
    return True, error_msgs["tables_and_columns_defined_msg"]