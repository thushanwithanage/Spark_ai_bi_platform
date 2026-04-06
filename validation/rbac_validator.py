import json
import os
from typing import List, Dict, Tuple, Any
from config.file_path import CONFIG_PATH

def load_json_file(filename: str) -> dict:
    path = os.path.join(CONFIG_PATH, filename)
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}

rbac_configs: dict = load_json_file("rbac_config.json")
error_msgs: dict = load_json_file("error_msgs.json")


def validate_rbac(columns_list: List[Dict[str, Any]], role: str) -> Tuple[bool, str]:
    if not isinstance(columns_list, list) or not all(isinstance(c, dict) for c in columns_list):
        return False, "Invalid columns_list input"

    if not isinstance(role, str):
        return False, "Invalid role input"

    user_role = rbac_configs.get("roles", {}).get(role)
    if not user_role:
        return False, error_msgs.get("role_permission_error", "Role not found")

    tables_permissions = user_role.get("tables", {})

    for item in columns_list:
        table = item.get("table")
        column = item.get("column")

        if table == "<subquery:sub>":
            continue

        if table not in tables_permissions:
            return False, error_msgs.get("table_permission_error", "Table permission denied")

        allowed_columns = tables_permissions.get(table, [])
        if column not in allowed_columns:
            return False, error_msgs.get("column_permission_error", "Column permission denied")

    return True, ""