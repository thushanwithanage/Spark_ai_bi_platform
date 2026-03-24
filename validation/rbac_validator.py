import json
import os
from config.file_path import CONFIG_PATH

with open(os.path.join(CONFIG_PATH, "rbac_config.json"), "r") as f:
    rbac_configs = json.load(f)

with open(os.path.join(CONFIG_PATH, "error_msgs.json"), "r") as f:
    error_msgs = json.load(f)

def validate_rbac(columns_list: list[dict], role: str) -> tuple[bool, str]:
    user_role = rbac_configs["roles"].get(role)
    for item in columns_list:
        if item["table"] != "<subquery:sub>":
            if item["table"] in user_role["tables"].keys():
                  if item["column"] not in user_role["tables"].get(item["table"]):
                    return False, error_msgs["column_permission_error"]
            else:
                return False, error_msgs["table_permission_error"]
    return True, ""