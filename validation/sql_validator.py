import re
import json
import os
import sqlglot
from config.file_path import SEMANTIC_LAYER_DIR

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(BASE_DIR, "..", "config")

with open(os.path.join(CONFIG_DIR, "error_msgs.json"), "r") as f:
    error_msgs = json.load(f)
    
def validate_query(query: str) -> tuple[bool, str]:
    if not query or not query.strip():
        return False, error_msgs["empty_query_error"]
    
    query = query.strip()

    query = extract_sql_query(query)
    if not query:
        return False, error_msgs["invalid_ai_sql_error"]
    
    if not re.match(r'^\s*SELECT\b', query, re.IGNORECASE):
        return False, error_msgs["select_not_found_error"]
    
    reserved_keywords = ["DROP", "DELETE", "ALTER", "TRUNCATE", "GRANT", "REVOKE", "CREATE", "USE", "EXECUTE", "MERGE", "CALL", "EXPLAIN"]
    
    if any(value in query.upper() for value in reserved_keywords):
        return False, error_msgs["reserved_keywords_error"]
    
    if "SELECT *" in query.upper():
        return False, error_msgs["select_all_error"]

    if query.count('(') != query.count(')'):
        return False, error_msgs["unbalanced_parentheses_error"]

    rstrip_query = query.rstrip(';')
    if ';' in rstrip_query:
        return False, error_msgs["multiple_statements_error"]
    
    is_valid, message = validate_tables_and_columns(rstrip_query)

    return is_valid, message

def extract_sql_query(text: str) -> str:
    fenced = re.search(r'```(?:sql)?\s*(.*?)```', text, re.DOTALL | re.IGNORECASE)
    if fenced:
        return fenced.group(1).strip()

    # Return from first SELECT keyword
    match = re.search(r'\bSELECT\b', text, re.IGNORECASE)
    if match:
        return text[match.start():].strip()

    return ""

def validate_tables_and_columns(query: str) -> tuple[bool, str]:
    with open(os.path.join(SEMANTIC_LAYER_DIR, "schema_context.json")) as f:
        schema = json.load(f)

    parsed = sqlglot.parse_one(query)

    tables = {t.name for t in parsed.find_all(sqlglot.exp.Table)}
    columns = {c.name for c in parsed.find_all(sqlglot.exp.Column)}

    defined_tables = set(schema["tables"].keys())
    
    defined_columns = set()
    for column in schema["tables"].values():
        defined_columns.update(column)

    invalid_tables = tables - defined_tables
    if invalid_tables:
        return False, error_msgs["table_not_defined_error"]
    
    invalid_columns = columns - defined_columns
    if invalid_columns:
        return False, error_msgs["column_not_defined_error"]

    return True, error_msgs["valid_query"]