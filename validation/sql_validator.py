import re
import json
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(BASE_DIR, "..", "config")

with open(os.path.join(CONFIG_DIR, "error_msgs.json"), "r") as f:
    error_msgs = json.load(f)
    
def validate_query(query: str) -> tuple[bool, str]:
    if not query or not query.strip():
        return False, error_msgs["empty_query_error"]
    
    query = query.strip()

    query = validate_sql(query)
    if not query:
        return False, error_msgs["invalid_ai_sql_error"]
    
    if not re.match(r'^\s*SELECT\b', query, re.IGNORECASE):
        return False, error_msgs["select_not_found_error"]
    
    unacceptable_values = ["DROP", "DELETE", "ALTER", "TRUNCATE", "GRANT", "REVOKE", "CREATE", "USE", "EXECUTE", "MERGE", "CALL", "EXPLAIN"]
    
    if any(value in query.upper() for value in unacceptable_values):
        return False, error_msgs["reserved_keywords_error"]
    
    if "SELECT *" in query.upper():
        return False, error_msgs["select_all_error"]

    if query.count('(') != query.count(')'):
        return False, error_msgs["unbalanced_parentheses_error"]

    clean = query.rstrip(';')
    if ';' in clean:
        return False, error_msgs["multiple_statements_error"]
    
    return True, error_msgs["valid_query"]

def validate_sql(text: str) -> str:
    fenced = re.search(r'```(?:sql)?\s*(.*?)```', text, re.DOTALL | re.IGNORECASE)
    if fenced:
        return fenced.group(1).strip()

    # Return from first SELECT keyword
    match = re.search(r'\bSELECT\b', text, re.IGNORECASE)
    if match:
        return text[match.start():].strip()

    return ""