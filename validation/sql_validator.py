import re
import json
import os
import sqlglot
from sqlglot import exp
from config.file_path import SEMANTIC_LAYER_DIR, CONFIG_PATH

with open(os.path.join(CONFIG_PATH, "error_msgs.json"), "r") as f:
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
    
    parsed_query = sqlglot.parse_one(rstrip_query)
    is_valid_schema, message = validate_table_and_column_names(parsed_query)

    if not is_valid_schema:
        return False, message
    
    is_valid_limit, message = validate_limit(parsed_query)
    if not is_valid_limit:
        return False, message
    
    is_valid_join_count, message = validate_join_count(parsed_query) 
    if not is_valid_join_count:
        return False, message
    
    is_valid_filter, message = validate_filter(parsed_query)
    return is_valid_filter, message

# Extract SQL query to return from first SELECT block
def extract_sql_query(text: str) -> str:
    fenced = re.search(r'```(?:sql)?\s*(.*?)```', text, re.DOTALL | re.IGNORECASE)
    if fenced:
        return fenced.group(1).strip()

    # Return from first SELECT keyword
    match = re.search(r'\bSELECT\b', text, re.IGNORECASE)
    if match:
        return text[match.start():].strip()

    return ""

# Validate table and column names
def validate_table_and_column_names(parsed_query: sqlglot.exp.Query) -> tuple[bool, str]:
    with open(os.path.join(SEMANTIC_LAYER_DIR, "schema_context.json")) as f:
        schema = json.load(f)

    tables = {t.name for t in parsed_query.find_all(sqlglot.exp.Table)}
    columns = {c.name for c in parsed_query.find_all(sqlglot.exp.Column)}

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

# Validate query limit
def validate_limit(parsed_query: sqlglot.exp.Query) -> tuple[bool, str]:
    MAX_LIMIT = 100

    limit = parsed_query.args.get("limit")

    if not limit:
        return False, error_msgs["limit_not_found_error"]

    limit_value = int(limit.expression.name)

    if limit_value > MAX_LIMIT:
        return False, error_msgs["query_limit_exceeded_error"]+ str(MAX_LIMIT)

    return True, error_msgs["valid_query"]

# Validate JOIN count
def validate_join_count(parsed_query: sqlglot.exp.Query) -> tuple[bool, str]:
    MAX_JOINS = 3

    joins = list(parsed_query.find_all(sqlglot.exp.Join))

    if len(joins) > MAX_JOINS:
        return False, error_msgs["join_count_exceeded_error"] + str(MAX_JOINS)

    return True, error_msgs["valid_query"]

# Validate WHERE clause
def validate_filter(parsed_query: str) -> tuple[bool, str]:
    where = parsed_query.args.get("where")

    if not where:
        return False, error_msgs["where_clause_error"]

    return True, error_msgs["valid_query"]