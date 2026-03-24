import re
import json
import os
import sqlglot
from config.file_path import CONFIG_PATH
from validation.tables_and_columns_validator import extract_columns_with_tables, validate_columns_and_tables

with open(os.path.join(CONFIG_PATH, "error_msgs.json"), "r") as f:
    error_msgs = json.load(f)
    
def validate_query(query: str) -> tuple[bool, bool, str]:
    if not query or not query.strip():
        return False, True, error_msgs["empty_query_error"]
    
    query = query.strip()

    query = extract_sql_query(query)
    if not query:
        return False, True, error_msgs["invalid_ai_sql_error"]
    
    if not re.match(r'^\s*SELECT\b', query, re.IGNORECASE):
        return False, True, error_msgs["select_not_found_error"]
    
    reserved_keywords = ["DROP", "DELETE", "ALTER", "TRUNCATE", "GRANT", "REVOKE", "CREATE", "USE", "EXECUTE", "MERGE", "CALL", "EXPLAIN"]
    
    if any(value in query.upper() for value in reserved_keywords):
        return False, True, error_msgs["reserved_keywords_error"]
    
    if "SELECT *" in query.upper():
        return False, True, error_msgs["select_all_error"]

    if query.count('(') != query.count(')'):
        return False, True, error_msgs["unbalanced_parentheses_error"]

    rstrip_query = query.rstrip(';')
    if ';' in rstrip_query:
        return False, True, error_msgs["multiple_statements_error"]
    
    parsed_query = sqlglot.parse_one(rstrip_query)
    columns_list = extract_columns_with_tables(parsed_query)
    is_valid_schema, message = validate_columns_and_tables(columns_list)

    if not is_valid_schema:
        return False, True, message
    
    is_valid_limit, message = validate_limit(parsed_query)
    if not is_valid_limit:
        return False, False, message
    
    is_valid_join_count, message = validate_join_count(parsed_query) 
    if not is_valid_join_count:
        return False, False, message
    
    is_valid_filter, message = validate_filter(parsed_query)
    return is_valid_filter, True, message

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