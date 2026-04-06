import re
import json
import os
import sqlglot
from typing import Tuple
from config.file_path import CONFIG_PATH
from validation.tables_and_columns_validator import extract_columns_with_tables, validate_columns_and_tables

def load_json_safe(filename: str) -> dict:
    path = os.path.join(CONFIG_PATH, filename)
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}

error_msgs = load_json_safe("error_msgs.json")

def validate_query(query: str) -> Tuple[bool, bool, str]:
    if not query or not isinstance(query, str) or not query.strip():
        return False, True, error_msgs.get("empty_query_error", "Query is empty")

    query = query.strip()

    # Extract SQL safely
    query = extract_sql_query(query)
    if not query:
        return False, True, error_msgs.get("invalid_ai_sql_error", "Invalid SQL format")

    # Only allow SELECT statements
    if not re.match(r'^\s*SELECT\b', query, re.IGNORECASE):
        return False, True, error_msgs.get("select_not_found_error", "SELECT statement not found")

    # Reserved keywords check
    reserved_keywords = [
        "DROP", "DELETE", "ALTER", "TRUNCATE", "GRANT",
        "REVOKE", "CREATE", "USE", "EXECUTE", "MERGE",
        "CALL", "EXPLAIN"
    ]
    if any(kw in query.upper() for kw in reserved_keywords):
        return False, True, error_msgs.get("reserved_keywords_error", "Reserved keyword detected")

    if "SELECT *" in query.upper():
        return False, True, error_msgs.get("select_all_error", "SELECT * not allowed")

    if query.count('(') != query.count(')'):
        return False, True, error_msgs.get("unbalanced_parentheses_error", "Unbalanced parentheses")

    rstrip_query = query.rstrip(';')
    if ';' in rstrip_query:
        return False, True, error_msgs.get("multiple_statements_error", "Multiple statements not allowed")

    try:
        parsed_query = sqlglot.parse_one(rstrip_query)
    except Exception:
        return False, True, error_msgs.get("invalid_ai_sql_error", "SQL parsing failed")

    # Validate tables and columns
    try:
        columns_list = extract_columns_with_tables(parsed_query)
        is_valid_schema, message = validate_columns_and_tables(columns_list)
        if not is_valid_schema:
            return False, True, message
    except Exception:
        return False, True, "Schema validation failed"

    # Validate LIMIT
    try:
        is_valid_limit, message = validate_limit(parsed_query)
        if not is_valid_limit:
            return False, False, message
    except Exception:
        return False, False, "Limit validation failed"

    # Validate JOIN count
    try:
        is_valid_join_count, message = validate_join_count(parsed_query)
        if not is_valid_join_count:
            return False, False, message
    except Exception:
        return False, False, "JOIN count validation failed"

    # Validate WHERE clause
    try:
        is_valid_filter, message = validate_filter(parsed_query)
    except Exception:
        return False, False, "WHERE clause validation failed"

    return is_valid_filter, True, message

def extract_sql_query(text: str) -> str:
    if not text or not isinstance(text, str):
        return ""
    try:
        txt = re.search(r'```(?:sql)?\s*(.*?)```', text, re.DOTALL | re.IGNORECASE)
        if txt:
            return txt.group(1).strip()
        match = re.search(r'\bSELECT\b', text, re.IGNORECASE)
        if match:
            return text[match.start():].strip()
    except Exception:
        return ""
    return ""


def validate_limit(parsed_query: sqlglot.exp.Query) -> Tuple[bool, str]:
    MAX_LIMIT = 100
    try:
        limit = parsed_query.args.get("limit")
        if not limit:
            return False, error_msgs.get("limit_not_found_error", "LIMIT not found")
        limit_value = int(getattr(limit.expression, "name", 0))
        if limit_value > MAX_LIMIT:
            return False, error_msgs.get("query_limit_exceeded_error", f"Limit exceeds {MAX_LIMIT}") + str(MAX_LIMIT)
        return True, error_msgs.get("valid_query", "Query valid")
    except Exception:
        return False, "LIMIT validation failed"


def validate_join_count(parsed_query: sqlglot.exp.Query) -> Tuple[bool, str]:
    MAX_JOINS = 3
    try:
        joins = list(parsed_query.find_all(sqlglot.exp.Join))
        if len(joins) > MAX_JOINS:
            return False, error_msgs.get("join_count_exceeded_error", f"JOINs exceed {MAX_JOINS}") + str(MAX_JOINS)
        return True, error_msgs.get("valid_query", "Query valid")
    except Exception:
        return False, "JOIN count validation failed"


def validate_filter(parsed_query: sqlglot.exp.Query) -> Tuple[bool, str]:
    try:
        where = parsed_query.args.get("where")
        if not where:
            return False, error_msgs.get("where_clause_error", "WHERE clause required")
        return True, error_msgs.get("valid_query", "Query valid")
    except Exception:
        return False, "WHERE clause validation failed"