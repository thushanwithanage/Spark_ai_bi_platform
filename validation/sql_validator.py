import re

def validate_query(query: str) -> tuple[bool, str]:
    if not query or not query.strip():
        return False, "Empty query"
    
    query = query.strip()

    query = validate_sql(query)
    if not query:
        return False, "Invalid SQL in AI response"
    
    if not re.match(r'^\s*SELECT\b', query, re.IGNORECASE):
        return False, "Query not starting with SELECT"
    
    unacceptable_values = ["DROP", "DELETE", "ALTER", "TRUNCATE", "GRANT", "REVOKE", "CREATE", "USE", "EXECUTE", "MERGE", "CALL", "EXPLAIN"]
    
    if any(value in query.upper() for value in unacceptable_values):
        return False, "Reserved keywords found in query"
    
    if "SELECT *" in query.upper():
        return False, "SELECT * found in query"

    if query.count('(') != query.count(')'):
        return False, "Unbalanced parentheses in query"

    clean = query.rstrip(';')
    if ';' in clean:
        return False, "Multiple SQL statements in query"
    
    return True, "Valid query"

def validate_sql(text: str) -> str:
    fenced = re.search(r'```(?:sql)?\s*(.*?)```', text, re.DOTALL | re.IGNORECASE)
    if fenced:
        return fenced.group(1).strip()

    # Return from first SELECT keyword
    match = re.search(r'\bSELECT\b', text, re.IGNORECASE)
    if match:
        return text[match.start():].strip()

    return ""