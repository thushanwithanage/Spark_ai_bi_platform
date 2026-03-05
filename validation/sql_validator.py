def validate_query(query):
    unacceptable_values = ["DROP", "DELETE", "ALTER", "TRUNCATE", "GRANT", "REVOKE", "CREATE", "USE", "EXECUTE", "MERGE", "CALL", "EXPLAIN"]
    
    if any(value in query.upper() for value in unacceptable_values):
        return False, "Reserved keywords found in query"
    
    if "SELECT *" in query.upper():
        return False, "SELECT * found in query"
    
    return True, "Valid query"