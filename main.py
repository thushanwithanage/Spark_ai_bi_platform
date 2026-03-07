from ai_engine.nl_to_sql import generate_sql
from validation.sql_validator import validate_query
from execution.spark_executor import execute_sql

user_query = "Select top 5 rows by revenue in the EMEA region for the year 2023"

sql_query = generate_sql(user_query)
print(sql_query)
is_valid, message = validate_query(sql_query)

if is_valid: 
    df = execute_sql(sql_query)
    df.show(5)
else:    
    print(f"Invalid SQL Query: {message}")