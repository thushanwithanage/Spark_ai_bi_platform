from ai_engine.nl_to_sql import generate_sql
from validation.sql_validator import validate_query
from execution.spark_executor import execute_sql
from config.query_logger import log_query
import time

user_query = "Select top 5 rows by service category"

sql_query = generate_sql(user_query)
print(sql_query)

is_valid, message = validate_query(sql_query)

if is_valid:
    start_time = time.time()
    df = execute_sql(sql_query)
    end_time = time.time()
    execution_time = round(end_time - start_time, 2)
    df.show(5)

    log_query(
        question=user_query,
        sql=sql_query,
        status="success",
        execution_time=execution_time
    )

else:
    print(f"Invalid SQL Query: {message}")
    log_query(
        question=user_query,
        sql=sql_query,
        status="failed",
        message=message
    )