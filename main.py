from ai_engine.nl_to_sql import generate_sql
from validation.sql_validator import validate_query
from validation.rbac_validator import validate_rbac
from execution.spark_executor import execute_sql
from config.query_logger import log_query
from prompts.sql_correction_prompt import build_sql_correction_prompt
from validation.tables_and_columns_validator import extract_columns_with_tables
import time

user_question = "Top 5 records by revenue in EMEA region"
max_retries = 2
i = 0
ai_error = False

user_context = {
    "user_id": "u123",
    "role": "analyst"
}

# Generate SQL
sql_query = generate_sql(user_question)

# Extract query columns list
columns_list = extract_columns_with_tables(sql_query)

# RBAC check
rbac_status, error_msg = validate_rbac(columns_list, user_context["role"])
rbac_status, error_msg = True, ""

if rbac_status is not False:
    while i < max_retries:
        if i > 0 and ai_error:
            correction_prompt = build_sql_correction_prompt(user_question, sql_query, message)
            sql_query = generate_sql(correction_prompt)
        elif i > 0 and not ai_error:
            log_query(
                question=user_question,
                sql=sql_query,
                status="failed",
                message=message
            )
            print(f"Please update the question to fix the error. {message}")
            break
        
        # Validate query
        is_valid, ai_error, message = validate_query(sql_query)

        if is_valid:
            start_time = time.time()

            # Execute query
            df = execute_sql(sql_query)

            end_time = time.time()
            execution_time = round(end_time - start_time, 2)

            df.show(5)

            log_query(
                question=user_question,
                sql=sql_query,
                status="success",
                execution_time=execution_time
            )
            break

        else:
            log_query(
                question=user_question,
                sql=sql_query,
                status="failed",
                message=message
            )

            i += 1
    if i == max_retries:
        print(f"Error : {message}")
else:
    print(error_msg)