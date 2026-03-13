from ai_engine.nl_to_sql import generate_sql
from validation.sql_validator import validate_query
from execution.spark_executor import execute_sql
from config.query_logger import log_query
from prompts.sql_correction_prompt import build_sql_correction_prompt
import time

user_question = "Top 5 regions by revenue in 2023"
max_retries = 2
i = 0

while i < max_retries:

    if i == 0:
        sql_query = generate_sql(user_question)
    else:
        correction_prompt = build_sql_correction_prompt(sql_query, message)
        sql_query = generate_sql(correction_prompt)

    print(sql_query)

    is_valid, message = validate_query(sql_query)

    if is_valid:
        start_time = time.time()

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
        print(f"Invalid SQL Query: {message}")

        log_query(
            question=user_question,
            sql=sql_query,
            status="failed",
            message=message
        )

        i += 1