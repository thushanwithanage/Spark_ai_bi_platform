import time

from ai_engine.nl_to_sql import generate_sql
from validation.sql_validator import validate_query
from validation.rbac_validator import validate_rbac
from execution.spark_executor import execute_sql
from config.query_logger import log_query
from prompts.sql_correction_prompt import build_sql_correction_prompt
from validation.tables_and_columns_validator import extract_columns_with_tables
from optimization.predicate_pushdown_optimizer import apply_predicate_pushdown
from optimization.sub_query_optimizer import (
    contains_subquery,
    find_unused_subquery_columns,
    remove_unused_subquery_columns,
    replace_subquery_in_query,
    remove_unused_joins
)

user_question = "Top 5 records by revenue in EMEA region"
max_retries = 2
i = 0
ai_error = False
message = ""

user_context = {"user_id": "u123", "role": "analyst"}

try:
    # Generate SQL
    try:
        sql_query = generate_sql(user_question)
    except Exception as e:
        raise RuntimeError(f"SQL generation failed: {e}")

    # Optimize SQL
    try:
        sql_query = apply_predicate_pushdown(sql_query)
    except Exception:
        pass

    try:
        if contains_subquery(sql_query):
            subquery_cols, outer_refs = find_unused_subquery_columns(sql_query)
            sub_query = remove_unused_subquery_columns(sql_query, subquery_cols - outer_refs)
            sql_query = replace_subquery_in_query(sql_query, sub_query)
            sql_query = remove_unused_joins(sql_query)
    except Exception:
        pass

    # Extract query columns list
    try:
        columns_list = extract_columns_with_tables(sql_query)
    except Exception:
        columns_list = []

    # RBAC check
    try:
        rbac_status, error_msg = validate_rbac(columns_list, user_context["role"])
    except Exception as e:
        rbac_status, error_msg = False, f"RBAC check failed: {e}"

    if rbac_status:
        while i < max_retries:
            if i > 0:
                if ai_error:
                    try:
                        correction_prompt = build_sql_correction_prompt(user_question, sql_query, message)
                        sql_query = generate_sql(correction_prompt)
                    except Exception:
                        print("SQL correction failed, using last query")
                else:
                    log_query(question=user_question, sql=sql_query, status="failed", message=message)
                    print(f"Please update the question to fix the error: {message}")
                    break

            # Validate query
            try:
                is_valid, ai_error, message = validate_query(sql_query)
            except Exception:
                is_valid, ai_error, message = False, True, "SQL validation failed"

            if is_valid:
                try:
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
                except Exception as e:
                    print(f"Query execution failed: {e}")
                    log_query(question=user_question, sql=sql_query, status="failed", message=str(e))
                    break
            else:
                log_query(question=user_question, sql=sql_query, status="failed", message=message)
                i += 1

        if i == max_retries:
            print(f"Error: {message}")
    else:
        print(f"Permission error on data: {error_msg}")

except Exception as e:
    print(f"Processing failed: {e}")