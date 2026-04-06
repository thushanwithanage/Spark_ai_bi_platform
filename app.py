import streamlit as st
import time

from ai_engine.nl_to_sql import generate_sql
from validation.sql_validator import validate_query
from validation.rbac_validator import validate_rbac
from execution.spark_executor import execute_sql
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

st.set_page_config(page_title="AI BI Platform", layout="wide")
st.title("Governed AI BI Platform")

user_question = st.text_area("Ask a business question")
run_button = st.button("Run Query")

if run_button and user_question:
    max_retries = 2
    i = 0
    ai_error = False
    message = ""

    user_context = {
        "user_id": "u123",
        "role": "analyst"
    }

    try:
        with st.spinner("Generating SQL..."):
            sql_query = generate_sql(user_question)

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

        st.subheader("SQL")
        st.code(sql_query, language="sql")

        # RBAC validation
        try:
            columns_list = extract_columns_with_tables(sql_query)
            rbac_status, error_msg = validate_rbac(columns_list, user_context["role"])
            if not rbac_status:
                st.error(f"Permission error: {error_msg}")
                raise RuntimeError(error_msg)
        except Exception as e:
            st.error(f"RBAC validation failed: {str(e)}")
            raise RuntimeError("RBAC validation failed")

        # Retry loop
        while i < max_retries:
            if i > 0:
                if ai_error:
                    try:
                        correction_prompt = build_sql_correction_prompt(user_question, sql_query, message)
                        sql_query = generate_sql(correction_prompt)
                    except Exception:
                        st.warning("SQL correction failed, using last query")
                else:
                    st.warning(f"Please fix the question: {message}")
                    break

            # Validate SQL
            try:
                is_valid, ai_error, message = validate_query(sql_query)
            except Exception:
                is_valid, ai_error, message = False, True, "SQL validation failed"

            if is_valid:
                try:
                    with st.spinner("Executing query..."):
                        start_time = time.time()
                        df = execute_sql(sql_query)
                        pandas_df = df.limit(1000).toPandas()
                        end_time = time.time()

                    execution_time = round(end_time - start_time, 2)
                    st.success(f"Query executed in {execution_time}s")
                    st.subheader("Results")
                    st.dataframe(pandas_df, use_container_width=True)
                    break

                except Exception as e:
                    st.error(f"Execution error: {str(e)}")
                    break
            else:
                i += 1

        if i == max_retries:
            st.error(f"Error: {message}")

    except Exception as e:
        st.error(f"Processing failed: {str(e)}")