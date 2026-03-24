def build_sql_correction_prompt(user_question: str, sql_query: str, error_message: str) -> str:
    return f"""
        The SQL query generated previously is invalid.

        User question:
        {user_question}

        SQL:
        {sql_query}

        Validation Error:
        {error_message}

        Please correct the SQL query and return only the valid SQL statement.
        """