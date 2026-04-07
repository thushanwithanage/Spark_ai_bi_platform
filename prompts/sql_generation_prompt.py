def build_sql_generation_prompt(schema, metrics, user_question):
    return f"""
    Generate Spark SQL.

    Available tables:
    {schema}

    Available metrics:
    {metrics}

    User Question:
    {user_question}

    Rules:
    - Only use listed tables
    - Only use listed columns
    - Do not use SELECT *
    - Output only SQL without SQL quotes
    - Do not return schema names in the output
    - Return an error when the query does not align with the defined schema
    """