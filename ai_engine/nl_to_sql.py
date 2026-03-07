import json
import os
from openai import OpenAI
from config.file_path import SEMANTIC_LAYER_DIR

client = OpenAI(
    api_key=os.getenv("API_KEY"),
    base_url=os.getenv("BASE_URL"),
)

def generate_sql(user_question):
    with open(os.path.join(SEMANTIC_LAYER_DIR, "schema_context.json")) as f:
        schema = json.load(f)
    with open(os.path.join(SEMANTIC_LAYER_DIR, "metrics_catalog.json")) as f:
        metrics = json.load(f)
    
    prompt = f"""
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
    """

    response = client.chat.completions.create(
        model=os.getenv("MODEL"),
        messages=[
            {"role": "user", "content": prompt}
        ]
    )
    
    return response.choices[0].message.content