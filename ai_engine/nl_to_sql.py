import json
import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

client = OpenAI(
    api_key=os.getenv("API_KEY"),
    base_url=os.getenv("BASE_URL"),
)

def generate_sql(user_question):

    with open("semantic_layer/schema_context.json") as f:
        schema = json.load(f)

    with open("semantic_layer/metrics_catalog.json") as f:
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
    """

    response = client.chat.completions.create(
        model=os.getenv("MODEL"),
        messages=[
            {"role": "user", "content": prompt}
        ]
    )
    
    return response.choices[0].message.content