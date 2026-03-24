import json
import os
from openai import OpenAI
from config.file_path import SEMANTIC_LAYER_DIR
from prompts.sql_generation_prompt import build_sql_generation_prompt
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(
    api_key=os.getenv("API_KEY"),
    base_url=os.getenv("BASE_URL"),
)



def generate_sql(user_question):
    with open(os.path.join(SEMANTIC_LAYER_DIR, "schema_context.json")) as f:
        schema = json.load(f)
    with open(os.path.join(SEMANTIC_LAYER_DIR, "metrics_catalog.json")) as f:
        metrics = json.load(f)
    
    prompt = build_sql_generation_prompt(schema, metrics, user_question)

    response = client.chat.completions.create(
        model=os.getenv("MODEL"),
        messages=[
            {"role": "user", "content": prompt}
        ]
    )
    
    return response.choices[0].message.content