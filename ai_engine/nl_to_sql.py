import json
import os
from dotenv import load_dotenv
from openai import OpenAI
from config.file_path import SEMANTIC_LAYER_DIR
from prompts.sql_generation_prompt import build_sql_generation_prompt

load_dotenv()

client = OpenAI(
    api_key=os.getenv("API_KEY"),
    base_url=os.getenv("BASE_URL"),
)

def generate_sql(user_question) -> str:
    try:
        if not user_question or not isinstance(user_question, str):
            raise ValueError("Invalid user question")
        
        if not os.getenv("API_KEY") or not os.getenv("BASE_URL"):
            raise EnvironmentError("Environmental variables not set")

        try:
            with open(os.path.join(SEMANTIC_LAYER_DIR, "schema_context.json")) as f:
                schema = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError("Schema_context.json file not found")
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in schema_context.json file")
        
        try:
            with open(os.path.join(SEMANTIC_LAYER_DIR, "metrics_catalog.json")) as f:
                metrics = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError("mMtrics_catalog.json file not found")
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in metrics_catalog.json file")
        
        try:
            prompt = build_sql_generation_prompt(schema, metrics, user_question)
        except Exception as e:
            raise Exception(f"Prompt generation failed {e}")

        try:
            response = client.chat.completions.create(
                model=os.getenv("MODEL"),
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
        except Exception as e:
            raise Exception(f"Response generation failed {e}")
        try:
            sql =  response.choices[0].message.content
            if not sql:
                raise ValueError("Empty response from LLM")
            return sql
        except Exception:
            raise Exception("Error in LLM response")
    
    except Exception as e:
        raise Exception(f"Unkonwn error {e}")