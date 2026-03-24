import os
from pyspark.sql import SparkSession
from config.file_path import DATA_PATH

spark = SparkSession.builder.appName("AI Query Engine").getOrCreate()

def load_tables():
    loaded = []
    failed = []

    for table_name in os.listdir(DATA_PATH):
        table_path = os.path.join(DATA_PATH, table_name)
        
        if not os.path.isdir(table_path):
            continue
        
        try:
            spark.read.parquet(table_path).createOrReplaceTempView(table_name)
            loaded.append(table_name)
        except Exception as e:
            failed.append((table_name, str(e)))

    if failed:
        print(f"Failed to load: {failed}")

def execute_sql(query):
    df = spark.sql(query)
    return df

load_tables()