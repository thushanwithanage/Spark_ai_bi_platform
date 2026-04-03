import os
import streamlit as st
from pyspark.sql import SparkSession
from config.file_path import DATA_PATH

@st.cache_resource(show_spinner="Initializing Spark Engine...")
def get_spark_session():
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
    
    spark = SparkSession.builder.getOrCreate()
    
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
        
    return spark

def execute_sql(query):
    spark = get_spark_session()
    df = spark.sql(query)
    return df