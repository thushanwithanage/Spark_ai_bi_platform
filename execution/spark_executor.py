from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AI Query Engine").getOrCreate()

DATA_PATH = "/workspace/spark_governed_ai_bi_platform/data/gold"

def load_tables():
    """Load gold layer parquet files and register as temp views."""
    spark.read.parquet(f"{DATA_PATH}/revenue_by_region") \
         .createOrReplaceTempView("revenue_by_region")
    
    spark.read.parquet(f"{DATA_PATH}/revenue_by_channel") \
         .createOrReplaceTempView("revenue_by_channel")
    
    spark.read.parquet(f"{DATA_PATH}/customer_summary") \
         .createOrReplaceTempView("customer_summary")
    
    print("Gold tables loaded successfully")

def execute_sql(query):
    df = spark.sql(query)
    return df

load_tables()