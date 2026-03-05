from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Gold Analysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("/workspace/spark_governed_ai_bi_platform/data/gold/customer_summary")

# Dataset shape
print(f"Rows: {df.count()}, Columns: {len(df.columns)}")

df.show(5)

spark.stop()