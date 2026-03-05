from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month

spark = SparkSession.builder.appName("Bronze Ingestion").getOrCreate()

# Read from Raw
df = spark.read.csv(
    "/workspace/spark_governed_ai_bi_platform/data/raw/transactions.csv",
    header=True,
    inferSchema=True
)

df = df.withColumn("year", year("transaction_date")) \
       .withColumn("month", month("transaction_date"))

# Write to Bronze layer
df.write \
  .mode("overwrite") \
  .partitionBy("year", "month") \
  .parquet("/workspace/spark_governed_ai_bi_platform/data/bronze/transactions")

print("Bronze layer created successfully")
spark.stop()