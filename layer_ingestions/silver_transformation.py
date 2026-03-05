from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Silver Transformation").getOrCreate()

# Read from Bronze layer
df = spark.read.parquet(
    "/workspace/spark_governed_ai_bi_platform/data/bronze/transactions"
)

# Drop duplicates
df = df.dropDuplicates(["transaction_id"])

# Drop nulls
df = df.dropna(subset=["transaction_id", "transaction_date", "customer_id"])

# Filter invalid values
df = df.filter(col("quantity") > 0) \
       .filter(col("unit_price") > 0) \
       .filter(col("revenue") > 0)

# Write to Silver layer
df.write \
  .mode("overwrite") \
  .partitionBy("year", "month") \
  .parquet("/workspace/spark_governed_ai_bi_platform/data/silver/transactions")

print("Silver layer created successfully")
spark.stop()