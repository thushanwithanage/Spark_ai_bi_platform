from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, round

spark = SparkSession.builder.appName("Gold Aggregation").getOrCreate()

# Read from Silver layer
df = spark.read.parquet(
    "/workspace/spark_governed_ai_bi_platform/data/silver/transactions"
)

# Revenue by Region & Month table
revenue_by_region = df.groupBy("region", "year", "month") \
    .agg(
        round(sum("revenue"), 2).alias("total_revenue"),
        round(sum("cogs"), 2).alias("total_cogs"),
        round((sum("revenue") - sum("cogs")), 2).alias("gross_profit"),
        count("transaction_id").alias("total_transactions")
    )

revenue_by_region.write \
    .mode("overwrite") \
    .parquet("/workspace/spark_governed_ai_bi_platform/data/gold/revenue_by_region")

revenue_by_region.createOrReplaceTempView("revenue_by_region")

# Revenue by Channel & Month table
revenue_by_channel = df.groupBy("channel", "year", "month") \
    .agg(
        round(sum("revenue"), 2).alias("total_revenue"),
        round(avg("unit_price"), 2).alias("avg_unit_price"),
        count("transaction_id").alias("total_transactions")
    )

revenue_by_channel.write \
    .mode("overwrite") \
    .parquet("/workspace/spark_governed_ai_bi_platform/data/gold/revenue_by_channel")

revenue_by_channel.createOrReplaceTempView("revenue_by_channel")

# Customer Analytics table
customer_summary = df.groupBy("customer_id") \
    .agg(
        count("transaction_id").alias("total_orders"),
        round(sum("revenue"), 2).alias("total_spent"),
        round(avg("revenue"), 2).alias("avg_order_value")
    )

customer_summary.write \
    .mode("overwrite") \
    .parquet("/workspace/spark_governed_ai_bi_platform/data/gold/customer_summary")

customer_summary.createOrReplaceTempView("customer_summary")

print("Gold layer created successfully")
spark.stop()