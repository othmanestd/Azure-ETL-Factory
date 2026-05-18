# Databricks notebook source

# MAGIC %md
# MAGIC # Gold Layer - Business Aggregations
# MAGIC Daily store performance, category trends, regional revenue.

# COMMAND ----------

from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, countDistinct,
    round as spark_round, current_timestamp, when, lit
)

# COMMAND ----------

SILVER_PATH = "/mnt/datalake/silver/retail_sales_clean"
GOLD_STORE_PATH = "/mnt/datalake/gold/store_daily_performance"
GOLD_CATEGORY_PATH = "/mnt/datalake/gold/category_trends"
GOLD_REGIONAL_PATH = "/mnt/datalake/gold/regional_revenue"

silver_df = spark.read.format("delta").load(SILVER_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Daily Performance

# COMMAND ----------

store_daily = (
    silver_df
    .groupBy("sale_date_key", "store_id", "store_name", "region_clean", "country_clean")
    .agg(
        spark_round(spark_sum("total_amount"), 2).alias("gross_revenue"),
        spark_round(spark_sum("net_amount"), 2).alias("net_revenue"),
        count("*").alias("transaction_count"),
        countDistinct("customer_id").alias("unique_customers"),
        spark_round(avg("total_amount"), 2).alias("avg_basket_size"),
        spark_sum(when(col("is_discounted"), 1).otherwise(0)).alias("discounted_sales"),
        spark_sum(when(col("channel") == "online", 1).otherwise(0)).alias("online_sales"),
    )
    .withColumn("discount_rate", spark_round(col("discounted_sales") / col("transaction_count") * 100, 1))
    .withColumn("online_rate", spark_round(col("online_sales") / col("transaction_count") * 100, 1))
    .withColumn("_calculated_at", current_timestamp())
)

store_daily.write.format("delta").mode("overwrite").partitionBy("sale_date_key").save(GOLD_STORE_PATH)
print(f"Store daily: {store_daily.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Category Trends (Monthly)

# COMMAND ----------

category_monthly = (
    silver_df
    .groupBy("sale_month", "category")
    .agg(
        spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        count("*").alias("units_sold"),
        countDistinct("customer_id").alias("unique_buyers"),
        spark_round(avg("unit_price"), 2).alias("avg_selling_price"),
    )
    .orderBy("sale_month", col("total_revenue").desc())
)

category_monthly.write.format("delta").mode("overwrite").save(GOLD_CATEGORY_PATH)
category_monthly.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regional Revenue Summary

# COMMAND ----------

regional = (
    silver_df
    .groupBy("region_clean", "country_clean")
    .agg(
        spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        count("*").alias("total_transactions"),
        countDistinct("store_id").alias("active_stores"),
        countDistinct("customer_id").alias("unique_customers"),
        spark_round(avg("total_amount"), 2).alias("avg_transaction_value"),
    )
    .orderBy(col("total_revenue").desc())
)

regional.write.format("delta").mode("overwrite").save(GOLD_REGIONAL_PATH)
regional.show(truncate=False)
