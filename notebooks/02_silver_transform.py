# Databricks notebook source

# MAGIC %md
# MAGIC # Silver Layer - Clean & Enrich Retail Sales
# MAGIC Dedup, validate, join with dimension tables, standardize.

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, lit, row_number, to_date, to_timestamp,
    concat, date_format, current_timestamp, upper, trim, coalesce
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("run_date", "2024-01-01")
run_date = dbutils.widgets.get("run_date")

BRONZE_PATH = "/mnt/datalake/bronze/retail_sales_delta"
SILVER_PATH = "/mnt/datalake/silver/retail_sales_clean"

# COMMAND ----------

bronze_df = (
    spark.read.format("delta").load(BRONZE_PATH)
    .filter(col("_run_date") == run_date)
)
print(f"Bronze records for {run_date}: {bronze_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication

# COMMAND ----------

dedup_window = Window.partitionBy("sale_id").orderBy(col("_ingestion_timestamp").desc())
deduped_df = (
    bronze_df
    .withColumn("_rn", row_number().over(dedup_window))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation & Enrichment

# COMMAND ----------

silver_df = (
    deduped_df
    .withColumn("sale_timestamp", to_timestamp(concat(col("sale_date"), lit(" "), col("sale_time"))))
    .withColumn("sale_date_key", date_format(col("sale_timestamp"), "yyyy-MM-dd"))
    .withColumn("sale_month", date_format(col("sale_timestamp"), "yyyy-MM"))
    .withColumn("sale_year", date_format(col("sale_timestamp"), "yyyy"))
    .withColumn("day_of_week", date_format(col("sale_timestamp"), "EEEE"))
    .withColumn("region_clean", upper(trim(col("region"))))
    .withColumn("country_clean", upper(trim(col("country"))))
    .withColumn("is_discounted", when(col("discount_pct") > 0, True).otherwise(False))
    .withColumn("net_amount", col("total_amount") * (1 - col("discount_pct")))
    .withColumn("_processed_at", current_timestamp())
    .filter(col("total_amount") > 0)
    .filter(col("quantity") > 0)
    .drop("_source_file", "_run_date", "_ingestion_timestamp", "sale_time")
)

# COMMAND ----------

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    delta_table = DeltaTable.forPath(spark, SILVER_PATH)
    (
        delta_table.alias("t")
        .merge(silver_df.alias("s"), "t.sale_id = s.sale_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    silver_df.write.format("delta").partitionBy("sale_month").save(SILVER_PATH)

print(f"Silver total: {spark.read.format('delta').load(SILVER_PATH).count()}")
