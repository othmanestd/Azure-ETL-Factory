# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze Layer - Raw Sales Ingestion
# MAGIC Reads Parquet files dropped by ADF Copy Activity into Delta Lake.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, lit

# COMMAND ----------

dbutils.widgets.text("run_date", "2024-01-01")
run_date = dbutils.widgets.get("run_date")

LANDING_PATH = f"/mnt/datalake/bronze/retail/sales/{run_date}/"
BRONZE_PATH = "/mnt/datalake/bronze/retail_sales_delta"

# COMMAND ----------

raw_df = (
    spark.read.format("parquet").load(LANDING_PATH)
    .withColumn("_source_file", input_file_name())
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_run_date", lit(run_date))
)

print(f"Loaded {raw_df.count()} records for {run_date}")
raw_df.printSchema()

# COMMAND ----------

(
    raw_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .partitionBy("sale_date")
    .save(BRONZE_PATH)
)

print(f"Bronze total: {spark.read.format('delta').load(BRONZE_PATH).count()}")
