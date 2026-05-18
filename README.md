# Azure-ETL-Factory

## Overview
Batch ETL pipeline for retail sales data, orchestrated by **Azure Data Factory**
with **Databricks** transformations and **Power BI** dashboards served via **Synapse Serverless SQL**.

## Architecture
![Architecture](architecture/architecture.png)

**Pipeline Flow:**
1. **Trigger** - ADF daily schedule at 06:00 UTC (or event-based)
2. **Validate** - Lookup activity checks source data availability
3. **Extract** - Copy Activity moves data from Azure SQL to Data Lake (Parquet)
4. **Transform Bronze** - Databricks notebook reads Parquet into Delta Lake
5. **Transform Silver** - Dedup, validate, enrich, MERGE upsert
6. **Aggregate Gold** - Store performance, category trends, regional revenue
7. **Serve** - Synapse Serverless SQL views for Power BI
8. **Refresh** - Web Activity triggers Power BI dataset refresh

## Tech Stack
| Component | Technology |
|-----------|-----------|
| Orchestration | Azure Data Factory (pipelines, triggers, monitoring) |
| Source | Azure SQL Database |
| Storage | Azure Data Lake Gen2 (Parquet + Delta Lake) |
| Processing | Databricks (Apache Spark), PySpark |
| Serving | Azure Synapse Analytics (Serverless SQL) |
| Visualization | Power BI |
| Security | Azure Key Vault, Managed Identity |

## ADF Pipeline Design
```
[Validate Source] --> [Copy to Bronze] --> [Silver Transform] --> [Gold Aggregate] --> [Refresh Power BI]
     Lookup            Copy Activity       Databricks Notebook   Databricks Notebook    Web Activity
```

- **Retry policy:** 2 retries, 5-minute interval
- **Idempotent:** Delta Lake MERGE prevents duplicates on re-run
- **Parameterized:** run_date parameter enables backfill

## Project Structure
```
Azure-ETL-Factory/
|-- adf/
|   |-- pipelines/
|   |   +-- pipeline_daily_etl.json
|   |-- datasets/
|   |   |-- ds_source_sql.json
|   |   +-- ds_bronze_parquet.json
|   +-- linkedservices/
|       |-- ls_source_sqldb.json
|       |-- ls_datalake.json
|       +-- ls_databricks.json
|-- notebooks/
|   |-- 01_bronze_ingestion.py
|   |-- 02_silver_transform.py
|   +-- 03_gold_aggregate.py
|-- src/generators/
|   +-- retail_generator.py
|-- sql/
|   +-- create_views.sql
|-- tests/
|   +-- test_transformations.py
|-- config/
|   +-- pipeline_config.yaml
+-- requirements.txt
```

## Setup & Run
```bash
pip install -r requirements.txt
python src/generators/retail_generator.py
pytest tests/ -v
```

## Gold Layer Outputs
- **Store Daily Performance:** revenue, transactions, basket size, discount/online rates
- **Category Trends:** monthly revenue, units sold, avg selling price by category
- **Regional Revenue:** revenue by region/country with customer metrics
- **KPI Summary:** aggregated dashboard header metrics

## Author
**Othmane Sadiki** - [LinkedIn](https://www.linkedin.com/in/sadiki-othmane/) - othmanesadiki6114@gmail.com
