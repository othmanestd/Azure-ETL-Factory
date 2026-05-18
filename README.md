# 🏭 Azure-ETL-Factory — Batch Pipeline with ADF

<p align="center">
  <img src="https://img.shields.io/badge/Azure_Data_Factory-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white" alt="ADF"/>
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white" alt="Databricks"/>
  <img src="https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=delta&logoColor=white" alt="Delta Lake"/>
  <img src="https://img.shields.io/badge/Synapse-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white" alt="Synapse"/>
  <img src="https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black" alt="Power BI"/>
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
</p>

## 🎯 Project Overview

Batch ETL pipeline for **retail sales data** orchestrated by **Azure Data Factory**. ADF copies data from Azure SQL to the Data Lake, triggers Databricks notebooks for Bronze/Silver/Gold transformations, and refreshes Power BI dashboards via Synapse Serverless SQL.

## 🏗️ Architecture

```mermaid
flowchart LR
    subgraph Source ["🗄️ Source"]
        SQL[(Azure SQL\nRetail POS Data)]
    end

    subgraph ADF ["⚙️ Azure Data Factory"]
        direction TB
        T1["🔍 Validate\n(Lookup Activity)"]
        T2["📋 Copy to Bronze\n(Copy Activity)"]
        T3["🔄 Silver Transform\n(Databricks Notebook)"]
        T4["📊 Gold Aggregate\n(Databricks Notebook)"]
        T5["📈 Refresh Power BI\n(Web Activity)"]
        T1 --> T2 --> T3 --> T4 --> T5
    end

    subgraph Lake ["💾 Data Lake Gen2"]
        B["🥉 Bronze\n(Parquet)"]
        S["🥈 Silver\n(Delta Lake)"]
        G["🥇 Gold\n(Delta Lake)"]
    end

    subgraph Serve ["📊 Serving"]
        SYN[Synapse SQL]
        PBI[Power BI]
    end

    SQL --> T1
    T2 --> B
    T3 --> S
    T4 --> G
    G --> SYN --> PBI

    style B fill:#CD7F32,color:#fff
    style S fill:#C0C0C0,color:#000
    style G fill:#FFD700,color:#000
```

## 🚀 Features

- ⚙️ **ADF orchestration** with Lookup, Copy, Databricks, and Web activities
- 🔄 **Retry & monitoring** — 2 retries, 5-min interval, email alerts on failure
- 🔐 **Secure** — Azure Key Vault for secrets, Managed Identity authentication
- 📅 **Parameterized** — `run_date` enables backfill for historical reprocessing
- 🔄 **Idempotent** — Delta Lake MERGE prevents duplicates on re-runs
- 📊 **Auto-refresh** — triggers Power BI dataset refresh after Gold completion

## 🛠️ ADF Pipeline Design

```
[Validate Source] → [Copy to Bronze] → [Silver Transform] → [Gold Aggregate] → [Refresh Power BI]
     Lookup            Copy Activity     Databricks Notebook  Databricks Notebook    Web Activity
```

## 📂 Project Structure

```
Azure-ETL-Factory/
├── 📁 adf/
│   ├── pipelines/
│   │   └── pipeline_daily_etl.json     # ADF pipeline definition
│   ├── datasets/
│   │   ├── ds_source_sql.json          # Source dataset
│   │   └── ds_bronze_parquet.json      # Bronze dataset
│   └── linkedservices/
│       ├── ls_source_sqldb.json        # SQL linked service
│       ├── ls_datalake.json            # Data Lake linked service
│       └── ls_databricks.json          # Databricks linked service
├── 📁 notebooks/
│   ├── 01_bronze_ingestion.py          # Parquet to Delta Lake
│   ├── 02_silver_transform.py          # Dedup + Enrich + MERGE
│   └── 03_gold_aggregate.py            # Store + Category + Regional
├── 📁 sql/
│   └── create_views.sql                # Synapse SQL views for Power BI
├── 📁 tests/
│   └── test_transformations.py
└── requirements.txt
```

## 📈 Gold Layer Outputs

| Output | Metrics |
|--------|---------|
| 🏪 **Store Daily** | Gross/net revenue, transactions, basket size, discount & online rates |
| 📦 **Category Trends** | Monthly revenue, units sold, avg selling price by category |
| 🌍 **Regional Revenue** | Revenue by region/country, active stores, unique customers |

## ⚙️ Setup & Run

```bash
pip install -r requirements.txt
python src/generators/retail_generator.py
pytest tests/ -v
```

## 👨‍💻 Author

**Othmane Sadiki** — Data Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/sadiki-othmane/)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/othmanestd)
