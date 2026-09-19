
# Azure Medallion Data Engineering Project

## 📌 Project Overview

This project demonstrates an end-to-end Azure data engineering workflow using **Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, PySpark, Azure Synapse Analytics, and Power BI**.

The project follows the **Medallion Architecture**, where data is ingested into the Bronze layer, transformed using Databricks and PySpark into the Silver layer, and then exposed through Gold views in Azure Synapse for reporting and analysis.

---

## 🏗️ Architecture

```text
Source Data
     │
     ▼
Azure Data Factory
     │
     ▼
ADLS Gen2 - Bronze Layer
     │
     ▼
Azure Databricks
     │
     │  PySpark Transformation
     ▼
ADLS Gen2 - Silver Layer
     │
     ▼
Azure Synapse Analytics
     │
     │  Gold Views / External Tables
     ▼
Power BI
     │
     ▼
KPI Report
```

---

## 🛠️ Technologies Used

* **Azure Data Factory (ADF)**
* **Azure Data Lake Storage Gen2 (ADLS Gen2)**
* **Azure Databricks**
* **Apache Spark / PySpark**
* **Azure Synapse Analytics**
* **SQL**
* **Power BI**
* **Parquet**
* **GitHub**

---

## 🔄 Project Workflow

### 1. Data Ingestion — Azure Data Factory

Azure Data Factory is used to ingest source datasets into the **Bronze layer** of Azure Data Lake Storage Gen2.

The ADF implementation includes:

* Pipelines
* Datasets
* Linked Services
* Dynamic data movement
* Lookup and ForEach activities
* Configuration-driven processing

The ADF resources are available in the repository under:

```text
dataset/
factory/
linkedService/
pipeline/
```

---

### 2. Bronze Layer — ADLS Gen2

The ingested source data is stored in the **Bronze layer** of ADLS Gen2.

The Bronze layer maintains the ingested data before transformation.

---

### 3. Silver Layer — Databricks & PySpark

Azure Databricks is used to process the Bronze data using **PySpark**.

The transformation process includes reading CSV data from the Bronze layer, applying data processing and cleaning operations, and writing the transformed data in **Parquet format** to the Silver layer.

Example workflow:

```text
Bronze CSV
    ↓
Databricks / PySpark
    ↓
Data Cleaning & Transformation
    ↓
Silver Parquet
```

The Databricks transformation notebook is available under:

```text
databricks/silver/
```

---

### 4. Gold Layer — Azure Synapse Analytics

The transformed Silver data is accessed through **Azure Synapse Analytics**.

Synapse SQL scripts are used to:

* Create database-scoped credentials using Managed Identity
* Create external data sources
* Define Parquet file formats
* Create external tables
* Create Gold views over Silver Parquet data

The Synapse SQL scripts are available under:

```text
synapse/sql/
```

---

### 5. Power BI Reporting

The processed data is connected to Power BI for reporting.

A KPI-based report was created to provide a simple analytical view of the processed data.

A screenshot of the report is available under:

```text
screenshots/
```

---

## 📂 Repository Structure

```text
azure-medallion-data-engineering/
│
├── dataset/
├── factory/
├── linkedService/
├── pipeline/
│
├── databricks/
│   └── silver/
│       └── silver_transformation.py
│
├── synapse/
│   └── sql/
│       ├── create_external_objects.sql
│       └── create_gold_views.sql
│
├── screenshots/
│   └── powerbi_report.png
│
├── publish_config.json
└── README.md
```

---

## 🎯 Key Skills Demonstrated

* Azure Data Engineering
* ETL / ELT workflows
* Azure Data Factory
* Data Lake Storage Gen2
* Medallion Architecture
* PySpark
* Apache Spark
* Data Cleaning & Transformation
* Parquet Data Processing
* Azure Synapse Analytics
* SQL
* External Tables and Views
* Power BI Reporting
* Configuration-driven data pipelines

---

## 📊 Project Outcome

The project demonstrates how raw source data can be moved through an Azure-based data engineering pipeline, transformed using PySpark, stored in optimized Parquet format, exposed through Synapse, and finally used for Power BI reporting.

---

## 👩‍💻 Author

**Muskan Parmar**

B.Tech Information Technology | 2022–2026

Interested in **Data Engineering, Data Analytics, Cloud Data Platforms, and Azure technologies**.
