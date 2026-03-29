# fabric-finance-lakehouse
Enterprise-grade Microsoft Fabric Lakehouse project implementing Medallion architecture (Bronze, Silver, Gold) for financial transactions
# Finance Data Engineering Pipeline — Microsoft Fabric

## 🚀 Project Overview

This project implements an end-to-end data engineering pipeline using Microsoft Fabric to process financial transaction data.

The pipeline follows a Medallion Architecture (Bronze, Silver, Gold) and supports both full and incremental data processing.

---

## 🏗️ Architecture

* Bronze Layer → Raw data ingestion
* Silver Layer → Data cleaning and transformation
* Gold Layer → Business model and aggregations
* Optimize Layer → Performance optimization
* Data Quality Layer → Validation and checks

Pipeline Flow:
DDL → Bronze → Silver → Gold → Optimize → Data Quality

---

## ⚙️ Key Features

* Parameter-driven execution (FULL / INCR)
* Incremental processing using watermark logic
* Centralized failure handling
* Activity-level audit logging
* Data Quality validation layer
* Scheduled pipeline execution
* CI/CD-ready design (DEV / UAT / PROD)

---

## 🔁 Processing Modes

### FULL Load

* Loads complete dataset
* Used for initial load or recovery

### INCR Load

* Loads only new/updated data
* Improves performance and efficiency

---

## 🧠 Pipeline Design

### FULL Pipeline

* pl_finance_e2e_batch
* Runs once daily

### INCR Pipeline

* pl_finance_e2e_batch_incr
* Runs hourly

---

## 📊 Data Model

* Fact Table: fact_transactions
* Dimensions:

  * dim_account (SCD Type 2)
  * dim_date
  * dim_transaction_type

Supports analytical queries and reporting.

---

## 🛠️ Technology Stack

* Microsoft Fabric (Lakehouse, Pipelines, Notebooks)
* PySpark
* Delta Lake
* SQL Endpoint
* Git Integration

---

## 🔄 CI/CD Strategy

* DEV / UAT / PROD workspace separation
* Git branch mapping:

  * dev → DEV
  * uat → UAT
  * main → PROD
* Promotion flow:

  * feature → dev → uat → main
* Deployment checklist for safe release

---

## 📈 Monitoring & Logging

* meta.pipeline_activity_audit
* meta.pipeline_run_audit

Tracks execution status, duration, and failures.

---

## 🧪 Testing Strategy

* FULL load validation
* INCR load validation
* Invalid parameter handling
* Failure simulation testing
* Data validation checks

---

## 🎯 Business Value

* Scalable financial data processing
* Supports fraud detection use cases
* Enables KPI reporting and analytics
* Production-ready pipeline design

---

## 📂 Project Structure

```text
docs/
   01_Project_Overview.md
   02_Architecture.md
   03_Pipeline_Design.md
   04_Notebook_Design.md
   05_Data_Model.md
   06_CICD_Strategy.md
   07_Testing_Strategy.md
```

---

## 📌 Author

Finance Data Engineering Project using Microsoft Fabric
Designed with enterprise-grade architecture and best practices.
