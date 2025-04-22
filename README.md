# STEDI Human Balance Analysis – Project README

## 📌 Project Purpose
This project aims to build a cloud-based data pipeline to analyze human balance using accelerometer and step trainer data. The objective is to ingest raw data, clean and transform it, and produce curated datasets ready for machine learning model training and advanced analytics. This end-to-end pipeline helps monitor and improve human balance, contributing toward potential healthcare or fitness applications.

---

## ⚙️ Pipeline Overview
This project implements a complete ETL (Extract, Transform, Load) pipeline using AWS services. Here's a detailed step-by-step process of what was done:

### 1. **IAM Role Setup**
- An IAM role was created with necessary permissions to access Amazon S3 and AWS Glue.
- This role was used by AWS Glue jobs and crawlers to read from and write to S3 securely.

### 2. **Data Ingestion**
- JSON files containing sample data for customers, accelerometers, and step trainers were uploaded into S3 buckets.
- The bucket was organized with separate folders for each data source under `landing/`, `trusted/`, and `curated/` layers.

### 3. **Data Cataloging with AWS Glue Crawlers**
- AWS Glue Crawlers were used to scan the S3 folders and create metadata tables in the AWS Glue Data Catalog.
- These tables allowed SQL queries on the data using Amazon Athena.

### 4. **ETL Jobs for Data Cleaning and Transformation**
- Multiple AWS Glue Jobs (PySpark scripts) were created to perform data cleaning and transformation tasks:
  - Remove nulls and duplicates.
  - Join tables to enrich customer data with accelerometer readings.
  - Extract relevant features for analysis.
- Each job writes output to the `trusted/` or `curated/` S3 paths.

### 5. **Curated Data for Analysis**
- Final curated tables were created with selected features necessary for machine learning analysis (such as `x`, `y`, `z`, `timestamp`).
- Example: `customer_curated` includes only `user`, `customerName`, `x`, `y`, `z` for efficiency and relevance.
- These curated tables are lightweight and optimized for downstream analytics.

> 📝 The schema of curated tables can be extended in the future based on analysis requirements.

### 6. **Pipeline Summary**
- The complete process forms a data pipeline from ingestion to curation.
- Data flows from raw JSON files (landing) to cleaned and validated datasets (trusted), and finally to simplified, analytics-ready datasets (curated).
- The pipeline was built entirely on AWS using S3, Glue, and Athena.

---

## 🧰 Tools and Technologies Used
- **Amazon S3** – Data storage for all layers: landing, trusted, and curated.
- **AWS Glue** – Managed ETL service used for crawlers and transformation jobs.
- **Amazon Athena** – SQL querying service used to validate table outputs and perform exploratory analysis.
- **AWS IAM** – For secure access control and permissions.

---

## ✅ Outcome
- A fully functional data pipeline that transforms raw JSON sensor data into structured and curated tables.
- The curated datasets are ready for downstream applications such as machine learning model development or reporting.


