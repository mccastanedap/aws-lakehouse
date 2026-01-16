# AWS Lakehouse – Demo Project

This repository contains a small end‑to‑end lakehouse‑style data pipeline on AWS.
The goal is to practice data engineering & architecture skills using AWS services while keeping costs minimal.

Note on Costs:
To avoid unnecessary AWS charges, Glue jobs and Lambda functions are not pre‑deployed.
You can recreate them easily by following the setup instructions below.

---
## Project Goals
- Ingest raw e‑commerce orders into an S3 Bronze layer.
- Transform and validate data into Silver and Gold layers using AWS Glue (PySpark).
- Query curated datasets with Amazon Athena for analytics and dashboards.
- Showcase modular ETL code, schema enforcement, data quality checks, and orchestration triggers.
---

## Architecture (High Level)

1. **Source data – synthetic e-commerce orders**
   - A Lambda function generates sample order data (`order_id`, `order_date`, `customer_id`, `country`, `product`, `quantity`, `unit_price`) and writes CSV files to S3.

2. **Bronze layer (raw) – S3**
   - Raw CSV files are stored under:  
     `s3://melissa-lakehouse-demo/aws-lakehouse/bronze/`
   - Example: `orders_lambda_YYYYMMDD_HHMMSS.csv`.

3. **Silver layer (cleaned) – S3 + AWS Glue**
   - An AWS Glue job (`bronze_to_silver_orders`) reads the bronze CSVs,
     casts types, and writes cleaned **Parquet** files to:  
     `s3://melissa-lakehouse-demo/aws-lakehouse/silver/silver_orders/`
   - Data is registered in the Glue Data Catalog as the `silver_orders` table
     and queried with Amazon Athena.

4. **Gold layer (analytics) – S3 + AWS Glue**
   - A second Glue job (`silver_to_gold_orders`) aggregates silver data
     (e.g. total quantity & revenue per `order_date`, `country`, `product`)
     and writes Parquet files to:  
     `s3://melissa-lakehouse-demo/aws-lakehouse/gold/gold_orders/`
   - Exposed in Athena as the `gold_orders` table for analytics / dashboards.

> All lakehouse data lives in the S3 bucket  
> `s3://melissa-lakehouse-demo` under the prefix `aws-lakehouse/`.
---

## Query Layer – Amazon Athena

Cleaned (silver) and aggregated (gold) data are exposed through Amazon Athena using the AWS Glue Data Catalog:

- `aws_lakehouse_db.silver_orders`
- `aws_lakehouse_db.gold_orders`

Example queries:

```sql
- Inspect cleaned orders
SELECT *
FROM aws_lakehouse_db.silver_orders
LIMIT 10;

- Revenue by country and product from the gold layer
SELECT
  order_date,
  country,
  product,
  total_quantity,
  total_revenue
FROM aws_lakehouse_db.gold_orders
ORDER BY order_date, country, product;
```

---
## Setup Instructions
1. S3 Bucket
   - Create an S3 bucket (e.g. my-lakehouse-demo).
   - Add folders: bronze/, silver/, gold/.
2. Lambda Function
   - Create a Lambda function in AWS Console.
   - Upload src/ingestion/lambda_ingest_orders.py.
   - Configure trigger (manual or scheduled) to write CSVs into the Bronze path.
3. Glue Jobs
   - Go to AWS Glue Console → Jobs → Create Job.
   - Upload scripts from glue/jobs/:
   - bronze_to_silver_orders.py
   - silver_to_gold_orders.py
   - Configure IAM role with S3 + Glue + Athena permissions.
   - Register outputs in Glue Data Catalog:
   - silver_orders
   - gold_orders
4. Athena Queries
   - Ensure Glue Data Catalog tables exist.
   - Run queries from docs/queries.sql or README examples.

---
---

## Project Structure

```text
src/
  ingestion/
    lambda_ingest_orders.py
  transformation/
    bronze_to_silver_orders.py
  analytics/
    silver_to_gold_orders.py
  utils/

glue/
  jobs/
    bronze_to_silver_orders.py
    silver_to_gold_orders.py

configs/
  dev.yaml

notebooks/
  exploration.ipynb

docs/
  architecture.png
  architecture.md
