# AWS Lakehouse – Demo Project

This repository contains a small end-to-end **lakehouse-style** data pipeline on AWS.

The goal is to practice **data engineering & architecture skills**:

- Raw data landing in S3 (**bronze**).
- Cleaned & modeled data written back to S3 (**silver**) and aggregated (**gold**).
- Reusable configuration and clear project structure.
- Infrastructure and code that follow good practices (Lambda + Glue + Athena).

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
-- Inspect cleaned orders
SELECT *
FROM aws_lakehouse_db.silver_orders
LIMIT 10;

-- Revenue by country and product from the gold layer
SELECT
  order_date,
  country,
  product,
  total_quantity,
  total_revenue
FROM aws_lakehouse_db.gold_orders
ORDER BY order_date, country, product;

---

## Project Structure

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
