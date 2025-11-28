# AWS Lakehouse – Demo Project

This repository contains a small end-to-end **lakehouse-style** data pipeline on AWS.

The goal is to practice **data engineering & architecture skills**:

- Raw data landing in S3 (**bronze**).
- Cleaned & modeled data written back to S3 (**silver / gold**).
- Reusable configuration and clear project structure.
- Infrastructure and code that follow good practices.

---

## Architecture (High Level)

1. **Source data** – public dataset (e.g., NYC taxi trips / e-commerce orders).
2. **Bronze layer (raw)** – files stored in S3 under `bronze/`.
3. **Silver layer (cleaned)** – transformed data stored in `silver/`.
4. **Gold layer (analytics)** – aggregated, query-ready data in `gold/`.

> All objects live in a demo S3 bucket (e.g. `your-demo-bucket/aws-lakehouse/`).

---

## Project Structure

```text
src/
  ingestion/         # Scripts to load raw data into the bronze layer
  transformation/    # Scripts to clean and standardize data (silver)
  analytics/         # Scripts to build aggregates / marts (gold)
  utils/             # Shared helpers (config loading, logging, etc.)
configs/             # Environment configs (dev / test)
notebooks/           # Optional exploration and prototyping
docs/                # Architecture notes, diagrams, etc.
