import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, sum as _sum, avg as _avg


# 1) Job arguments & logging

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SILVER_DB",
        "SILVER_TABLE",
        "GOLD_PATH",
        "PARTITION_COLUMN"  # e.g. "country" or "order_date"
    ]
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
logger.info("Starting silver_to_gold job")


# 2) Glue / Spark bootstrap

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_DB = args["SILVER_DB"]
SILVER_TABLE = args["SILVER_TABLE"]
GOLD_PATH = args["GOLD_PATH"]
PARTITION_COLUMN = args["PARTITION_COLUMN"]


# 3) Read Silver data

silver_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SILVER_DB,
    table_name=SILVER_TABLE
)
silver_df = silver_dyf.toDF()

raw_count = silver_df.count()
logger.info(f"Silver rows (raw): {raw_count}")


# 4) Aggregations for Gold layer
#    - total quantity & revenue per country/product/date
#    - average unit price for sanity checks

gold_df = (
    silver_df
    .groupBy("order_date", "country", "product")
    .agg(
        _sum("quantity").alias("total_quantity"),
        _sum("total_amount").alias("total_revenue"),
        _avg("unit_price").alias("avg_unit_price")
    )
)

gold_count = gold_df.count()
logger.info(f"Gold rows (aggregated): {gold_count}")


# 5) Data quality guardrails
#    - Drop rows with nulls in grouping keys
#    - Ensure totals are non-negative

gold_df = (
    gold_df
    .dropna(subset=["order_date", "country", "product"])
    .filter(col("total_quantity") >= 0)
    .filter(col("total_revenue") >= 0.0)
)

clean_count = gold_df.count()
logger.info(f"Gold rows (after DQ): {clean_count}")

if clean_count == 0:
    logger.warning("No rows remaining after aggregation. Skipping write to Gold.")
    job.commit()
    sys.exit(0)


# 6) Write to Gold: Parquet + partitioning

partition_cols = []
if PARTITION_COLUMN and PARTITION_COLUMN in gold_df.columns:
    partition_cols = [PARTITION_COLUMN]
else:
    logger.warning(
        f"Partition column '{PARTITION_COLUMN}' not found. Writing without partitioning."
    )

if partition_cols:
    (
        gold_df
        .write
        .mode("overwrite")
        .partitionBy(*partition_cols)
        .parquet(GOLD_PATH)
    )
else:
    (
        gold_df
        .write
        .mode("overwrite")
        .parquet(GOLD_PATH)
    )

logger.info(f"Gold Parquet written to: {GOLD_PATH}")
job.commit()
logger.info("silver_to_gold job completed successfully")