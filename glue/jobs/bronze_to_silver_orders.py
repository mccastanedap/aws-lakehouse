import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType
)


# 1) Job arguments & logging
#    - Make paths and catalog objects configurable

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BRONZE_DB",
        "BRONZE_TABLE",
        "SILVER_PATH",
        "PARTITION_COLUMN"  # e.g. "order_date" or "country"
    ]
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)
logger.info("Starting bronze_to_silver job")


# 2) Glue / Spark bootstrap

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BRONZE_DB = args["BRONZE_DB"]
BRONZE_TABLE = args["BRONZE_TABLE"]
SILVER_PATH = args["SILVER_PATH"]
PARTITION_COLUMN = args["PARTITION_COLUMN"]


# 3) Explicit source schema (resilient to drift)
#     Read as strings first, then cast deterministically

source_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),  
    StructField("customer_id", StringType(), True),
    StructField("country", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", StringType(), True),    
    StructField("unit_price", StringType(), True),  
])

# Read via Glue Catalog for AWS-native integration
bronze_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=BRONZE_DB,
    table_name=BRONZE_TABLE
)

bronze_df = bronze_dyf.toDF()

# Align incoming DF to the expected schema columns (missing columns become null)
def align_to_schema(df: DataFrame, schema: StructType) -> DataFrame:
    for field in schema:
        if field.name not in df.columns:
            df = df.withColumn(field.name, col(field.name))  # creates null column
    # Select in the schema-defined order (extra columns are kept if needed)
    return df.select(*[col(f.name) for f in schema])

bronze_df = align_to_schema(bronze_df, source_schema)

raw_count = bronze_df.count()
logger.info(f"Bronze rows (raw): {raw_count}")


# 4) Type casting, derived columns, and basic DQ

clean_df = (
    bronze_df
    # Casts
    .withColumn("order_date", to_date(col("order_date")))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("double"))
    # Derived metrics
    .withColumn("total_amount", col("quantity") * col("unit_price"))
)

# Basic data quality rules
# - Drop rows missing critical fields
# - Filter negative/zero quantities or prices
# - Deduplicate on order_id
clean_df = (
    clean_df
    .dropna(subset=["order_id", "order_date", "customer_id"])
    .filter(col("quantity") > 0)
    .filter(col("unit_price") > 0.0)
    .dropDuplicates(["order_id"])
)

clean_count = clean_df.count()
logger.info(f"Cleaned rows (after DQ): {clean_count}")
logger.info(f"Rows removed by DQ: {raw_count - clean_count}")

# Optional: guardrail if everything was dropped (helps detect upstream issues)
if clean_count == 0:
    logger.warning("No rows remaining after cleaning. Skipping write to Silver.")
    job.commit()
    sys.exit(0)


# 5) Write to Silver: Parquet + partitioning + compression

# If partition column doesn’t exist or is null-heavy, fall back to non-partitioned write
partition_cols = []
if PARTITION_COLUMN and PARTITION_COLUMN in clean_df.columns:
    partition_cols = [PARTITION_COLUMN]
else:
    logger.warning(
        f"Partition column '{PARTITION_COLUMN}' not found. Writing without partitioning."
    )

if partition_cols:
    (
        clean_df
        .write
        .mode("overwrite")
        .partitionBy(*partition_cols)
        .parquet(SILVER_PATH)
    )
else:
    (
        clean_df
        .write
        .mode("overwrite")
        .parquet(SILVER_PATH)
    )

logger.info(f"Silver Parquet written to: {SILVER_PATH}")
job.commit()
logger.info("bronze_to_silver job completed successfully")