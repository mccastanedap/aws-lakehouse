import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date

# -------------------------------------------------------------------
# 1. Glue job bootstrap
# -------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------------------------
# 2. Config – UPDATE these to match your environment
# -------------------------------------------------------------------
BRONZE_DB = "lakehouse_demo"          # <-- your Glue database name
BRONZE_TABLE = "orders_bronze"        # <-- your bronze table name
SILVER_PATH = (
    "s3://melissa-lakehouse-demo/"
    "aws-lakehouse/silver/orders/"
)                                       # <-- silver S3 folder

# -------------------------------------------------------------------
# 3. Read from Bronze (Glue Catalog table)
# -------------------------------------------------------------------
bronze_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=BRONZE_DB,
    table_name=BRONZE_TABLE
)

bronze_df = bronze_dyf.toDF()

# -------------------------------------------------------------------
# 4. Simple “bronze → silver” transforms
#    - clean types
#    - add total_amount
#    - drop obviously bad rows
# -------------------------------------------------------------------
clean_df = (
    bronze_df
    # cast types
    .withColumn("order_date", to_date(col("order_date")))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("double"))
    # add a new measure
    .withColumn("total_amount", col("quantity") * col("unit_price"))
    # basic data quality: remove rows missing critical fields
    .dropna(subset=["order_id", "order_date", "customer_id"])
)

silver_dyf = glueContext.create_dynamic_frame.from_df(
    clean_df, glueContext, "silver_orders"
)

# -------------------------------------------------------------------
# 5. Write to Silver in Parquet
# -------------------------------------------------------------------
glueContext.write_dynamic_frame.from_options(
    frame=silver_dyf,
    connection_type="s3",
    connection_options={"path": SILVER_PATH},
    format="parquet",
    format_options={"compression": "snappy"},
)

job.commit()
