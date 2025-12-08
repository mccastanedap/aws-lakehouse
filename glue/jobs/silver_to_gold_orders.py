import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, sum as spark_sum

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

silver_df = spark.read.parquet(
    "s3://melissa-lakehouse-demo/aws-lakehouse/silver/silver_orders/"
)

silver_df = silver_df.withColumn("revenue", col("quantity") * col("unit_price"))

gold_df = (
    silver_df.groupBy("order_date", "country", "product")
    .agg(
        spark_sum("quantity").alias("total_quantity"),
        spark_sum("revenue").alias("total_revenue"),
    )
)

gold_df.write.mode("overwrite").parquet(
    "s3://melissa-lakehouse-demo/aws-lakehouse/gold/gold_orders/"
)

job.commit()
