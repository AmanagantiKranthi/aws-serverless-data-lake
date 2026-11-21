"""
AWS Glue PySpark job: validate, deduplicate and curate order CSV files.

Required job arguments:
  --JOB_NAME
  --S3_INPUT_PATH
  --S3_OUTPUT_PATH
  --S3_REJECT_PATH

This script is intended for an AWS Glue Spark job runtime.
"""
import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "S3_INPUT_PATH", "S3_OUTPUT_PATH", "S3_REJECT_PATH"],
)

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

required_columns = [
    "order_id", "order_ts", "customer_id", "product",
    "quantity", "unit_price", "country",
]

raw = (
    spark.read.option("header", True)
    .option("mode", "PERMISSIVE")
    .csv(args["S3_INPUT_PATH"])
)

missing = [column for column in required_columns if column not in raw.columns]
if missing:
    raise ValueError(f"Input is missing required columns: {missing}")

typed = (
    raw.select(*required_columns)
    .withColumn("order_id", F.trim(F.col("order_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("product", F.trim(F.col("product")))
    .withColumn("country", F.trim(F.col("country")))
    .withColumn("order_ts", F.to_timestamp("order_ts"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
    .withColumn("order_date", F.to_date("order_ts"))
)

invalid_condition = (
    F.col("order_id").isNull() | (F.col("order_id") == "") |
    F.col("customer_id").isNull() | (F.col("customer_id") == "") |
    F.col("product").isNull() | (F.col("product") == "") |
    F.col("country").isNull() | (F.col("country") == "") |
    F.col("order_ts").isNull() |
    F.col("quantity").isNull() | (F.col("quantity") <= 0) |
    F.col("unit_price").isNull() | (F.col("unit_price") < 0) |
    F.col("order_date").isNull()
)

valid = typed.filter(~invalid_condition)
rejected = typed.filter(invalid_condition).withColumn(
    "rejection_reason",
    F.concat_ws(
        ";",
        F.when(F.col("order_id").isNull() | (F.col("order_id") == ""), "missing_order_id"),
        F.when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), "missing_customer_id"),
        F.when(F.col("product").isNull() | (F.col("product") == ""), "missing_product"),
        F.when(F.col("country").isNull() | (F.col("country") == ""), "missing_country"),
        F.when(F.col("order_ts").isNull(), "invalid_order_ts"),
        F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity"),
        F.when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), "invalid_unit_price"),
    ),
)

# For production data, add a stable tie-breaker such as ingestion timestamp.
window = Window.partitionBy("order_id").orderBy(F.col("order_ts").desc())
deduplicated = (
    valid.withColumn("row_number", F.row_number().over(window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
    .withColumn("gross_amount", F.col("quantity") * F.col("unit_price"))
)

(
    deduplicated.write.mode("overwrite")
    .partitionBy("order_date")
    .parquet(args["S3_OUTPUT_PATH"])
)

if rejected.limit(1).count() > 0:
    rejected.write.mode("append").parquet(args["S3_REJECT_PATH"])

print(
    "ETL summary | "
    f"input={raw.count()} valid={valid.count()} "
    f"curated={deduplicated.count()} rejected={rejected.count()}"
)

job.commit()
