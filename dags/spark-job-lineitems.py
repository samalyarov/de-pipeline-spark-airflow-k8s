from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import uuid
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Config - loaded from environment variables
S3END_POINT = os.getenv("S3END_POINT")
S3REGION = os.getenv("S3REGION")
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

# Paths
LINEITEM_PATH = "s3a://de-raw/lineitem"
TARGET_PATH = f"s3a://de-project/lineitems_report"

def _spark_session():
    return (SparkSession.builder
            .appName(f"spark-job-s-malyarov-lineitems-fix-{uuid.uuid4().hex}")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")
            .config("spark.hadoop.fs.s3a.endpoint", S3END_POINT)
            .config("spark.hadoop.fs.s3a.region", S3REGION)
            .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
            .getOrCreate())

def main():
    spark = _spark_session()

    df = spark.read.parquet(LINEITEM_PATH).select(
        "L_ORDERKEY", 
        "L_EXTENDEDPRICE", 
        "L_DISCOUNT",
        "L_TAX",
        "L_RECEIPTDATE",
        "L_SHIPDATE",
        "L_RETURNFLAG"
    )

    result_df = df.groupBy("L_ORDERKEY").agg(
        F.count("*").alias("count"),
        F.sum("L_EXTENDEDPRICE").alias("sum_extendprice"),
        F.avg("L_DISCOUNT").alias("mean_discount"),
        F.avg("L_TAX").alias("mean_tax"),
        F.avg(F.datediff("L_RECEIPTDATE", "L_SHIPDATE")).alias("delivery_days"),
        F.count(F.when(F.col("L_RETURNFLAG") == "A", True)).alias("A_return_flags"),
        F.count(F.when(F.col("L_RETURNFLAG") == "R", True)).alias("R_return_flags"),
        F.count(F.when(F.col("L_RETURNFLAG") == "N", True)).alias("N_return_flags"),
    ).orderBy("L_ORDERKEY")

    result_df.show()
    result_df.write.mode("overwrite").parquet(TARGET_PATH)
    spark.stop()

if __name__ == "__main__":
    main()
