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
SUPPLIER_PATH = "s3a://de-raw/supplier"
NATION_PATH = "s3a://de-raw/nation"
REGION_PATH = "s3a://de-raw/region"

TARGET_PATH = f"s3a://de-project/suppliers_report"

# Session
def _spark_session():
    return (SparkSession.builder
            .appName(f"spark-job-s-malyarov-suppliers-{uuid.uuid4().hex}")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")
            .config("spark.hadoop.fs.s3a.endpoint", S3END_POINT)
            .config("spark.hadoop.fs.s3a.region", S3REGION)
            .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
            .getOrCreate())

def main():
    # Initializing the Spark session
    spark = _spark_session()

    # Reading the source dataframes
    df_supplier = spark.read.parquet(SUPPLIER_PATH)
    df_nation = spark.read.parquet(NATION_PATH)
    df_region = spark.read.parquet(REGION_PATH)

    # Joining the dataframes to get a single one to operate one
    result_df = df_supplier.join(df_nation, df_supplier.S_NATIONKEY == df_nation.N_NATIONKEY, how="left")\
                           .join(df_region, df_nation.N_REGIONKEY == df_region.R_REGIONKEY, how="left")
    
    # Performing the transformation
    result_df = (result_df.select("R_NAME", "N_NAME", "S_SUPPKEY", "S_ADDRESS", "S_ACCTBAL")
                          .groupBy(["R_NAME", "N_NAME"])
                          .agg(
                               F.countDistinct("S_SUPPKEY").alias("unique_supplers_count"),
                               F.avg("S_ACCTBAL").alias("avg_acctbal"),
                               F.mean("S_ACCTBAL").alias("mean_acctbal"),
                               F.min("S_ACCTBAL").alias("min_acctbal"),
                               F.max("S_ACCTBAL").alias("max_acctbal")
                          ).orderBy(["R_NAME", "N_NAME"])
                 )

    # Saving the results
    result_df.write.mode("overwrite").parquet(TARGET_PATH)
    spark.stop()

if __name__ == "__main__":
    main()

