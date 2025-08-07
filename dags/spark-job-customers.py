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
REGION_PATH = "s3a://de-raw/region"
CUSTOMER_PATH = "s3a://de-raw/customer"
NATION_PATH = "s3a://de-raw/nation"

TARGET_PATH = f"s3a://de-project/customers_report"

# Session
def _spark_session():
    return (SparkSession.builder
            .appName(f"spark-job-s-malyarov-customers-{uuid.uuid4().hex}")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")
            .config("spark.hadoop.fs.s3a.endpoint", S3END_POINT)
            .config("spark.hadoop.fs.s3a.region", S3REGION)
            .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
            .getOrCreate())

def main():
     # Initializaing the Spark session
    spark = _spark_session()

    # Reading the source dataframes
    region_df = spark.read.parquet(REGION_PATH)
    customer_df = spark.read.parquet(CUSTOMER_PATH)
    nation_df = spark.read.parquet(NATION_PATH)

    # Joining the dataframes to get a single one to operate on
    result_df = customer_df.join(nation_df, customer_df.C_NATIONKEY == nation_df.N_NATIONKEY, how="left")\
                           .join(region_df, nation_df.N_REGIONKEY == region_df.R_REGIONKEY, how="left")
    
    # Performing the transformation
    result_df = (result_df.select('R_NAME', 'N_NAME', 'C_MKTSEGMENT', 'C_CUSTKEY', 'C_ADDRESS', 'C_ACCTBAL')
                          .groupBy(["R_NAME", "N_NAME", "C_MKTSEGMENT"])
                          .agg(
                               F.countDistinct('C_CUSTKEY').alias("unique_customers_count"),
                               F.avg('C_ACCTBAL').alias("avg_acctbal"),
                               F.mean('C_ACCTBAL').alias("mean_acctbal"),
                               F.min('C_ACCTBAL').alias("min_acctbal"),
                               F.max('C_ACCTBAL').alias("max_acctbal")
                               )
                           .orderBy(["R_NAME", "N_NAME"])
                          )
    
    # Saving the results
    result_df.write.mode("overwrite").parquet(TARGET_PATH)
    spark.stop()

if __name__ == "__main__":
    main()
