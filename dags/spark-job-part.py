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
PART_PATH = "s3a://de-raw/part"
PARTSUPP_PATH = "s3a://de-raw/partsupp"
SUPPLIER_PATH = "s3a://de-raw/supplier"
NATION_PATH = "s3a://de-raw/nation"

TARGET_PATH = f"s3a://de-project/parts_report"

# Session
def _spark_session():
    return (SparkSession.builder
            .appName(f"spark-job-s-malyarov-parts-{uuid.uuid4().hex}")
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
    df_part = spark.read.parquet(PART_PATH)
    df_partsupp = spark.read.parquet(PARTSUPP_PATH)
    df_supplier = spark.read.parquet(SUPPLIER_PATH)
    df_nation = spark.read.parquet(NATION_PATH)

    # Joining the dataframes to get a single one to operate on
    result_df = df_part.join(df_partsupp, df_part.P_PARTKEY == df_partsupp.PS_PARTKEY)\
                       .join(df_supplier, df_partsupp.PS_SUPPKEY == df_supplier.S_SUPPKEY)\
                       .join(df_nation, df_supplier.S_NATIONKEY == df_nation.N_NATIONKEY)

    # Performing the transformation
    result_df   = (result_df.select("N_NAME", "P_TYPE", "P_CONTAINER", "P_PARTKEY", "P_RETAILPRICE", "P_SIZE", "PS_SUPPLYCOST")
                            .groupBy("N_NAME", "P_TYPE", "P_CONTAINER")
                            .agg(
                                F.count("P_PARTKEY").alias("parts_count"),
                                F.avg("P_RETAILPRICE").alias("avg_retailprice"),
                                F.sum("P_SIZE").alias("size"),
                                F.percentile_approx("P_RETAILPRICE", 0.5).alias("mean_retailprice"),
                                F.min("P_RETAILPRICE").alias("min_retailprice"),
                                F.max("P_RETAILPRICE").alias("max_retailprice"),
                                F.avg("PS_SUPPLYCOST").alias("avg_supplycost"),
                                F.percentile_approx("PS_SUPPLYCOST", 0.5).alias("mean_supplycost"),
                                F.min("PS_SUPPLYCOST").alias("min_supplycost"),
                                F.max("PS_SUPPLYCOST").alias("max_supplycost")
                             )
                             )

    # Saving the results
    result_df.write.mode("overwrite").parquet(TARGET_PATH)
    spark.stop()

if __name__ == "__main__":
    main()

