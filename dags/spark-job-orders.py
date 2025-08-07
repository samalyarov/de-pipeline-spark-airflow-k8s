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
ORDERS_PATH = "s3a://de-raw/orders"
CUSTOMER_PATH = "s3a://de-raw/customer"
NATION_PATH = "s3a://de-raw/nation"

TARGET_PATH = f"s3a://de-project/orders_report"

# Session
def _spark_session():
    return (SparkSession.builder
            .appName(f"spark-job-s-malyarov-orders-{uuid.uuid4().hex}")
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
    df_orders = spark.read.parquet(ORDERS_PATH).select("O_CUSTKEY", "O_ORDERKEY", "O_ORDERDATE", "O_ORDERPRIORITY", "O_TOTALPRICE", "O_ORDERSTATUS")
    df_customer = spark.read.parquet(CUSTOMER_PATH).select("C_CUSTKEY", "C_NATIONKEY")
    df_nation = spark.read.parquet(NATION_PATH).select("N_NATIONKEY", "N_NAME")

    # Joining the dataframes to get a single one to operate on
    result_df = df_orders.join(df_customer, df_orders.O_CUSTKEY == df_customer.C_CUSTKEY, "left")\
                         .join(df_nation, df_customer.C_NATIONKEY == df_nation.N_NATIONKEY, "left")
    
    # Performing the transformation
    result_df = (result_df.select("O_ORDERDATE", "N_NAME", "O_ORDERPRIORITY", "O_ORDERKEY", "O_TOTALPRICE", "O_ORDERSTATUS")
                          .withColumn("O_MONTH", F.substring(F.col("O_ORDERDATE"), 0, 7))
                          .groupBy(["O_MONTH", "N_NAME", "O_ORDERPRIORITY"])
                          .agg(
                               F.count("O_ORDERKEY").alias("orders_count"),
                               F.avg("O_TOTALPRICE").alias("avg_order_price"),
                               F.sum("O_TOTALPRICE").alias("sum_order_price"),
                               F.min("O_TOTALPRICE").alias("min_order_price"),
                               F.max("O_TOTALPRICE").alias("max_order_price"),
                               F.sum(F.when(F.col("O_ORDERSTATUS") == "F", 1).otherwise(0)).alias("f_order_status"),
                               F.sum(F.when(F.col("O_ORDERSTATUS") == "O", 1).otherwise(0)).alias("o_order_status"),
                               F.sum(F.when(F.col("O_ORDERSTATUS") == "P", 1).otherwise(0)).alias("p_order_status")
                               )
                          .orderBy(["N_NAME", "O_ORDERPRIORITY"])
                          )
    
    # Saving the results
    result_df.write.mode("overwrite").parquet(TARGET_PATH)
    spark.stop()


if __name__ == "__main__":
    main()