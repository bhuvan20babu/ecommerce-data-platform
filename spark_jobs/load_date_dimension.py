from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, quarter
from pyspark.sql.types import DateType

spark = SparkSession.builder \
    .appName("LoadDateDimension") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://ecommerce_postgres:5432/ecommerce_dw"

connection_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

orders_df = spark.read.csv(
    "/app/data/raw_orders.csv",
    header=True,
    inferSchema=True
)

dates_df = orders_df \
    .select(col("order_date").cast(DateType()).alias("full_date")) \
    .distinct()

dim_date_df = dates_df \
    .withColumn("year", year(col("full_date"))) \
    .withColumn("quarter", quarter(col("full_date"))) \
    .withColumn("month", month(col("full_date"))) \
    .withColumn("day", dayofmonth(col("full_date")))

dim_date_df.write.jdbc(
    url=jdbc_url,
    table="dim_date",
    mode="append",
    properties=connection_properties
)

spark.stop()