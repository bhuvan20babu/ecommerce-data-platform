from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lead
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType

spark = SparkSession.builder \
    .appName("SimpleSCD2Rebuild") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://ecommerce_postgres:5432/ecommerce_dw"

connection_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# Read full historical input
raw_df = spark.read.csv(
    "/app/data/raw_customers.csv",
    header=True,
    inferSchema=True
).withColumn("update_ts", col("update_ts").cast(TimestampType()))

# Order history correctly
window = Window.partitionBy("customer_id").orderBy("update_ts")

scd_df = raw_df \
    .withColumn("start_date", col("update_ts")) \
    .withColumn("end_date", lead("update_ts").over(window)) \
    .withColumn("is_current", col("end_date").isNull())

final_df = scd_df.select(
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "city",
    "country",
    "update_ts",
    "start_date",
    "end_date",
    "is_current"
)

final_df.show()

# Append to empty table
final_df.write.jdbc(
    url=jdbc_url,
    table="dim_customer",
    mode="append",
    properties=connection_properties
)

spark.stop()