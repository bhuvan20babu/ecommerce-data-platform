from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

spark = SparkSession.builder.appName("Silver Dedup").getOrCreate()

jdbc_url = "jdbc:postgresql://ecommerce_postgres:5432/ecommerce_dw"

db_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

bronze_df = spark.read.jdbc(
    url=jdbc_url,
    table="bronze_customer_events",
    properties=db_properties
)

window = Window.partitionBy("event_id").orderBy(col("event_time").desc())

silver_df = bronze_df.withColumn(
    "rn",
    row_number().over(window)
).filter(col("rn") == 1).drop("rn")

silver_df.write.jdbc(
    url=jdbc_url,
    table="silver_customer_events",
    mode="overwrite",
    properties=db_properties
)

spark.stop()