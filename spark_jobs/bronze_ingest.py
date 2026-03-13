from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Bronze Ingest").getOrCreate()

jdbc_url = "jdbc:postgresql://ecommerce_postgres:5432/ecommerce_dw"

db_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

raw_df = spark.read.csv(
    "/app/data/raw_customers.csv",
    header=True,
    inferSchema=True
)

raw_df.write.jdbc(
    url=jdbc_url,
    table="bronze_customer_events",
    mode="append",
    properties=db_properties
)

spark.stop()