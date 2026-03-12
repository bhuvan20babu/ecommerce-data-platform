from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LoadProducts") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://ecommerce_postgres:5432/ecommerce_dw"

connection_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

print("=== Reading Products CSV ===")

products_df = spark.read.csv(
    "/app/data/raw_products.csv",
    header=True,
    inferSchema=True
)

products_df.show()

print("=== Writing to dim_product ===")

products_df.write.jdbc(
    url=jdbc_url,
    table="dim_product",
    mode="append",
    properties=connection_properties
)

print("=== Product Load Complete ===")

spark.stop()