from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import TimestampType

spark = SparkSession.builder \
    .appName("LoadFactSalesProper") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://ecommerce_postgres:5432/ecommerce_dw"

connection_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

print("=== Reading Orders ===")

orders_df = spark.read.csv(
    "/app/data/raw_orders.csv",
    header=True,
    inferSchema=True
).withColumn("order_date", col("order_date").cast(TimestampType()))

orders_df.show()

print("=== Reading Dimensions ===")

dim_customer = spark.read.jdbc(
    url=jdbc_url,
    table="dim_customer",
    properties=connection_properties
)

dim_product = spark.read.jdbc(
    url=jdbc_url,
    table="dim_product",
    properties=connection_properties
)

dim_date = spark.read.jdbc(
    url=jdbc_url,
    table="dim_date",
    properties=connection_properties
)

# ---------------------------
# 1️⃣ Temporal Customer Join
# ---------------------------

customer_join = orders_df.alias("o").join(
    dim_customer.alias("c"),
    (
        (col("o.customer_id") == col("c.customer_id")) &
        (col("o.order_date") >= col("c.start_date")) &
        (
            col("c.end_date").isNull() |
            (col("o.order_date") < col("c.end_date"))
        )
    ),
    "left"
)

# ---------------------------
# 2️⃣ Product Join
# ---------------------------

product_join = customer_join.join(
    dim_product.alias("p"),
    col("o.product_id") == col("p.product_id"),
    "left"
)

# ---------------------------
# 3️⃣ Date Join (Using full_date)
# ---------------------------

product_join = product_join.withColumn(
    "order_date_only",
    to_date(col("o.order_date"))
)

date_join = product_join.join(
    dim_date.alias("d"),
    col("order_date_only") == col("d.full_date"),
    "left"
)

# ---------------------------
# 4️⃣ Final Fact Selection
# ---------------------------

fact_df = date_join.select(
    col("o.order_id"),
    col("c.customer_sk"),
    col("p.product_sk"),
    col("d.date_sk"),
    col("o.quantity"),
    col("o.total_amount")
)

print("=== Fact Preview ===")
fact_df.show()

fact_df.write.jdbc(
    url=jdbc_url,
    table="fact_sales",
    mode="append",
    properties=connection_properties
)

print("=== Fact Load Complete ===")

spark.stop()