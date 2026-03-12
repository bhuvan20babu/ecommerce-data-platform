from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import TimestampType
import psycopg2

# -------------------------------
# 1. Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("FactSalesRebuild_Idempotent") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://ecommerce_postgres:5432/ecommerce_dw"

connection_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# -------------------------------
# 2. Truncate Fact Table
# -------------------------------
def truncate_fact():
    conn = psycopg2.connect(
        dbname="ecommerce_dw",
        user="admin",
        password="admin",
        host="ecommerce_postgres",
        port="5432"
    )
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE fact_sales RESTART IDENTITY CASCADE;")
    conn.commit()
    cur.close()
    conn.close()

truncate_fact()

print("=== fact_sales truncated ===")

# -------------------------------
# 3. Read Orders
# -------------------------------
orders_df = spark.read.csv(
    "/app/data/raw_orders.csv",
    header=True,
    inferSchema=True
).withColumn("order_date", col("order_date").cast(TimestampType()))

# -------------------------------
# 4. Read Dimensions
# -------------------------------
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

# -------------------------------
# 5. Temporal Customer Join
# -------------------------------
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

# -------------------------------
# 6. Product Join
# -------------------------------
product_join = customer_join.join(
    dim_product.alias("p"),
    col("o.product_id") == col("p.product_id"),
    "left"
)

# -------------------------------
# 7. Date Join
# -------------------------------
product_join = product_join.withColumn(
    "order_date_only",
    to_date(col("o.order_date"))
)

date_join = product_join.join(
    dim_date.alias("d"),
    col("order_date_only") == col("d.full_date"),
    "left"
)

# -------------------------------
# 8. Final Fact Selection
# -------------------------------
fact_df = date_join.select(
    col("o.order_id"),
    col("c.customer_sk"),
    col("p.product_sk"),
    col("d.date_sk"),
    col("o.quantity"),
    col("o.total_amount")
)

print("=== Final Fact Output ===")
fact_df.show()

# -------------------------------
# 9. Write Facts
# -------------------------------
fact_df.write.jdbc(
    url=jdbc_url,
    table="fact_sales",
    mode="append",
    properties=connection_properties
)

print("=== Fact Load Complete ===")

spark.stop()