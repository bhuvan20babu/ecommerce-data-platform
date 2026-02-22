from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lead
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
import psycopg2

# -------------------------------
# 1. Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("SCD2CustomerRebuild_Idempotent") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://ecommerce_postgres:5432/ecommerce_dw"

connection_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# -------------------------------
# 2. Truncate Dimension (Idempotency)
# -------------------------------
def truncate_dimension():
    conn = psycopg2.connect(
        dbname="ecommerce_dw",
        user="admin",
        password="admin",
        host="ecommerce_postgres",
        port="5432"
    )
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE dim_customer RESTART IDENTITY CASCADE;")
    conn.commit()
    cur.close()
    conn.close()

truncate_dimension()

print("=== dim_customer truncated ===")

# -------------------------------
# 3. Read Historical Input
# -------------------------------
raw_df = spark.read.csv(
    "/app/data/raw_customers.csv",
    header=True,
    inferSchema=True
).withColumn("update_ts", col("update_ts").cast(TimestampType()))

# -------------------------------
# 4. SCD2 Using Window + lead()
# -------------------------------
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

print("=== Final SCD2 Output ===")
final_df.show()

# -------------------------------
# 5. Write to Postgres
# -------------------------------
final_df.write.jdbc(
    url=jdbc_url,
    table="dim_customer",
    mode="append",
    properties=connection_properties
)

print("=== Dimension Load Complete ===")

spark.stop()