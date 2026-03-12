from pyspark.sql.functions import md5, concat_ws
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max
import psycopg2

spark = SparkSession.builder \
    .appName("Incremental SCD2 Customer Load") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://ecommerce_postgres:5432/ecommerce_dw"

db_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# -------------------------
# READ WATERMARK
# -------------------------

watermark_df = spark.read.jdbc(
    url=jdbc_url,
    table="pipeline_state",
    properties=db_properties
)

last_ts = watermark_df.filter(
    col("pipeline_name") == "customer_pipeline"
).collect()[0]["last_processed_ts"]

print("Last watermark:", last_ts)

# -------------------------
# READ RAW DATA
# -------------------------

raw_df = spark.read.csv(
    "/app/data/raw_customers.csv",
    header=True,
    inferSchema=True
)

incremental_df = raw_df.filter(col("update_ts") > last_ts)

if incremental_df.count() == 0:
    print("No new records")
    spark.stop()
    exit()

stage_df = incremental_df.withColumn(
    "hash_diff",
    md5(concat_ws("||",
                  "first_name",
                  "last_name",
                  "email",
                  "city",
                  "country"
                  ))
)

# -------------------------
# WRITE TO STAGING TABLE
# -------------------------

stage_df.write.jdbc(
    url=jdbc_url,
    table="customer_stage",
    mode="overwrite",
    properties=db_properties
)

# -------------------------
# EXECUTE MERGE
# -------------------------

conn = psycopg2.connect(
    host="ecommerce_postgres",
    database="ecommerce_dw",
    user="admin",
    password="admin"
)

cur = conn.cursor()

expire_sql = """
MERGE INTO dim_customer AS d
USING customer_stage AS s
ON d.customer_id = s.customer_id
AND d.is_current = true

WHEN MATCHED AND d.hash_diff <> s.hash_diff THEN
UPDATE SET
    end_date = s.update_ts,
    is_current = false;
"""

insert_sql = """
INSERT INTO dim_customer (
    customer_id,
    first_name,
    last_name,
    email,
    city,
    country,
    start_date,
    end_date,
    is_current,
    hash_diff
)
SELECT
    s.customer_id,
    s.first_name,
    s.last_name,
    s.email,
    s.city,
    s.country,
    s.update_ts,
    NULL,
    true,
    s.hash_diff
FROM customer_stage s
LEFT JOIN dim_customer d
ON s.customer_id = d.customer_id
AND d.is_current = true
WHERE d.customer_id IS NULL
   OR d.hash_diff <> s.hash_diff;
"""

cur.execute(expire_sql)
cur.execute(insert_sql)
conn.commit()


# -------------------------
# UPDATE WATERMARK
# -------------------------

max_ts = incremental_df.agg(
    spark_max("update_ts")
).collect()[0][0]

cur.execute("""
UPDATE pipeline_state
SET last_processed_ts = %s
WHERE pipeline_name = 'customer_pipeline'
""", (max_ts,))

conn.commit()

cur.close()
conn.close()

spark.stop()

print("Incremental SCD2 load completed")