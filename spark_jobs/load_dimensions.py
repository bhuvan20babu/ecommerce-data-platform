from pyspark.sql import SparkSession
from pyspark.sql.functions import col, md5, concat_ws, max as spark_max, lead
from pyspark.sql.window import Window
from pyspark.sql.functions import expr
from pyspark.sql.functions import current_timestamp
from datetime import datetime
import psycopg2

spark = SparkSession.builder \
    .appName("Gold Layer SCD2 Customer Load") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://ecommerce_postgres:5432/ecommerce_dw"

db_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# --------------------------------
# READ WATERMARK
# --------------------------------

watermark_df = spark.read.jdbc(
    url=jdbc_url,
    table="pipeline_state",
    properties=db_properties
)

last_ts = watermark_df.filter(
    col("pipeline_name") == "customer_pipeline"
).collect()[0]["last_processed_ts"]

print("Last watermark:", last_ts)

# --------------------------------
# READ SILVER EVENTS
# --------------------------------

silver_df = spark.read.jdbc(
    url=jdbc_url,
    table="silver_customer_events",
    properties=db_properties
)

# --------------------------------
# FILTER INCREMENTAL EVENTS
# --------------------------------

incremental_df = silver_df.filter(
    col("event_time") > expr(f"timestamp '{last_ts}' - interval 7 days")
)

if incremental_df.count() == 0:
    print("No new events")
    spark.stop()
    exit()

# DQ Checks

from pyspark.sql.functions import lit, current_timestamp

print("Running DQ validation")

# --------------------------------
# INVALID RECORD CONDITIONS
# --------------------------------

invalid_null_customer = incremental_df.filter(
    col("customer_id").isNull()
).withColumn("dq_error_reason", lit("NULL_CUSTOMER_ID"))

invalid_future_time = incremental_df.filter(
    col("event_time") > current_timestamp()
).withColumn("dq_error_reason", lit("FUTURE_TIMESTAMP"))

duplicate_events = incremental_df.groupBy("event_id").count().filter(
    col("count") > 1
).select("event_id")

invalid_duplicates = incremental_df.join(
    duplicate_events,
    "event_id"
).withColumn("dq_error_reason", lit("DUPLICATE_EVENT"))

# --------------------------------
# COMBINE INVALID RECORDS
# --------------------------------

invalid_records = invalid_null_customer.unionByName(invalid_future_time).unionByName(invalid_duplicates)

invalid_count = invalid_records.count()

print("Invalid records:", invalid_count)

if invalid_count > 0:

    invalid_records.write.jdbc(
        url=jdbc_url,
        table="dq_failed_events",
        mode="append",
        properties=db_properties
    )

valid_records = incremental_df.join(
    invalid_records.select("event_id"),
    "event_id",
    "left_anti"
)


# --------------------------------
# HASH FOR CHANGE DETECTION
# --------------------------------

stage_df = valid_records.withColumn(
    "hash_diff",
    md5(concat_ws("||",
                  "first_name",
                  "last_name",
                  "email",
                  "city",
                  "country"
                  ))
)

# --------------------------------
# CONNECT POSTGRES
# --------------------------------

conn = psycopg2.connect(
    host="ecommerce_postgres",
    database="ecommerce_dw",
    user="admin",
    password="admin"
)

cur = conn.cursor()

rows = stage_df.collect()

for row in rows:

    customer_id = row["customer_id"]
    hash_diff = row["hash_diff"]
    event_time = row["event_time"]

    print("Processing:", customer_id, event_time)

    # -----------------------------
    # DETECT LATE EVENT
    # -----------------------------

    cur.execute("""
    SELECT start_date
    FROM dim_customer
    WHERE customer_id = %s
    AND is_current = true
    """, (customer_id,))

    result = cur.fetchone()

    late_event = False

    if result:
        current_start = result[0]
        if event_time < current_start:
            late_event = True

    # -----------------------------
    # HANDLE LATE EVENT
    # -----------------------------

    if late_event:

        print("Late arriving event detected:", customer_id)

        customer_events = silver_df.filter(
            col("customer_id") == customer_id
        )

        window = Window.partitionBy("customer_id").orderBy("event_time")

        timeline_df = customer_events.withColumn(
            "start_date",
            col("event_time")
        ).withColumn(
            "end_date",
            lead("event_time").over(window)
        )

        timeline_df = timeline_df.withColumn(
            "is_current",
            col("end_date").isNull()
        )

        timeline_df = timeline_df.withColumn(
            "hash_diff",
            md5(concat_ws("||",
                          "first_name",
                          "last_name",
                          "email",
                          "city",
                          "country"
                          ))
        )

        # delete old history
        cur.execute("""
        DELETE FROM dim_customer
        WHERE customer_id = %s
        """, (customer_id,))

        conn.commit()

        timeline_df.select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "city",
            "country",
            "start_date",
            "end_date",
            "is_current",
            "hash_diff"
        ).write.jdbc(
            url=jdbc_url,
            table="dim_customer",
            mode="append",
            properties=db_properties
        )

    else:

        # -----------------------------
        # NORMAL SCD2
        # -----------------------------

        expire_sql = """
        UPDATE dim_customer
        SET end_date = %s,
            is_current = false
        WHERE customer_id = %s
        AND is_current = true
        AND hash_diff <> %s
        """

        cur.execute(expire_sql, (event_time, customer_id, hash_diff))

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
        SELECT %s,%s,%s,%s,%s,%s,%s,NULL,true,%s
        WHERE NOT EXISTS (
            SELECT 1
            FROM dim_customer
            WHERE customer_id = %s
            AND is_current = true
            AND hash_diff = %s
        )
        """

        cur.execute(insert_sql, (
            row["customer_id"],
            row["first_name"],
            row["last_name"],
            row["email"],
            row["city"],
            row["country"],
            event_time,
            hash_diff,
            customer_id,
            hash_diff
        ))

    conn.commit()

# --------------------------------
# UPDATE WATERMARK
# --------------------------------

max_ts = incremental_df.agg(
    spark_max("event_time")
).collect()[0][0]

cur.execute("""
UPDATE pipeline_state
SET last_processed_ts = %s
WHERE pipeline_name = 'customer_pipeline'
""", (max_ts,))

processed_count = incremental_df.count()
loaded_count = valid_records.count()
failed_count = invalid_count

print(f"Processed: {processed_count}, Loaded: {loaded_count}, Failed: {failed_count}")

cur.execute("""
INSERT INTO pipeline_metrics
VALUES (%s, CURRENT_TIMESTAMP, %s, %s, %s)
""", (
    "customer_pipeline",
    processed_count,
    failed_count,
    loaded_count
))

conn.commit()

# --------------------------------
# Freshness SLA Check
# --------------------------------

latest_event_df = spark.read.jdbc(
    url=jdbc_url,
    table="dim_customer",
    properties=db_properties
)

latest_event_time = latest_event_df.agg(
    spark_max("start_date")
).collect()[0][0]

current_time = datetime.utcnow()

freshness_minutes = int(
    (current_time - latest_event_time).total_seconds() / 60
)

future_gold = latest_event_df.filter(
    col("start_date") > current_timestamp()
)

if future_gold.count() > 0:
    print("WARNING: Future timestamp detected in Gold")

if freshness_minutes < 0:
    freshness_minutes = 0

sla_threshold = 120

if freshness_minutes > sla_threshold:
    status = "SLA_BREACH"
else:
    status = "OK"

cur.execute("""
INSERT INTO pipeline_sla_monitor
VALUES (%s, CURRENT_TIMESTAMP, %s, %s, %s, %s)
""", (
    "customer_pipeline",
    latest_event_time,
    freshness_minutes,
    sla_threshold,
    status
))


conn.commit()

cur.close()
conn.close()

spark.stop()

print("Gold SCD2 load completed")