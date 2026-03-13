from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "bhuvan",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="ecommerce_data_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    bronze = BashOperator(
        task_id="bronze_ingest",
        bash_command="docker exec ecommerce_spark /opt/spark/bin/spark-submit /app/spark_jobs/bronze_ingest.py"
    )

    silver = BashOperator(
    task_id="silver_deduplicate",
    bash_command="docker exec ecommerce_spark /opt/spark/bin/spark-submit /app/spark_jobs/silver_deduplicate.py"
    )

    gold = BashOperator(
    task_id="gold_customer_dimension",
    bash_command="docker exec ecommerce_spark /opt/spark/bin/spark-submit /app/spark_jobs/load_dimensions.py"
    )

    bronze >> silver >> gold