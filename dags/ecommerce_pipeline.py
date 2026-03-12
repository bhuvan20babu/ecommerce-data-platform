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

    load_dimensions = BashOperator(
        task_id="load_dimensions",
        bash_command="docker exec ecommerce_spark /opt/spark/bin/spark-submit /app/spark_jobs/load_dimensions.py"
    )

    load_products = BashOperator(
        task_id="load_products",
        bash_command="docker exec ecommerce_spark /opt/spark/bin/spark-submit /app/spark_jobs/load_products.py"
    )

    load_dates = BashOperator(
        task_id="load_date_dimension",
        bash_command="docker exec ecommerce_spark /opt/spark/bin/spark-submit /app/spark_jobs/load_date_dimension.py"
    )

    load_facts = BashOperator(
        task_id="load_facts",
        bash_command="docker exec ecommerce_spark /opt/spark/bin/spark-submit /app/spark_jobs/load_facts.py"
    )

    load_dimensions >> load_products >> load_dates >> load_facts