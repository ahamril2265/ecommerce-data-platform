from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_batch_pipeline",
    description="Daily E-commerce Batch Data Pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "batch", "medallion"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_synthetic_data",
        bash_command="""
        cd /opt/airflow/projects/ecommerce-data-platform &&
        python data_generator/run_generation.py
        """
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver_transform",
        bash_command="""
        cd /opt/airflow/projects/ecommerce-data-platform &&
        python pipelines/transform/bronze_to_silver/run_bronze_to_silver.py
        """
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold_transform",
        bash_command="""
        cd /opt/airflow/projects/ecommerce-data-platform &&
        python pipelines/transform/silver_to_gold/run_silver_to_gold.py
        """
    )

    generate_data >> bronze_to_silver >> silver_to_gold
