"""
DAG de Airflow: Pipeline E-Commerce
Ejecuta diariamente: generar datos → ingestar → dbt run → dbt test
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "samuelrisso",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="Pipeline de datos e-commerce: ingestión → transformación → testing",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "pipeline"],
) as dag:

    generar_datos = BashOperator(
        task_id="generar_datos",
        bash_command="python /app/jobs/generar_datos.py",
    )

    ingestar_kafka = BashOperator(
        task_id="ingestar_kafka",
        bash_command="python /app/jobs/producer.py",
    )

    consumir_a_minio = BashOperator(
        task_id="consumir_a_minio",
        bash_command="python /app/jobs/consumer.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --project-dir /app/dbt --profiles-dir /app/dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --project-dir /app/dbt --profiles-dir /app/dbt",
    )

    # Dependencias del DAG
    generar_datos >> ingestar_kafka >> consumir_a_minio >> dbt_run >> dbt_test
