from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2026, 9, 17),
    schedule=None,
    catchup=False,
) as dag:

    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'Hello Airflow from Kubernetes!'",
    )
