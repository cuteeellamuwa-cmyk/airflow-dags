from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="bigdata_pipeline",
    start_date=datetime(2026, 9, 17),
    schedule=None,
    catchup=False,
    tags=["bigdata", "kubernetes"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Starting big data pipeline...'",
    )

    check_hdfs = BashOperator(
        task_id="check_hdfs",
        bash_command="""
        echo "Checking HDFS..."
        kubectl get pods -n bigdata | grep hdfs
        """,
    )

    check_kafka = BashOperator(
        task_id="check_kafka",
        bash_command="""
        echo "Checking Kafka..."
        kubectl get pods -n bigdata | grep kafka
        """,
    )

    check_flink = BashOperator(
        task_id="check_flink",
        bash_command="""
        echo "Checking Flink..."
        kubectl get pods -n bigdata | grep flink
        """,
    )

    finish = BashOperator(
        task_id="finish",
        bash_command="echo 'Big data pipeline finished!'",
    )

    start >> check_hdfs >> check_kafka >> check_flink >> finish
