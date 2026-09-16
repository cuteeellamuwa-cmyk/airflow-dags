from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


with DAG(
    dag_id="bigdata_pipeline",
    start_date=datetime(2026, 9, 17),
    schedule=None,
    catchup=False,
    tags=["bigdata", "kubernetes"],
) as dag:

    start = KubernetesPodOperator(
        task_id="start",
        name="airflow-k8s-start",
        namespace="bigdata",
        image="apache/airflow:3.2.2",
        cmds=["bash", "-c"],
        arguments=[
            "echo 'Starting big data pipeline from KubernetesPodOperator...'"
        ],
        service_account_name="airflow-worker",
        get_logs=True,
        is_delete_operator_pod=True,
    )

    check_hdfs = KubernetesPodOperator(
        task_id="check_hdfs",
        name="airflow-k8s-check-hdfs",
        namespace="bigdata",
        image="apache/airflow:3.2.2",
        cmds=["python", "-c"],
        arguments=[
            """
from kubernetes import client, config
config.load_incluster_config()
v1 = client.CoreV1Api()
pods = v1.list_namespaced_pod(namespace="bigdata")
print("=== HDFS Pods ===")
found = False
for pod in pods.items:
    name = pod.metadata.name
    if "hdfs" in name.lower():
        found = True
        print(name, "->", pod.status.phase)
if not found:
    print("No HDFS pod found")
"""
        ],
        service_account_name="airflow-worker",
        get_logs=True,
        is_delete_operator_pod=True,
    )

    check_kafka = KubernetesPodOperator(
        task_id="check_kafka",
        name="airflow-k8s-check-kafka",
        namespace="bigdata",
        image="apache/airflow:3.2.2",
        cmds=["python", "-c"],
        arguments=[
            """
from kubernetes import client, config
config.load_incluster_config()
v1 = client.CoreV1Api()
pods = v1.list_namespaced_pod(namespace="bigdata")
print("=== Kafka Pods ===")
found = False
for pod in pods.items:
    name = pod.metadata.name
    if "kafka" in name.lower():
        found = True
        print(name, "->", pod.status.phase)
if not found:
    print("No Kafka pod found")
"""
        ],
        service_account_name="airflow-worker",
        get_logs=True,
        is_delete_operator_pod=True,
    )

    check_flink = KubernetesPodOperator(
        task_id="check_flink",
        name="airflow-k8s-check-flink",
        namespace="bigdata",
        image="apache/airflow:3.2.2",
        cmds=["python", "-c"],
        arguments=[
            """
from kubernetes import client, config
config.load_incluster_config()
v1 = client.CoreV1Api()
pods = v1.list_namespaced_pod(namespace="bigdata")
print("=== Flink Pods ===")
found = False
for pod in pods.items:
    name = pod.metadata.name
    if "flink" in name.lower():
        found = True
        print(name, "->", pod.status.phase)
if not found:
    print("No Flink pod found")
"""
        ],
        service_account_name="airflow-worker",
        get_logs=True,
        is_delete_operator_pod=True,
    )

    finish = KubernetesPodOperator(
        task_id="finish",
        name="airflow-k8s-finish",
        namespace="bigdata",
        image="apache/airflow:3.2.2",
        cmds=["bash", "-c"],
        arguments=[
            "echo 'Big data pipeline finished successfully!'"
        ],
        service_account_name="airflow-worker",
        get_logs=True,
        is_delete_operator_pod=True,
    )

    start >> check_hdfs >> check_kafka >> check_flink >> finish
