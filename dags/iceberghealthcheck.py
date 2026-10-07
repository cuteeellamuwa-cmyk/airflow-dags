from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


SPARK_IMAGE = "192.168.1.115:30500/cute/spark:3.5.9"

SPARK_COMMAND = r"""
set -e

/opt/spark/bin/spark-submit \
  --master k8s://https://kubernetes.default.svc:443 \
  --deploy-mode cluster \
  --name icebergfileanalysis \
  --conf spark.kubernetes.namespace=bigdata \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.container.image=192.168.1.115:30500/cute/spark:3.5.9 \
  --conf spark.kubernetes.hadoop.configMapName=spark-hadoop-config \
  --conf spark.kubernetes.driver.podTemplateFile=/opt/spark-template/driver-template.yaml \
  --conf spark.kubernetes.submission.waitAppCompletion=true \
  --conf spark.kubernetes.driver.request.cores=0.25 \
  --conf spark.kubernetes.executor.request.cores=0.25 \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=1g \
  --conf spark.executor.instances=1 \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hive \
  --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.kubernetes.driver.label.app=icebergfileanalysis \
  --conf spark.kubernetes.executor.label.app=icebergfileanalysis \
  --jars local:///opt/iceberg/iceberg-spark-runtime-3.5_2.12-1.10.2.jar \
  local:///opt/spark-job/iceberg-file-analysis.py
"""

with DAG(
    dag_id="iceberghealthcheck",
    start_date=datetime(2026, 10, 8),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["spark", "iceberg", "healthcheck"],
) as dag:

    iceberganalysis = KubernetesPodOperator(
        task_id="iceberganalysis",
        name="sparkiceberghealth",
        namespace="bigdata",
        image=SPARK_IMAGE,
        image_pull_policy="IfNotPresent",
        service_account_name="spark",
        in_cluster=True,
        get_logs=True,
        on_finish_action="keep_pod",
        random_name_suffix=True,
        cmds=["/bin/bash", "-c"],
        arguments=[SPARK_COMMAND],
        volumes=[
            k8s.V1Volume(
                name="spark-template",
                config_map=k8s.V1ConfigMapVolumeSource(
                    name="spark-iceberg-analysis-driver-template"
                ),
            ),
            k8s.V1Volume(
                name="spark-script",
                config_map=k8s.V1ConfigMapVolumeSource(
                    name="spark-iceberg-analysis-script"
                ),
            ),
        ],
        volume_mounts=[
            k8s.V1VolumeMount(
                name="spark-template",
                mount_path="/opt/spark-template",
                read_only=True,
            ),
            k8s.V1VolumeMount(
                name="spark-script",
                mount_path="/opt/spark-job",
                read_only=True,
            ),
        ],
    )
