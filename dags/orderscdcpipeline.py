from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


# ============================================================
# 基础配置
# ============================================================

NAMESPACE = "bigdata"

FLINK_IMAGE = "bitnamilegacy/flink:1.20.1-debian-12-r5"

FLINK_JOBMANAGER = "flink-jobmanager:8081"


# ============================================================
# Airflow DAG
# ============================================================

with DAG(
    dag_id="orderscdcpipeline",

    # DAG 生效起始日期
    start_date=datetime(2026, 10, 3),

    # 不设置定时周期，暂时由我们手动触发
    schedule=None,

    # 不补跑历史任务
    catchup=False,

    # Airflow UI 中用于分类
    tags=["bigdata", "flink", "cdc", "kafka"],
) as dag:

    check_flink_job = KubernetesPodOperator(
        # Airflow Task 名称
        task_id="check_flink_job",

        # 临时 Pod 名称
        name="check-orders-cdc-job",

        # Pod 创建到 bigdata namespace
        namespace=NAMESPACE,

        # 使用和当前 Flink 集群相同版本的镜像
        image=FLINK_IMAGE,

        # Pod 内执行的程序
        cmds=[
            "/opt/bitnami/flink/bin/flink",
        ],

        # 相当于：
        # flink list -m flink-jobmanager:8081
        arguments=[
            "list",
            "-m",
            FLINK_JOBMANAGER,
        ],

        # 执行结束后删除临时 Pod
        on_finish_action="delete_pod",

        # 把 Pod 日志显示到 Airflow Task Log
        get_logs=True,
    )
