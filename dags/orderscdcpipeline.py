from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


# ============================================================
# 基础配置
# ============================================================

# Kubernetes 命名空间
# Flink、Kafka、MySQL、Airflow 都运行在 bigdata namespace
NAMESPACE = "bigdata"

# Flink 客户端镜像
# 必须和当前实际 Flink 集群版本保持一致：1.20.1
FLINK_IMAGE = "bitnamilegacy/flink:1.20.1-debian-12-r5"

# Flink JobManager REST 地址
# 临时 Pod 通过这个 Service 访问现有 Flink 集群
FLINK_JOBMANAGER = "flink-jobmanager:8081"

# 用于识别订单 CDC Job 的稳定业务名称
# 不使用 Job ID，因为 Job 每次重新提交后 Job ID 都可能变化
FLINK_JOB_NAME = "orderscdcsink"


# ============================================================
# Airflow DAG
# ============================================================

with DAG(

    # DAG 唯一标识
    dag_id="orderscdcpipeline",

    # 中文说明
    description="订单CDC实时数据流水线",

    # DAG 生效时间
    start_date=datetime(2026, 10, 3),

    # 不设置自动定时任务
    # 当前阶段由我们手动触发
    schedule=None,

    # 不补跑 start_date 到现在之间的历史任务
    catchup=False,

    # Airflow UI 分类标签
    tags=[
        "bigdata",
        "flink",
        "cdc",
        "kafka",
    ],

) as dag:

    # ========================================================
    # Task 1：检查订单 CDC Flink Job
    # ========================================================

    check_flink_job = KubernetesPodOperator(

        # Airflow Task ID
        task_id="checkflinkjob",

        # Kubernetes 临时 Pod 名称
        name="check-orders-cdc-job",

        # Pod 创建到 bigdata namespace
        namespace=NAMESPACE,

        # 使用与 Flink 集群相同版本的镜像
        image=FLINK_IMAGE,

        # 使用 shell 执行下面的检查脚本
        cmds=[
            "sh",
            "-c",
        ],

        # 实际执行的 Shell 脚本
        arguments=[
            f"""
            echo "========================================"
            echo "正在检查订单 CDC Flink Job..."
            echo "目标 Job：{FLINK_JOB_NAME}"
            echo "Flink：{FLINK_JOBMANAGER}"
            echo "========================================"

            if /opt/bitnami/flink/bin/flink list \
                -m {FLINK_JOBMANAGER} \
                | grep "{FLINK_JOB_NAME}" \
                | grep "(RUNNING)"; then

                echo ""
                echo "订单 CDC Job 已经处于 RUNNING 状态。"
                echo "为了避免重复消费 MySQL CDC 和重复写入 Kafka，"
                echo "本次不需要启动新的 CDC Job。"

                exit 0

            else

                echo ""
                echo "没有发现正在 RUNNING 的订单 CDC Job。"
                echo "后续正式版本将在这里进入 Job 提交流程。"

                exit 0

            fi
            """
        ],

        # Pod 执行结束后自动删除
        # 防止长期留下大量 Completed Pod
        on_finish_action="delete_pod",

        # 把临时 Pod 的日志输出到 Airflow Task Log
        get_logs=True,
    )
