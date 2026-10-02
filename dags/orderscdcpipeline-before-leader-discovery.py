from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


# ============================================================
# 基础配置
# ============================================================

# Kubernetes 命名空间
NAMESPACE = "bigdata"

# 与当前 Flink 运行时保持一致：Flink 1.20.1
FLINK_IMAGE = "bitnamilegacy/flink:1.20.1-debian-12-r5"

# Flink JobManager Service
FLINK_JOBMANAGER = "flink-jobmanager:8081"

# 用于判断 CDC Job 是否已经运行
FLINK_JOB_NAME = "orderscdcsink"

# GitHub DAG / SQL 仓库
GIT_REPO = "https://github.com/cuteeellamuwa-cmyk/airflow-dags.git"


# ============================================================
# Airflow DAG
# ============================================================

with DAG(
    dag_id="orderscdcpipeline",
    description="订单CDC实时数据流水线",
    start_date=datetime(2026, 10, 3),
    schedule=None,
    catchup=False,

    # 防止两个 DAG Run 同时检查后又同时提交 CDC Job
    max_active_runs=1,

    tags=["bigdata", "flink", "cdc", "kafka"],
) as dag:

    # ========================================================
    # Task：确保订单 CDC Flink Job 正在运行
    # ========================================================

    ensure_flink_job = KubernetesPodOperator(
        task_id="ensureflinkjob",
        name="ensure-orders-cdc-job",
        namespace=NAMESPACE,

        # Flink 1.20.1 Client
        image=FLINK_IMAGE,

        cmds=["bash", "-c"],

        # ----------------------------------------------------
        # Kubernetes Secret → Pod 环境变量
        # ----------------------------------------------------
        env_vars=[
            k8s.V1EnvVar(
                name="CDC_USERNAME",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="flinkcdcsecret",
                        key="username",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="CDC_PASSWORD",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="flinkcdcsecret",
                        key="password",
                    )
                ),
            ),
        ],

        arguments=[
            f"""
set -e

echo "========================================"
echo "1. 检查订单 CDC Flink Job"
echo "========================================"

if /opt/bitnami/flink/bin/flink list \
    -m {FLINK_JOBMANAGER} \
    | grep "{FLINK_JOB_NAME}" \
    | grep "(RUNNING)"; then

    echo ""
    echo "订单 CDC Job 已经处于 RUNNING 状态。"
    echo "不重复提交 CDC Job。"
    exit 0
fi


echo ""
echo "没有发现 RUNNING 的订单 CDC Job。"
echo "开始自动提交..."
echo ""


# ============================================================
# 2. 准备工作目录
# ============================================================

WORKDIR="/tmp/orderscdc"

rm -rf "$WORKDIR"
mkdir -p "$WORKDIR/lib"


# ============================================================
# 3. 从 GitHub 获取 SQL 模板
# ============================================================

echo "========================================"
echo "2. 下载 GitHub SQL 模板"
echo "========================================"

# Flink 镜像中不依赖 git，直接下载 GitHub Raw 文件
curl -fsSL \
    "https://raw.githubusercontent.com/cuteeellamuwa-cmyk/airflow-dags/main/flink/sql/orderscdctokafka.sql" \
    -o "$WORKDIR/orderscdctemplate.sql"

test -s "$WORKDIR/orderscdctemplate.sql"

echo "SQL 模板下载成功。"


# ============================================================
# 4. Secret 变量替换
# ============================================================

echo ""
echo "========================================"
echo "3. 注入 MySQL CDC Secret"
echo "========================================"

# 使用 sed 在运行时注入 Kubernetes Secret
# 不输出最终 SQL，避免密码出现在 Airflow 日志
sed \
    -e "s|\\${{CDC_USERNAME}}|$CDC_USERNAME|g" \
    -e "s|\\${{CDC_PASSWORD}}|$CDC_PASSWORD|g" \
    "$WORKDIR/orderscdctemplate.sql" \
    > "$WORKDIR/orderscdctokafka.sql"

# 检查模板变量是否全部替换成功
if grep -q '\\${{CDC_USERNAME}}\\|\\${{CDC_PASSWORD}}' \
    "$WORKDIR/orderscdctokafka.sql"; then
    echo "错误：CDC Secret 变量替换失败。"
    exit 1
fi

echo "CDC 用户名和密码已完成运行时注入。"
echo "不会把密码输出到 Airflow 日志。"


# ============================================================
# 5. 下载 Flink Connector
# ============================================================

echo ""
echo "========================================"
echo "4. 下载 Flink Connector"
echo "========================================"

curl -fsSL \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-mysql-cdc/3.3.0/flink-sql-connector-mysql-cdc-3.3.0.jar" \
    -o "$WORKDIR/lib/flink-sql-connector-mysql-cdc-3.3.0.jar"

curl -fsSL \
    "https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar" \
    -o "$WORKDIR/lib/mysql-connector-java-8.0.27.jar"

curl -fsSL \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar" \
    -o "$WORKDIR/lib/flink-sql-connector-kafka-3.4.0-1.20.jar"


# ============================================================
# 6. 安装 Connector 到临时 Flink Client
# ============================================================

echo ""
echo "========================================"
echo "5. 安装 Connector"
echo "========================================"

cp "$WORKDIR/lib/"*.jar /opt/bitnami/flink/lib/

ls -lh /opt/bitnami/flink/lib/ | \
    grep -E "mysql-cdc|mysql-connector|connector-kafka"


# ============================================================
# 7. 配置现有 Flink JobManager
# ============================================================

export FLINK_CFG_REST_ADDRESS="flink-jobmanager"
export FLINK_CFG_REST_PORT="8081"

# Streaming Job 提交成功以后让 SQL Client 退出，
# Job 本身继续由 Flink 集群运行。
export FLINK_CFG_EXECUTION_ATTACHED="false"


# ============================================================
# 8. 提交订单 CDC Job
# ============================================================

echo ""
echo "========================================"
echo "6. 提交订单 CDC Flink Job"
echo "========================================"

/opt/bitnami/flink/bin/sql-client.sh \
    -f "$WORKDIR/orderscdctokafka.sql"


# ============================================================
# 9. 等待并验证 Job
# ============================================================

echo ""
echo "========================================"
echo "7. 验证订单 CDC Job"
echo "========================================"

FOUND=0

for i in 1 2 3 4 5 6; do

    if /opt/bitnami/flink/bin/flink list \
        -m {FLINK_JOBMANAGER} \
        | grep "{FLINK_JOB_NAME}" \
        | grep "(RUNNING)"; then

        FOUND=1
        break
    fi

    echo "等待 Flink Job RUNNING... $i/6"
    sleep 5
done


if [ "$FOUND" -ne 1 ]; then
    echo "错误：订单 CDC Job 提交后没有进入 RUNNING 状态。"
    exit 1
fi


echo ""
echo "========================================"
echo "订单 CDC Pipeline 已正常运行"
echo "========================================"
"""
        ],

        # Pod 完成后删除
        on_finish_action="delete_pod",

        # Pod 日志进入 Airflow Task Log
        get_logs=True,

        # Pod 启动超时时间
        startup_timeout_seconds=300,
    )
