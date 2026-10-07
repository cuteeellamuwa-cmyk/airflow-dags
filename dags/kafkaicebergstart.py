from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


# ============================================================
# 基础配置
# ============================================================

NAMESPACE = "bigdata"

FLINK_IMAGE = "bitnamilegacy/flink:1.20.1-debian-12-r5"

FLINK_JOB_NAME = "ordersversionfilter"


# ============================================================
# Airflow DAG
# ============================================================

with DAG(
    dag_id="kafkaicebergstart",
    description="订单 Kafka 到 Iceberg 实时数据流水线",
    start_date=datetime(2026, 10, 7),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigdata", "flink", "kafka", "iceberg"],
) as dag:

    ensure_flink_job = KubernetesPodOperator(
        task_id="ensureflinkjob",
        name="ensure-orders-iceberg-job",
        namespace=NAMESPACE,

        service_account_name="flinksubmit",

        image=FLINK_IMAGE,

        container_security_context=k8s.V1SecurityContext(
            run_as_user=0,
            run_as_group=0,
        ),

        cmds=["bash", "-c"],

        env_vars=[
            k8s.V1EnvVar(
                name="RESTORE_SAVEPOINT",
                value=(
                    "{{ var.value.get("
                    "'ordersiceberglastsavepoint', '') }}"
                ),
            ),
        ],

        volumes=[
            k8s.V1Volume(
                name="hadoop-conf",
                config_map=k8s.V1ConfigMapVolumeSource(
                    name="flink-hadoop-conf",
                ),
            ),
            k8s.V1Volume(
                name="hive-metastore-config",
                config_map=k8s.V1ConfigMapVolumeSource(
                    name="hive-metastore-config",
                    items=[
                        k8s.V1KeyToPath(
                            key="hive-site.xml",
                            path="hive-site.xml",
                        ),
                    ],
                ),
            ),
        ],

        volume_mounts=[
            k8s.V1VolumeMount(
                name="hadoop-conf",
                mount_path="/opt/hadoop-conf",
                read_only=True,
            ),
            k8s.V1VolumeMount(
                name="hive-metastore-config",
                mount_path="/opt/bitnami/flink/conf/hive-site.xml",
                sub_path="hive-site.xml",
                read_only=True,
            ),
        ],

        arguments=[
            f"""
set -e

export HADOOP_CONF_DIR="/opt/hadoop-conf"

echo "========================================"
echo "0. 自动发现 Flink Leader JobManager"
echo "========================================"

K8S_TOKEN="$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)"
K8S_CA="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
K8S_API="https://kubernetes.default.svc"

JM_PODS_JSON="$(curl -fsS \\
    --cacert "$K8S_CA" \\
    -H "Authorization: Bearer $K8S_TOKEN" \\
    "$K8S_API/api/v1/namespaces/{NAMESPACE}/pods?labelSelector=app.kubernetes.io%2Fcomponent%3Djobmanager,app.kubernetes.io%2Finstance%3Dflink")"

JM_IPS="$(printf '%s' "$JM_PODS_JSON" \\
    | grep -oE '"podIP"[[:space:]]*:[[:space:]]*"[^"]+"' \\
    | sed -E 's/.*"([^"]+)"$/\\1/')"

if [ -z "$JM_IPS" ]; then
    echo "错误：没有发现 Flink JobManager Pod IP。"
    exit 1
fi

FLINK_LEADER_IP=""

for ip in $JM_IPS; do

    echo "检查 JobManager: $ip"

    CODE="$(curl -sS \\
        -o /dev/null \\
        -w "%{{http_code}}" \\
        --connect-timeout 3 \\
        "http://$ip:8081/config" || true)"

    if [ "$CODE" = "200" ]; then
        FLINK_LEADER_IP="$ip"
        echo "发现当前 Flink Leader: $FLINK_LEADER_IP"
        break
    fi

    echo "JobManager $ip 不是当前 Leader，HTTP=$CODE"

done

if [ -z "$FLINK_LEADER_IP" ]; then
    echo "错误：没有找到可用的 Flink Leader JobManager。"
    exit 1
fi

FLINK_LEADER="$FLINK_LEADER_IP:8081"


echo ""
echo "========================================"
echo "1. 检查 ordersiceberg Flink Job"
echo "========================================"

if /opt/bitnami/flink/bin/flink list \\
    -m "$FLINK_LEADER" \\
    | grep "{FLINK_JOB_NAME}" \\
    | grep "(RUNNING)"; then

    echo ""
    echo "ordersiceberg Job 已经处于 RUNNING 状态。"
    echo "不重复提交 Iceberg Job。"
    exit 0
fi


echo ""
echo "没有发现 RUNNING 的 ordersiceberg Job。"
echo "准备启动 ordersiceberg Job..."


echo ""
echo "========================================"
echo "2. 判断 Iceberg 启动模式"
echo "========================================"

if [ -n "$RESTORE_SAVEPOINT" ]; then

    echo "检测到 Iceberg Savepoint。"
    echo "本次启动模式：SAVEPOINT RESTORE"
    echo ""
    echo "Restore Path:"
    echo "$RESTORE_SAVEPOINT"

else

    echo "没有检测到 Iceberg Savepoint。"
    echo "本次启动模式：INITIAL"

fi


WORKDIR="/tmp/ordersiceberg"

rm -rf "$WORKDIR"
mkdir -p "$WORKDIR"


echo ""
echo "========================================"
echo "3. 检查正式 Savepoint"
echo "========================================"

if [ -z "$RESTORE_SAVEPOINT" ]; then
    echo "错误：ordersversionfilter 是有状态正式 Job。"
    echo "未检测到 ordersiceberglastsavepoint，拒绝空 State 启动。"
    exit 1
fi

echo "正式启动模式：SAVEPOINT RESTORE"
echo "Restore Path:"
echo "$RESTORE_SAVEPOINT"


echo ""
echo "========================================"
echo "4. 下载 OrdersVersionFilter JAR"
echo "========================================"

curl -fsSL \
    "https://raw.githubusercontent.com/cuteeellamuwa-cmyk/airflow-dags/main/jars/ordersversionfilter-1.0.0.jar" \
    -o "$WORKDIR/ordersversionfilter-1.0.0.jar"

test -s "$WORKDIR/ordersversionfilter-1.0.0.jar"

echo "JAR 下载成功："
ls -lh "$WORKDIR/ordersversionfilter-1.0.0.jar"

echo ""
echo "下载 Hadoop Client（Flink 提交端依赖）..."

curl -fsSL \
    "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/3.4.3/hadoop-client-api-3.4.3.jar" \
    -o "/opt/bitnami/flink/lib/hadoop-client-api-3.4.3.jar"

curl -fsSL \
    "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.4.3/hadoop-client-runtime-3.4.3.jar" \
    -o "/opt/bitnami/flink/lib/hadoop-client-runtime-3.4.3.jar"

test -s "/opt/bitnami/flink/lib/hadoop-client-api-3.4.3.jar"
test -s "/opt/bitnami/flink/lib/hadoop-client-runtime-3.4.3.jar"

echo "Hadoop Client 已准备完成。"

echo ""
echo "下载 Iceberg Flink Runtime（Flink 提交端依赖）..."

curl -fsSL \
    "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.10.1/iceberg-flink-runtime-1.20-1.10.1.jar" \
    -o "/opt/bitnami/flink/lib/iceberg-flink-runtime-1.20-1.10.1.jar"

test -s "/opt/bitnami/flink/lib/iceberg-flink-runtime-1.20-1.10.1.jar"

echo "Iceberg Flink Runtime 已准备完成。"

echo ""
echo "下载 Flink Hive Connector（Iceberg Hive Catalog 提交端依赖）..."

curl -fsSL \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.3_2.12/1.20.1/flink-sql-connector-hive-3.1.3_2.12-1.20.1.jar" \
    -o "/opt/bitnami/flink/lib/flink-sql-connector-hive-3.1.3_2.12-1.20.1.jar"

test -s "/opt/bitnami/flink/lib/flink-sql-connector-hive-3.1.3_2.12-1.20.1.jar"

echo "Flink Hive Connector 已准备完成。"

echo ""
echo "下载 Commons Logging（Hive/Hadoop 提交端依赖）..."

curl -fsSL \
    "https://repo.maven.apache.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar" \
    -o "/opt/bitnami/flink/lib/commons-logging-1.2.jar"

test -s "/opt/bitnami/flink/lib/commons-logging-1.2.jar"

echo "Commons Logging 已准备完成。"




echo ""
echo "========================================"
echo "5. 提交 ordersversionfilter Flink Job"
echo "========================================"

export FLINK_CFG_REST_ADDRESS="$FLINK_LEADER_IP"
export FLINK_CFG_REST_PORT="8081"
export FLINK_CFG_EXECUTION_ATTACHED="false"

set +e

SUBMIT_OUTPUT="$(/opt/bitnami/flink/bin/flink run \\
    -d \\
    -m "$FLINK_LEADER" \\
    -s "$RESTORE_SAVEPOINT" \\
    -n \\
    -c com.cute.flink.OrdersVersionFilter \\
    "$WORKDIR/ordersversionfilter-1.0.0.jar" 2>&1)"

SUBMIT_RC=$?

set -e

echo "$SUBMIT_OUTPUT"

if [ "$SUBMIT_RC" -ne 0 ]; then
    echo ""
    echo "错误：ordersversionfilter 提交失败，退出码=$SUBMIT_RC"
    exit "$SUBMIT_RC"
fi


echo ""
echo "========================================"
echo "6. 验证 ordersversionfilter Job"
echo "========================================"

FOUND=0

for i in 1 2 3 4 5 6; do

    if /opt/bitnami/flink/bin/flink list \\
        -m "$FLINK_LEADER" \\
        | grep "{FLINK_JOB_NAME}" \\
        | grep "(RUNNING)"; then

        FOUND=1
        break
    fi

    echo "等待 Flink Job RUNNING... $i/6"
    sleep 5

done


if [ "$FOUND" -ne 1 ]; then
    echo "错误：ordersversionfilter Job 提交后没有进入 RUNNING 状态。"
    exit 1
fi


echo ""
echo "========================================"
echo "ordersversionfilter → Iceberg Pipeline 已正常运行"
echo "========================================"

echo "启动模式：SAVEPOINT RESTORE"
echo "恢复点：$RESTORE_SAVEPOINT"

"""
        ],

        on_finish_action="delete_pod",
        get_logs=True,
        startup_timeout_seconds=300,
    )
