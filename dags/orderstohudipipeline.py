from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


# ============================================================
# 基础配置
# ============================================================

NAMESPACE = "bigdata"

FLINK_IMAGE = "bitnamilegacy/flink:1.20.1-debian-12-r5"

FLINK_JOB_NAME = "ordershudi"

GIT_REPO = "https://github.com/cuteeellamuwa-cmyk/airflow-dags.git"


# ============================================================
# Airflow DAG
# ============================================================

with DAG(
    dag_id="orderstohudipipeline",
    description="订单CDC实时数据流水线",
    start_date=datetime(2026, 10, 3),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigdata", "flink", "cdc", "kafka"],
) as dag:

    ensure_flink_job = KubernetesPodOperator(
        task_id="ensureflinkjob",
        name="ensure-orders-cdc-job",
        namespace=NAMESPACE,

        service_account_name="flinksubmit",

        image=FLINK_IMAGE,

        container_security_context=k8s.V1SecurityContext(
            run_as_user=0,
            run_as_group=0,
        ),

        cmds=["bash", "-c"],

        # ====================================================
        # 环境变量
        # ====================================================

        env_vars=[
            # Airflow Variable
            #
            # 如果存在：
            # ordershudi_last_savepoint
            #
            # 就把它传给临时 Flink Client Pod。
            #
            # 如果 Variable 不存在，则返回空字符串。
            # ------------------------------------------------
            k8s.V1EnvVar(
                name="RESTORE_SAVEPOINT",
                value=(
                    "{{ var.value.get("
                    "'ordershudi_last_savepoint', '') }}"
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


# ============================================================
# Kubernetes API
# ============================================================

K8S_TOKEN="$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)"

K8S_CA="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

K8S_API="https://kubernetes.default.svc"


# ============================================================
# 查询所有 Flink JobManager Pod
# ============================================================

JM_PODS_JSON="$(curl -fsS \
    --cacert "$K8S_CA" \
    -H "Authorization: Bearer $K8S_TOKEN" \
    "$K8S_API/api/v1/namespaces/{NAMESPACE}/pods?labelSelector=app.kubernetes.io%2Fcomponent%3Djobmanager,app.kubernetes.io%2Finstance%3Dflink")"


JM_IPS="$(printf '%s' "$JM_PODS_JSON" \
    | grep -oE '"podIP"[[:space:]]*:[[:space:]]*"[^"]+"' \
    | sed -E 's/.*"([^"]+)"$/\\1/')"


if [ -z "$JM_IPS" ]; then

    echo "错误：没有发现 Flink JobManager Pod IP。"

    exit 1

fi


# ============================================================
# 找当前 Leader
# ============================================================

FLINK_LEADER_IP=""


for ip in $JM_IPS; do

    echo "检查 JobManager: $ip"


    CODE="$(curl -sS \
        -o /dev/null \
        -w "%{{http_code}}" \
        --connect-timeout 3 \
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
echo "1. 检查订单 CDC Flink Job"
echo "========================================"


# ============================================================
# 如果 Job 已经 RUNNING，则不重复提交
# ============================================================

if /opt/bitnami/flink/bin/flink list \
    -m "$FLINK_LEADER" \
    | grep "{FLINK_JOB_NAME}" \
    | grep "(RUNNING)"; then


    echo ""

    echo "订单 CDC Job 已经处于 RUNNING 状态。"

    echo "不重复提交 CDC Job。"

    exit 0

fi



echo ""

echo "没有发现 RUNNING 的订单 CDC Job。"

echo "准备启动订单 CDC Job..."

echo ""



# ============================================================
# 2. 判断启动模式
# ============================================================

echo "========================================"
echo "2. 判断 CDC 启动模式"
echo "========================================"


if [ -n "$RESTORE_SAVEPOINT" ]; then

    echo "检测到 CDC Savepoint。"

    echo "本次启动模式：SAVEPOINT RESTORE"

    echo ""

    echo "Restore Path:"

    echo "$RESTORE_SAVEPOINT"

else

    echo "没有检测到 CDC Savepoint。"

    echo "本次启动模式：INITIAL"

fi



# ============================================================
# 3. 准备工作目录
# ============================================================

WORKDIR="/tmp/orderscdc"


rm -rf "$WORKDIR"

mkdir -p "$WORKDIR/lib"



echo ""
echo "========================================"
echo "3. 下载 GitHub SQL 模板"
echo "========================================"


# ============================================================
# 下载 SQL
# ============================================================

curl -fsSL \
    "https://raw.githubusercontent.com/cuteeellamuwa-cmyk/airflow-dags/main/flink/sql/orderstokafkahudi.sql" \
    -o "$WORKDIR/orderstokafkahudi.sql"


test -s "$WORKDIR/orderstokafkahudi.sql"


echo "SQL 模板下载成功。"



# ============================================================
# 5. 下载 Flink Connector
# ============================================================

echo ""
echo "========================================"
echo "5. 下载 Kafka / Hudi / Hive / Hadoop Connector"
echo "========================================"

curl -fL https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar   -o $WORKDIR/lib/flink-sql-connector-kafka-3.4.0-1.20.jar
curl -fL https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/3.4.3/hadoop-client-api-3.4.3.jar   -o $WORKDIR/lib/hadoop-client-api-3.4.3.jar
curl -fL https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.4.3/hadoop-client-runtime-3.4.3.jar   -o $WORKDIR/lib/hadoop-client-runtime-3.4.3.jar
curl -fL https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.3_2.12/1.20.1/flink-sql-connector-hive-3.1.3_2.12-1.20.1.jar   -o $WORKDIR/lib/flink-sql-connector-hive-3.1.3_2.12-1.20.1.jar
curl -fL https://repo.maven.apache.org/maven2/org/apache/hudi/hudi-flink1.20-bundle/1.2.1/hudi-flink1.20-bundle-1.2.1.jar   -o $WORKDIR/lib/hudi-flink1.20-bundle-1.2.1.jar
curl -fL https://repo.maven.apache.org/maven2/commons-codec/commons-codec/1.15/commons-codec-1.15.jar -o $WORKDIR/lib/commons-codec-1.15.jar
curl -fL https://repo.maven.apache.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar -o $WORKDIR/lib/commons-logging-1.2.jar

# ============================================================
# 6. 安装 Connector
# ============================================================

echo ""
echo "========================================"
echo "6. 安装 Connector"
echo "========================================"


cp "$WORKDIR/lib/"*.jar /opt/bitnami/flink/lib/


ls -lh /opt/bitnami/flink/lib/ | \
    grep -E "connector-kafka|hudi-flink|connector-hive|hadoop-client|commons-codec"



# ============================================================
# 7. 配置 Flink Client
# ============================================================

export FLINK_CFG_REST_ADDRESS="$FLINK_LEADER_IP"

export FLINK_CFG_REST_PORT="8081"

export FLINK_CFG_EXECUTION_ATTACHED="false"



# ============================================================
# 8. 提交 CDC Job
# ============================================================

echo ""
echo "========================================"
echo "7. 提交订单 CDC Flink Job"
echo "========================================"


# ------------------------------------------------------------
# 有 Savepoint
# ------------------------------------------------------------

if [ -n "$RESTORE_SAVEPOINT" ]; then


    echo "正在从 Savepoint 恢复 CDC Job..."


    /opt/bitnami/flink/bin/sql-client.sh \
        -Drest.address="$FLINK_LEADER_IP" \
        -Drest.port=8081 \
        -Dexecution.attached=false \
        -Dexecution.state-recovery.path="$RESTORE_SAVEPOINT" \
        -Dexecution.state-recovery.claim-mode=NO_CLAIM \
        -Dexecution.state-recovery.ignore-unclaimed-state=false \
        -f "$WORKDIR/orderstokafkahudi.sql"


# ------------------------------------------------------------
# 没有 Savepoint
# ------------------------------------------------------------

else


    echo "正在首次启动 CDC Job..."


    /opt/bitnami/flink/bin/sql-client.sh \
        -Drest.address="$FLINK_LEADER_IP" \
        -Drest.port=8081 \
        -Dexecution.attached=false \
        -f "$WORKDIR/orderstokafkahudi.sql"


fi



# ============================================================
# 9. 验证 Job
# ============================================================

echo ""
echo "========================================"
echo "8. 验证订单 CDC Job"
echo "========================================"


FOUND=0


for i in 1 2 3 4 5 6; do


    if /opt/bitnami/flink/bin/flink list \
        -m "$FLINK_LEADER" \
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


if [ -n "$RESTORE_SAVEPOINT" ]; then

    echo "启动模式：SAVEPOINT RESTORE"

    echo "恢复点：$RESTORE_SAVEPOINT"

else

    echo "启动模式：INITIAL"

fi

"""
        ],

        on_finish_action="delete_pod",

        get_logs=True,

        startup_timeout_seconds=300,
    )
