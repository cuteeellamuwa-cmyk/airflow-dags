from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


# ============================================================
# 基础配置
# ============================================================

NAMESPACE = "bigdata"

FLINK_IMAGE = "bitnamilegacy/flink:1.20.1-debian-12-r5"

FLINK_JOB_NAME = "orderscdcsink"

SAVEPOINT_DIR = "hdfs://hdfs-namenode:9000/flink-savepoints"


# ============================================================
# Airflow DAG
# ============================================================

with DAG(
    dag_id="orderscdcstop",
    description="安全停止订单 CDC Flink Job 并创建 Savepoint",
    start_date=datetime(2026, 10, 4),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigdata", "flink", "cdc", "savepoint"],
) as dag:

    stop_flink_job = KubernetesPodOperator(
        task_id="stopflinkjob",
        name="stop-orders-cdc-job",
        namespace=NAMESPACE,

        service_account_name="flinksubmit",

        image=FLINK_IMAGE,

        cmds=["bash", "-c"],

        arguments=[
            f"""
set -e

echo "========================================"
echo "0. 自动发现 Flink Leader JobManager"
echo "========================================"

K8S_TOKEN="$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)"
K8S_CA="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
K8S_API="https://kubernetes.default.svc"

JM_PODS_JSON="$(curl -fsS \
    --cacert "$K8S_CA" \
    -H "Authorization: Bearer $K8S_TOKEN" \
    "$K8S_API/api/v1/namespaces/{NAMESPACE}/pods?labelSelector=app.kubernetes.io%2Fcomponent%3Djobmanager,app.kubernetes.io%2Finstance%3Dflink")"

JM_IPS="$(printf '%s' "$JM_PODS_JSON" \
    | grep -oE '"podIP"[[:space:]]*:[[:space:]]*"[^"]+"' \
    | sed -E 's/.*"([^"]+)"$/\\1/')"

if [ -z "$JM_IPS" ]; then
    echo "错误：没有发现 Flink JobManager Pod。"
    exit 1
fi


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
    echo "错误：没有找到 Flink Leader。"
    exit 1
fi


FLINK_LEADER="$FLINK_LEADER_IP:8081"


echo ""
echo "========================================"
echo "1. 查找 orders CDC Job"
echo "========================================"


JOB_LINE="$(/opt/bitnami/flink/bin/flink list \
    -m "$FLINK_LEADER" \
    | grep "{FLINK_JOB_NAME}" \
    | grep "(RUNNING)" \
    | head -1 || true)"


if [ -z "$JOB_LINE" ]; then

    echo ""
    echo "没有发现 RUNNING 的 {FLINK_JOB_NAME}。"
    echo "无需停止。"

    exit 0
fi


echo "发现 CDC Job："
echo "$JOB_LINE"


# Flink list 第一列就是 Job ID
JOB_ID="$(echo "$JOB_LINE" | awk '{{print $1}}')"


if [ -z "$JOB_ID" ]; then
    echo "错误：无法解析 Flink Job ID。"
    exit 1
fi


echo ""
echo "Job ID: $JOB_ID"


echo ""
echo "========================================"
echo "2. Stop With Savepoint"
echo "========================================"


STOP_OUTPUT="$(/opt/bitnami/flink/bin/flink stop \
    -p "{SAVEPOINT_DIR}" \
    "$JOB_ID" \
    -m "$FLINK_LEADER" 2>&1)"


echo "$STOP_OUTPUT"


echo ""
echo "========================================"
echo "3. 提取 Savepoint Path"
echo "========================================"


SAVEPOINT_PATH="$(printf '%s\n' "$STOP_OUTPUT" \
    | sed -n 's/^Savepoint completed\\. Path: //p' \
    | tail -1)"


if [ -z "$SAVEPOINT_PATH" ]; then

    echo "错误：Flink 没有返回 Savepoint Path。"
    exit 1

fi


echo ""
echo "SAVEPOINT_PATH=$SAVEPOINT_PATH"


echo ""
echo "========================================"
echo "订单 CDC Job 已安全停止"
echo "========================================"

echo "Job ID:"
echo "$JOB_ID"

echo ""
echo "Savepoint:"
echo "$SAVEPOINT_PATH"

"""
        ],

        on_finish_action="delete_pod",

        get_logs=True,

        startup_timeout_seconds=300,
    )
