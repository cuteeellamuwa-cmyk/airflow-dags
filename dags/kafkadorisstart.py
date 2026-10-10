from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


NAMESPACE = "bigdata"
FLINK_IMAGE = "bitnamilegacy/flink:1.20.1-debian-12-r5"
FLINK_JOB_NAME = "ordersdorissink"

JAR_URL = (
    "https://raw.githubusercontent.com/"
    "cuteeellamuwa-cmyk/airflow-dags/main/"
    "jars/ordersdorissink-1.0.0.jar"
)

# 这里使用你已经安装过的同版本 Doris Connector。
# 提交 Pod 是临时创建的，因此需要单独下载。
CONNECTOR_URL = (
    "https://repo.maven.apache.org/maven2/"
    "org/apache/doris/flink-doris-connector-1.20/"
    "26.2.0/flink-doris-connector-1.20-26.2.0.jar"
)


with DAG(
    dag_id="kafkadorisstart",
    description="Kafka CDC → Flink → Doris 实时订单同步",
    start_date=datetime(2026, 10, 10),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigdata", "flink", "kafka", "doris"],
) as dag:

    start_doris_job = KubernetesPodOperator(
        task_id="startdorisjob",
        name="start-orders-doris-job",
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
                name="DORIS_USERNAME",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="doris-sink-secret",
                        key="username",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="DORIS_PASSWORD",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="doris-sink-secret",
                        key="password",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="RESTORE_SAVEPOINT",
                value="{{ var.value.get('ordersdorislastsavepoint', '') }}",
            ),
            k8s.V1EnvVar(
                name="ALLOW_INITIAL",
                value="{{ var.value.get('ordersdorisallowinitial', 'false') }}",
            ),
        ],

        arguments=[
            r"""
set -euo pipefail

WORKDIR="/tmp/ordersdoris"
mkdir -p "$WORKDIR"

echo "1. 下载 Doris Job JAR"

curl -fsSL "__JAR_URL__" \
    -o "$WORKDIR/ordersdorissink-1.0.0.jar"

test -s "$WORKDIR/ordersdorissink-1.0.0.jar"

echo "2. 下载 Doris Connector"

curl -fsSL "__CONNECTOR_URL__" \
    -o /opt/bitnami/flink/lib/flink-doris-connector-1.20-26.2.0.jar

echo "下载 Kafka Connector"

curl -fsSL   "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar"   -o /opt/bitnami/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar

test -s /opt/bitnami/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar

echo "3. 自动发现 Flink Leader"

K8S_TOKEN="$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)"
K8S_CA="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
K8S_API="https://kubernetes.default.svc"

JM_PODS_JSON="$(curl -fsS \
    --cacert "$K8S_CA" \
    -H "Authorization: Bearer $K8S_TOKEN" \
    "$K8S_API/api/v1/namespaces/bigdata/pods?labelSelector=app.kubernetes.io%2Fcomponent%3Djobmanager,app.kubernetes.io%2Finstance%3Dflink")"

JM_IPS="$(printf '%s' "$JM_PODS_JSON" \
    | grep -oE '"podIP"[[:space:]]*:[[:space:]]*"[^"]+"' \
    | sed -E 's/.*"([^"]+)"$/\1/')"

FLINK_LEADER_IP=""

for ip in $JM_IPS; do
    CODE="$(curl -sS \
        -o /dev/null \
        -w "%{http_code}" \
        --connect-timeout 3 \
        "http://$ip:8081/config" || true)"

    if [ "$CODE" = "200" ]; then
        FLINK_LEADER_IP="$ip"
        break
    fi
done

if [ -z "$FLINK_LEADER_IP" ]; then
    echo "ERROR: Flink Leader not found"
    exit 1
fi

FLINK_LEADER="$FLINK_LEADER_IP:8081"

export FLINK_CFG_REST_ADDRESS="$FLINK_LEADER_IP"
export FLINK_CFG_REST_PORT="8081"
export FLINK_CFG_EXECUTION_ATTACHED="false"

echo "4. 检查是否已经运行"

if /opt/bitnami/flink/bin/flink list \
    -m "$FLINK_LEADER" \
    | grep "ordersdorissink" \
    | grep "(RUNNING)"; then

    echo "Doris Job already running. Skip."
    exit 0
fi

echo "5. 判断启动模式"

if [ -n "$RESTORE_SAVEPOINT" ]; then

    echo "SAVEPOINT RESTORE"

    /opt/bitnami/flink/bin/flink run \
        -d \
        -m "$FLINK_LEADER" \
        -s "$RESTORE_SAVEPOINT" \
        -n \
        -c com.cute.flink.OrdersDorisSink \
        "$WORKDIR/ordersdorissink-1.0.0.jar"

elif [ "$ALLOW_INITIAL" = "true" ]; then

    echo "INITIAL START"

    /opt/bitnami/flink/bin/flink run \
        -d \
        -m "$FLINK_LEADER" \
        -c com.cute.flink.OrdersDorisSink \
        "$WORKDIR/ordersdorissink-1.0.0.jar"

else

    echo "ERROR: No savepoint. Initial start not authorized."
    exit 1

fi

echo "6. 检查 Flink Job"

FOUND=0

for i in 1 2 3 4 5 6; do

    if /opt/bitnami/flink/bin/flink list \
        -m "$FLINK_LEADER" \
        | grep "ordersdorissink" \
        | grep "(RUNNING)"; then

        FOUND=1
        break
    fi

    sleep 5
done

if [ "$FOUND" -ne 1 ]; then
    echo "ERROR: Doris Job not RUNNING"
    exit 1
fi

echo "Doris Job submitted successfully"
""".replace("__JAR_URL__", JAR_URL)
   .replace("__CONNECTOR_URL__", CONNECTOR_URL)
        ],

        on_finish_action="delete_pod",
        get_logs=True,
        startup_timeout_seconds=300,
    )
