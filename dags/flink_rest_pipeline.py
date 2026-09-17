import json
import os
import time
import urllib.error
import urllib.request

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


FLINK_HOST = "flink-jobmanager"
FLINK_PORT = 8081
JAR_PATH = "/opt/airflow/dags/repo/jars/WordCount.jar"
JAR_NAME = "WordCount.jar"


def flink_request(method, path, body=None):
    url = f"http://{FLINK_HOST}:{FLINK_PORT}{path}"

    data = None
    headers = {}

    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(
        url,
        data=data,
        headers=headers,
        method=method,
    )

    with urllib.request.urlopen(request, timeout=30) as response:
        raw = response.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def upload_jar():
    if not os.path.exists(JAR_PATH):
        raise FileNotFoundError(
            f"{JAR_PATH} does not exist in Airflow Worker."
        )

    boundary = "----AirflowFlinkBoundary"

    with open(JAR_PATH, "rb") as f:
        jar_data = f.read()

    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="jarfile"; '
        f'filename="{JAR_NAME}"\r\n'
        f"Content-Type: application/java-archive\r\n"
        f"\r\n"
    ).encode()

    body += jar_data
    body += f"\r\n--{boundary}--\r\n".encode()

    request = urllib.request.Request(
        f"http://{FLINK_HOST}:{FLINK_PORT}/jars/upload",
        data=body,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}"
        },
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=60) as response:
        result = json.loads(response.read().decode("utf-8"))

    print("Upload result:", result)

    if result.get("status") != "success":
        raise RuntimeError(f"Flink JAR upload failed: {result}")

    return result["filename"]


def submit_flink_job(**context):
    filename = context["ti"].xcom_pull(task_ids="upload_jar")

    jar_id = filename.rsplit("/", 1)[-1]

    print("JAR ID:", jar_id)

    submit_time = int(time.time() * 1000)

    try:
        result = flink_request(
            "POST",
            f"/jars/{jar_id}/run",
            {},
        )

        print("Flink submit response:", result)

    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")

        print("Flink REST returned an error:")
        print(error_body)

        # WordCount.jar 官方示例调用 print()。
        # REST detached 模式可能返回错误，但 Job 实际已经提交并完成。
        if "Job was submitted in detached mode" not in error_body:
            raise

        print(
            "Detected detached-mode print() response. "
            "Will verify the actual Flink Job state."
        )

    context["ti"].xcom_push(
        key="submit_time",
        value=submit_time,
    )


def wait_for_flink_job(**context):
    submit_time = context["ti"].xcom_pull(
        task_ids="submit_flink_job",
        key="submit_time",
    )

    if not submit_time:
        raise RuntimeError("submit_time was not found.")

    deadline = time.time() + 180

    while time.time() < deadline:
        result = flink_request("GET", "/jobs/overview")

        jobs = result.get("jobs", [])

        recent_jobs = [
            job
            for job in jobs
            if job.get("start-time", 0) >= submit_time - 5000
        ]

        if recent_jobs:
            recent_jobs.sort(
                key=lambda job: job.get("start-time", 0),
                reverse=True,
            )

            job = recent_jobs[0]

            job_id = job.get("jid")
            state = job.get("state")

            print(f"Job ID: {job_id}")
            print(f"Job state: {state}")

            if state == "FINISHED":
                print("Flink Job finished successfully.")
                return

            if state in {"FAILED", "CANCELED"}:
                raise RuntimeError(
                    f"Flink Job ended with state={state}: {job}"
                )

        else:
            print("Waiting for Flink Job to appear...")

        time.sleep(5)

    raise TimeoutError(
        "Flink Job did not reach FINISHED within 180 seconds."
    )


with DAG(
    dag_id="flink_rest_pipeline",
    start_date=datetime(2026, 9, 18),
    schedule=None,
    catchup=False,
    tags=["bigdata", "flink", "rest"],
) as dag:

    upload = PythonOperator(
        task_id="upload_jar",
        python_callable=upload_jar,
    )

    submit = PythonOperator(
        task_id="submit_flink_job",
        python_callable=submit_flink_job,
    )

    wait = PythonOperator(
        task_id="wait_for_flink_job",
        python_callable=wait_for_flink_job,
    )

    upload >> submit >> wait
