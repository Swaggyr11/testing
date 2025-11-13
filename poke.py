from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kubernetes import client, config
import time
import json

# ------------------------
# DAG Default Config
# ------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 12),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

# ------------------------
# DAG Definition
# ------------------------
with DAG(
    dag_id="spark_stateful_pod_monitor_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Run Spark job on EKS, capture driver pod name, and monitor it statefully.",
) as dag:

    # -------------------------------------------------
    # Task 1: Submit Spark job and capture driver pod name
    # -------------------------------------------------
    spark_submit = KubernetesPodOperator(
        task_id="spark_submit_task",
        name="spark-job-launcher",
        namespace="default",
        image="123456789012.dkr.ecr.us-east-1.amazonaws.com/spark:latest",  # replace with your Spark image
        cmds=["/bin/bash", "-c"],
        arguments=[
            """
            set -e
            echo ">>> Launching Spark job on EKS..."

            # Run spark-submit and capture logs
            spark-submit \
                --master k8s://https://your-eks-endpoint \
                --deploy-mode cluster \
                --class com.example.MyJob \
                --conf spark.kubernetes.container.image=123456789012.dkr.ecr.us-east-1.amazonaws.com/spark:latest \
                s3a://your-bucket/jars/MyJob-assembly.jar arg1 arg2 > spark_log.txt 2>&1

            echo ">>> Spark job submitted, capturing driver pod name..."
            DRIVER_POD=$(grep 'Starting Kubernetes application:' spark_log.txt | awk '{print $4}')

            if [ -z "$DRIVER_POD" ]; then
                echo "❌ Could not find driver pod name in Spark logs!"
                DRIVER_POD="UNKNOWN"
            fi

            echo "✅ Driver pod detected: $DRIVER_POD"

            # Write to the special XCom JSON file so Airflow can capture it
            echo "{\\"pod_name\\": \\"$DRIVER_POD\\"}" > /airflow/xcom/return.json
            """
        ],
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=True,
    )

    # -------------------------------------------------
    # Task 2: Monitor the Spark driver pod status (stateful)
    # -------------------------------------------------
    def monitor_spark_pod(**context):
        """Monitor Spark driver pod until success or failure using poke-style interval."""
        pod_info = context['ti'].xcom_pull(task_ids='spark_submit_task')
        pod_name = pod_info.get('pod_name') if isinstance(pod_info, dict) else None
        namespace = "default"

        if not pod_name or pod_name == "UNKNOWN":
            raise Exception("❌ No valid pod name found in XCom. Cannot monitor Spark job.")

        print(f"🕵️ Monitoring Spark driver pod: {pod_name} in namespace '{namespace}'")

        # Load Kubernetes config (MWAA or in-cluster)
        try:
            config.load_incluster_config()
        except Exception:
            config.load_kube_config()

        v1 = client.CoreV1Api()

        poke_interval = 30    # seconds between checks
        timeout = 60 * 60     # 1 hour timeout
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                pod = v1.read_namespaced_pod_status(pod_name, namespace)
                phase = pod.status.phase
                print(f"⏱️ Pod '{pod_name}' current phase: {phase}")

                if phase == "Succeeded":
                    print("✅ Spark driver pod succeeded.")
                    return
                elif phase == "Failed":
                    raise Exception(f"❌ Spark driver pod failed (Pod: {pod_name}).")
                elif phase == "Unknown":
                    print("⚠️ Pod status unknown, retrying...")
                else:
                    print(f"Pod still {phase}... waiting {poke_interval}s.")
            except client.exceptions.ApiException as e:
                if e.status == 404:
                    print(f"⚠️ Pod {pod_name} not found yet, will retry...")
                else:
                    print(f"❌ Kubernetes API error: {e}")
            time.sleep(poke_interval)

        raise TimeoutError(f"⏰ Timeout: Pod '{pod_name}' did not complete within {timeout/60} minutes.")

    spark_monitor = PythonOperator(
        task_id="monitor_spark_pod_status",
        python_callable=monitor_spark_pod,
        provide_context=True,
    )

    # -------------------------------------------------
    # Task 3: Downstream (runs only if Spark succeeded)
    # -------------------------------------------------
    def downstream_task():
        print("🎉 Spark job completed successfully — continuing pipeline.")

    post_spark = PythonOperator(
        task_id="downstream_success_task",
        python_callable=downstream_task,
    )

    # -------------------------------------------------
    # DAG Flow
    # -------------------------------------------------
    spark_submit >> spark_monitor >> post_spark
