from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="quantexa_transaction_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   
    catchup=False,
    tags=["quantexa", "etl", "spark"],
) as dag:

   
    setup_env = BashOperator(
        task_id="setup_environment",
        bash_command="bash /path/to/source.sh"
    )

    generate_metadata = BashOperator(
        task_id="generate_metadata_row",
        bash_command="bash /path/to/runQSS.sh -s com.quantexa.project.citi.transaction.etl.GenerateMetadataRow -c /path/to/external.conf -r etl.transaction -l qssMetrics"
    )

    create_case_class = BashOperator(
        task_id="create_case_class",
        bash_command="bash /path/to/runQSS.sh -s com.quantexa.project.transaction.etl.CreateCaseClass -c /path/to/external.conf -r etl.transaction -l qssMetrics"
    )

    consolidate_transaction = BashOperator(
        task_id="consolidate_transaction",
        bash_command="bash /path/to/runQSS.sh -s com.quantexa.project.transaction.etl.ConsolidateTransactionScriptLegacy -c /path/to/external.conf -r etl.transaction -l qssMetrics"
    )


    spark_job = SparkSubmitOperator(
        task_id="spark_transaction_job",
        application="/data/amlradm/bin/spark35/spark-3.5.1-bin-without-hadoop/bin/spark-submit",
        application_args=[
            "--class", "com.quantexa.scriptrunner.QuantexaSparkScriptRunner",
            "--master", "yarn",
            "--deploy-mode", "cluster",
            "--executor-memory", "20g",
            "--num-executors", "200",
            "--executor-cores", "8"
        ],
        conf={
            "spark.sql.shuffle.partitions": "12001",
            "spark.sql.adaptive.enabled": "true"
        },
        jars="/path/to/data-source-all-shadow-2.6.9-projects.jar",
    )

    setup_env >> generate_metadata >> create_case_class >> consolidate_transaction >> spark_job
