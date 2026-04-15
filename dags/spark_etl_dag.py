from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    "owner": "sir",
    "start_date": datetime(2024, 1, 1)
}

with DAG(
    "spark_etl_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    run_spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command="spark-submit --master spark://spark-master:7077 --jars /opt/spark/jars/mysql-connector-j-8.0.33.jar /opt/jobs/job.py"
    )