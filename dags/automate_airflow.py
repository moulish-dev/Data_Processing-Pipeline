from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime.now,
    "retries": 1,
}

dag = DAG(
    "text_processing_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

# spark job task
spark_task = BashOperator(
    task_id = "run spark job",
    bash_command="spark submit ./text_processor.py",
    dag=dag,
)

spark_task