
from airflow import DAG
from datetime import datetime, timedelta
import configparser
import os

from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "Airflow",
    "start_date": datetime(2025, 12, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=30)
}

# ------------------------------------------------
# Configuration using local conf.conf file
# ------------------------------------------------
config = {}
config_path = os.path.join(os.path.dirname(__file__), 'conf.conf')

# Read key=value format config file
with open(config_path, 'r') as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#'):
            key, value = line.split('=', 1)
            config[key.strip()] = value.strip()

PROJECT_ID = config.get("PROJECT_ID")
BQ_DATASET = config.get("BQ_DATASET")
BUCKET_NAME = config.get("BUCKET_NAME")
IMAGE_NAME = config.get("IMAGE_NAME")
VERSION = config.get("VERSION", "v1")

# ------------------------------------------------
# Callback function for task1
# ------------------------------------------------

def list_files(bucket_name, prefix, processed_prefix="processed/"):
    gcs_hook = GCSHook()
    files = gcs_hook.list(bucket_name, prefix=prefix)

    if files:
        src = files[0]
        filename = src.split("/")[-1]
        dst = processed_prefix + filename

        gcs_hook.copy(bucket_name, src, bucket_name, dst)
        gcs_hook.delete(bucket_name, src)
        return dst
    else:
        raise ValueError("No files found in GCS with given prefix")


with DAG(
    dag_id = "food_orders_dag",
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:


# ------------------------------------------------
# Task 1 : GCS sensor
# ------------------------------------------------

    gcs_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id="gcs_sensor",
        bucket=BUCKET_NAME,
        prefix="food_daily",
        mode='reschedule',  # Changed to reschedule for Composer 3
        poke_interval=60,
        timeout=300,
    )

# ------------------------------------------------
# Task 2 : List files
# ------------------------------------------------

    list_files_task = PythonOperator(
        task_id="list_files",
        python_callable=list_files,
        op_kwargs={
            "bucket_name": f"{BUCKET_NAME}",
            "prefix": "food_daily",
        },
        do_xcom_push=True # for cross communication
    )


# ------------------------------------------------
# Task 3 : DataFlow start flex template
# ------------------------------------------------

    beamtask = DataflowStartFlexTemplateOperator(
        task_id="beamtask",
        project_id = f"{PROJECT_ID}",
        location="us-central1",
        body={
            "launchParameter": {
                "jobName": "food-orders-{{ ds_nodash }}" , # The jinja code is for uniqueness
                "containerSpecGcsPath": (
                    "gs:/f/{BUCKET_NAME}/templates/{IMAGE_NAME}_{VERSION}.json" # Points to Flex Template JSON artifact (Which Docker image to use , Which SDK (Python) , Which parameters are allowed)
                ),
                "parameters": {
                    "input": (
                        "gs:/f/{BUCKET_NAME}/{{ ti.xcom_pull(task_ids='list_files') }}"
                    ),
                    "project_id": f"{PROJECT_ID}",
                    "bq_dataset": f"{BQ_DATASET}"
                },
            }
        }
    )

    gcs_sensor >> list_files_task >> beamtask
