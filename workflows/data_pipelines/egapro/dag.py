from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
from dag_datalake_sirene.config import EMAIL_LIST
from dag_datalake_sirene.helpers import Notification
from dag_datalake_sirene.workflows.data_pipelines.egapro.processor import (
    EgaproProcessor,
)
from dag_datalake_sirene.workflows.data_pipelines.egapro.config import EGAPRO_CONFIG

egapro_processor = EgaproProcessor()

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}


@dag(
    tags=["egapro"],
    default_args=default_args,
    schedule_interval="0 16 * * *",
    start_date=days_ago(8),
    dagrun_timeout=timedelta(minutes=60),
    params={},
    catchup=False,
    on_failure_callback=Notification.send_notification_tchap,
    on_success_callback=Notification.send_notification_tchap,
)
def data_processing_egapro():
    @task.bash
    def clean_previous_outputs():
        return (
            f"rm -rf {EGAPRO_CONFIG.tmp_folder} && mkdir -p {EGAPRO_CONFIG.tmp_folder}"
        )

    @task
    def preprocess_egapro():
        return egapro_processor.preprocess_data()

    @task
    def save_date_last_modified():
        return egapro_processor.save_date_last_modified()

    @task
    def send_file_to_minio():
        return egapro_processor.send_file_to_minio()

    @task
    def compare_files_minio():
        return egapro_processor.compare_files_minio()

    (
        clean_previous_outputs()
        >> preprocess_egapro()
        >> save_date_last_modified()
        >> send_file_to_minio()
        >> compare_files_minio()
    )


# Instantiate the DAG
data_processing_egapro()