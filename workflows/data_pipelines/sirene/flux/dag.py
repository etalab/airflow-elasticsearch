from datetime import timedelta

from airflow.utils.dates import days_ago

from airflow.decorators import dag, task
from dag_datalake_sirene.config import EMAIL_LIST
from dag_datalake_sirene.helpers import Notification
from dag_datalake_sirene.workflows.data_pipelines.sirene.flux.processor import (
    SireneFluxProcessor,
)


default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}


@dag(
    tags=["sirene", "flux"],
    default_args=default_args,
    schedule_interval="0 4 2-31 * *",  # Daily at 4 AM except the 1st of every month
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60 * 5),
    params={},
    catchup=False,
    max_active_runs=1,
    on_failure_callback=Notification.send_notification_tchap,
    on_success_callback=Notification.send_notification_tchap,
)
def data_processing_sirene_flux():
    sirene_flux_processor = SireneFluxProcessor()

    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {sirene_flux_processor.config.tmp_folder} && mkdir -p {sirene_flux_processor.config.tmp_folder}"

    @task
    def get_flux_unites_legales():
        return sirene_flux_processor.get_current_flux_unite_legale()

    @task
    def get_flux_etablissements():
        return sirene_flux_processor.get_current_flux_etablissement()

    @task
    def save_date_last_modified():
        return sirene_flux_processor.save_date_last_modified()

    @task
    def send_flux_to_minio():
        return sirene_flux_processor.send_flux_to_minio()

    (
        clean_previous_outputs()
        >> get_flux_unites_legales()
        >> get_flux_etablissements()
        >> save_date_last_modified()
        >> send_flux_to_minio()
    )


# Instantiate the DAG
data_processing_sirene_flux()