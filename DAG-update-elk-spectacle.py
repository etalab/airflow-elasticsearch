from datetime import timedelta

from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from dag_datalake_sirene.external_data.task_functions import (
    compare_versions_file,
    preprocess_spectacle_data,
    publish_mattermost,
    update_es,
)
from dag_datalake_sirene.task_functions import (
    get_colors,
    get_object_minio,
    put_object_minio,
)
from operators.clean_folder import CleanFolderOperator

DAG_FOLDER = "dag_datalake_sirene/"
DAG_NAME = "update-elk-entrepreneur-spectacle"
TMP_FOLDER = "/tmp/"
EMAIL_LIST = Variable.get("EMAIL_LIST")
ENV = Variable.get("ENV")
PATH_AIO = Variable.get("PATH_AIO")

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval="0 23 10 * *",
    start_date=days_ago(10),
    dagrun_timeout=timedelta(minutes=60 * 8),
    tags=["spectacle"],
) as dag:
    get_colors = PythonOperator(
        task_id="get_colors",
        provide_context=True,
        python_callable=get_colors,
    )

    clean_previous_folder = CleanFolderOperator(
        task_id="clean_previous_folder",
        folder_path=f"{TMP_FOLDER}+{DAG_FOLDER}+{DAG_NAME}",
    )

    preprocess_spectacle_data = PythonOperator(
        task_id="preprocess_spectacle_data",
        python_callable=preprocess_spectacle_data,
        op_args=(TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",),
    )

    get_latest_spectacle_data = PythonOperator(
        task_id="get_latest_spectacle_data",
        python_callable=get_object_minio,
        op_args=(
            "spectacle-latest.csv",
            "ae/external_data/spectacle/",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/spectacle-latest.csv",
        ),
    )

    compare_versions_file = ShortCircuitOperator(
        task_id="compare_versions_file",
        python_callable=compare_versions_file,
        op_args=(
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/spectacle-latest.csv",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/spectacle-new.csv",
        ),
    )

    update_es = PythonOperator(
        task_id="update_es",
        python_callable=update_es,
        op_args=(
            "spectacle",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/spectacle-new.csv",
            "spectacle-errors.txt",
            "current",
        ),
    )

    put_file_error_to_minio = PythonOperator(
        task_id="put_file_error_to_minio",
        python_callable=put_object_minio,
        op_args=(
            "spectacle-errors.txt",
            "ae/external_data/spectacle/spectacle-errors.txt",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",
        ),
    )

    put_file_latest_to_minio = PythonOperator(
        task_id="put_file_latest_to_minio",
        python_callable=put_object_minio,
        op_args=(
            "spectacle-new.csv",
            "ae/external_data/spectacle/spectacle-latest.csv",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",
        ),
    )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost,
        op_args=(
            "Infos Annuaire : Données des entrepreneurs du spectacle vivant"
            " mises à jour sur l'API !",
        ),
    )

    clean_previous_folder.set_upstream(get_colors)
    preprocess_spectacle_data.set_upstream(clean_previous_folder)
    get_latest_spectacle_data.set_upstream(preprocess_spectacle_data)
    compare_versions_file.set_upstream(get_latest_spectacle_data)
    update_es.set_upstream(compare_versions_file)
    put_file_error_to_minio.set_upstream(update_es)
    put_file_latest_to_minio.set_upstream(update_es)
    publish_mattermost.set_upstream(put_file_error_to_minio)
    publish_mattermost.set_upstream(put_file_latest_to_minio)