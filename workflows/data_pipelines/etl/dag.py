import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from dag_datalake_sirene.helpers import Notification
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dag_datalake_sirene.helpers.dwh_processor import DataWarehouseProcessor
from dag_datalake_sirene.workflows.data_pipelines.agence_bio.config import (
    AGENCE_BIO_CONFIG,
)

# fmt: off
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.\
    create_etablissements_tables import (
    add_rne_data_to_siege_table,
    count_nombre_etablissement,
    count_nombre_etablissement_ouvert,
    create_etablissement_table,
    create_date_fermeture_etablissement_table,
    create_flux_etablissement_table,
    create_historique_etablissement_table,
    create_siege_table,
    insert_date_fermeture_etablissement,
    replace_etablissement_table,
    replace_siege_table,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.\
    create_additional_data_tables import (
    create_bilan_financier_table,
    create_colter_table,
    create_ess_table,
    create_rge_table,
    create_finess_table,
    create_egapro_table,
    create_elu_table,
    create_organisme_formation_table,
    create_spectacle_table,
    create_uai_table,
    create_convention_collective_table,
    create_marche_inclusion_table,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.\
    create_dirig_benef_tables import (
    create_benef_table,
    create_dirig_pm_table,
    create_dirig_pp_table,
    get_latest_rne_database,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.\
    create_immatriculation_table import (
        create_immatriculation_table,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.\
    create_unite_legale_tables import (
    create_date_fermeture_unite_legale_table,
    create_flux_unite_legale_table,
    create_historique_unite_legale_tables,
    create_unite_legale_table,
    insert_date_fermeture_unite_legale,
    replace_unite_legale_table,
    add_rne_data_to_unite_legale_table,
    add_ancien_siege_flux_data,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.\
    create_json_last_modified import (
    create_data_source_last_modified_file,
)
# fmt: on

from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.upload_db import (
    upload_db_to_minio,
)


from dag_datalake_sirene.config import (
    SIRENE_DATABASE_LOCATION,
    AIRFLOW_ETL_DAG_NAME,
    AIRFLOW_ELK_DAG_NAME,
    EMAIL_LIST,
)


default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id=AIRFLOW_ETL_DAG_NAME,
    tags=["database", "all-data"],
    default_args=default_args,
    schedule_interval="0 10 * * *",  # Run everyday at 10 am UTC
    start_date=datetime(2023, 12, 27),
    dagrun_timeout=timedelta(minutes=60 * 5),
    params={},
    catchup=False,  # False to ignore past runs
    on_failure_callback=Notification.send_notification_tchap,
    on_success_callback=Notification.send_notification_tchap,
    max_active_runs=1,
)
def datawarehouse_creation():
    @task.bash
    def clean_previous_tmp_folder() -> str:
        db_folder_path = os.path.dirname(SIRENE_DATABASE_LOCATION)
        return f"rm -rf {db_folder_path} && mkdir -p {db_folder_path}"

    create_unite_legale_table_task = PythonOperator(
        task_id="create_unite_legale_table",
        provide_context=True,
        python_callable=create_unite_legale_table,
    )

    create_etablissement_table_task = PythonOperator(
        task_id="create_etablissement_table",
        provide_context=True,
        python_callable=create_etablissement_table,
    )

    create_flux_unite_legale_table_task = PythonOperator(
        task_id="create_flux_unite_legale_table",
        provide_context=True,
        python_callable=create_flux_unite_legale_table,
    )

    add_ancien_siege_flux_data_task = PythonOperator(
        task_id="add_ancien_siege_flux_data",
        provide_context=True,
        python_callable=add_ancien_siege_flux_data,
    )

    create_flux_etablissement_table_task = PythonOperator(
        task_id="create_flux_etablissement_table",
        provide_context=True,
        python_callable=create_flux_etablissement_table,
        trigger_rule="all_done",
    )

    replace_unite_legale_table_task = PythonOperator(
        task_id="replace_unite_legale_table",
        provide_context=True,
        python_callable=replace_unite_legale_table,
        trigger_rule="all_done",
    )

    replace_etablissement_table_task = PythonOperator(
        task_id="replace_etablissement_table",
        provide_context=True,
        python_callable=replace_etablissement_table,
    )

    count_nombre_etablissement_task = PythonOperator(
        task_id="count_nombre_etablissement",
        provide_context=True,
        python_callable=count_nombre_etablissement,
    )

    count_nombre_etablissement_ouvert_task = PythonOperator(
        task_id="count_nombre_etablissement_ouvert",
        provide_context=True,
        python_callable=count_nombre_etablissement_ouvert,
    )

    create_historique_unite_legale_table_task = PythonOperator(
        task_id="create_historique_unite_legale_table",
        provide_context=True,
        python_callable=create_historique_unite_legale_tables,
    )

    create_date_fermeture_unite_legale_table_task = PythonOperator(
        task_id="create_date_fermeture_unite_legale_table",
        provide_context=True,
        python_callable=create_date_fermeture_unite_legale_table,
    )

    insert_date_fermeture_unite_legale_task = PythonOperator(
        task_id="insert_date_fermeture_unite_legale",
        provide_context=True,
        python_callable=insert_date_fermeture_unite_legale,
    )

    inject_rne_unite_legale_data_task = PythonOperator(
        task_id="add_rne_siren_data_to_unite_legale_table",
        provide_context=True,
        python_callable=add_rne_data_to_unite_legale_table,
    )

    create_siege_table_task = PythonOperator(
        task_id="create_siege_table",
        provide_context=True,
        python_callable=create_siege_table,
    )

    replace_siege_table_task = PythonOperator(
        task_id="replace_siege_table",
        provide_context=True,
        python_callable=replace_siege_table,
    )

    inject_rne_siege_data_task = PythonOperator(
        task_id="add_rne_data_to_siege_table",
        provide_context=True,
        python_callable=add_rne_data_to_siege_table,
    )

    create_historique_etablissement_table_task = PythonOperator(
        task_id="create_historique_etablissement_table",
        provide_context=True,
        python_callable=create_historique_etablissement_table,
        trigger_rule="all_done",
    )

    create_date_fermeture_etablissement_table_task = PythonOperator(
        task_id="create_date_fermeture_etablissement_table",
        provide_context=True,
        python_callable=create_date_fermeture_etablissement_table,
    )

    insert_date_fermeture_etablissement_task = PythonOperator(
        task_id="insert_date_fermeture_etablissement",
        provide_context=True,
        python_callable=insert_date_fermeture_etablissement,
    )

    get_latest_rne_database_task = PythonOperator(
        task_id="get_rne_database",
        provide_context=True,
        python_callable=get_latest_rne_database,
    )

    create_dirig_pp_table_task = PythonOperator(
        task_id="create_dirig_pp_table",
        provide_context=True,
        python_callable=create_dirig_pp_table,
    )

    create_dirig_pm_table_task = PythonOperator(
        task_id="create_dirig_pm_table",
        provide_context=True,
        python_callable=create_dirig_pm_table,
    )

    create_benef_table_task = PythonOperator(
        task_id="create_benef_table",
        provide_context=True,
        python_callable=create_benef_table,
    )

    create_immatriculation_table_task = PythonOperator(
        task_id="copy_immatriculation_table",
        provide_context=True,
        python_callable=create_immatriculation_table,
    )

    create_bilan_financier_table_task = PythonOperator(
        task_id="create_bilan_financier_table",
        provide_context=True,
        python_callable=create_bilan_financier_table,
    )

    create_convention_collective_table_task = PythonOperator(
        task_id="create_convention_collective_table",
        provide_context=True,
        python_callable=create_convention_collective_table,
    )

    create_ess_table_task = PythonOperator(
        task_id="create_ess_table",
        provide_context=True,
        python_callable=create_ess_table,
    )

    create_rge_table_task = PythonOperator(
        task_id="create_rge_table",
        provide_context=True,
        python_callable=create_rge_table,
    )

    create_finess_table_task = PythonOperator(
        task_id="create_finess_table",
        provide_context=True,
        python_callable=create_finess_table,
    )

    processor_list = [
        DataWarehouseProcessor(AGENCE_BIO_CONFIG),
    ]
    tasks = []
    for processor in processor_list:

        @task(task_id=f"create_{processor.config.name}_table")
        def create_table(**kwargs):
            processor.etl_create_table(SIRENE_DATABASE_LOCATION)

        task_instance = create_table()
        tasks.append(task_instance)

    create_finess_table_task >> tasks[0]
    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]

    create_organisme_formation_table_task = PythonOperator(
        task_id="create_organisme_formation_table",
        provide_context=True,
        python_callable=create_organisme_formation_table,
    )

    tasks[-1] >> create_organisme_formation_table_task

    create_uai_table_task = PythonOperator(
        task_id="create_uai_table",
        provide_context=True,
        python_callable=create_uai_table,
    )

    create_spectacle_table_task = PythonOperator(
        task_id="create_spectacle_table",
        provide_context=True,
        python_callable=create_spectacle_table,
    )

    create_egapro_table_task = PythonOperator(
        task_id="create_egapro_table",
        provide_context=True,
        python_callable=create_egapro_table,
    )

    create_elu_table_task = PythonOperator(
        task_id="create_elu_table",
        provide_context=True,
        python_callable=create_elu_table,
    )

    create_colter_table_task = PythonOperator(
        task_id="create_colter_table",
        provide_context=True,
        python_callable=create_colter_table,
    )

    create_marche_inclusion_table_task = PythonOperator(
        task_id="create_marche_inclusion_table",
        provide_context=True,
        python_callable=create_marche_inclusion_table,
    )

    send_database_to_minio_task = PythonOperator(
        task_id="upload_db_to_minio",
        provide_context=True,
        python_callable=upload_db_to_minio,
    )

    create_data_source_last_modified_file_task = PythonOperator(
        task_id="create_data_source_last_modified_file",
        provide_context=True,
        python_callable=create_data_source_last_modified_file,
    )

    @task.bash
    def clean_current_tmp_folder() -> str:
        db_folder_path = os.path.dirname(SIRENE_DATABASE_LOCATION)
        return f"rm -rf {db_folder_path} && mkdir -p {db_folder_path}"

    trigger_indexing_dag = TriggerDagRunOperator(
        task_id="trigger_indexing_dag",
        trigger_dag_id=AIRFLOW_ELK_DAG_NAME,
        wait_for_completion=False,
        deferrable=False,
    )

    clean_previous_tmp_folder() >> create_unite_legale_table_task

    create_historique_unite_legale_table_task.set_upstream(
        create_unite_legale_table_task
    )
    create_date_fermeture_unite_legale_table_task.set_upstream(
        create_historique_unite_legale_table_task
    )
    create_etablissement_table_task.set_upstream(
        create_date_fermeture_unite_legale_table_task
    )
    create_flux_unite_legale_table_task.set_upstream(create_etablissement_table_task)
    create_flux_etablissement_table_task.set_upstream(
        create_flux_unite_legale_table_task
    )
    replace_unite_legale_table_task.set_upstream(create_flux_etablissement_table_task)
    insert_date_fermeture_unite_legale_task.set_upstream(
        replace_unite_legale_table_task
    )
    replace_etablissement_table_task.set_upstream(
        insert_date_fermeture_unite_legale_task
    )
    count_nombre_etablissement_task.set_upstream(replace_etablissement_table_task)
    count_nombre_etablissement_ouvert_task.set_upstream(count_nombre_etablissement_task)
    create_siege_table_task.set_upstream(count_nombre_etablissement_ouvert_task)
    replace_siege_table_task.set_upstream(create_siege_table_task)
    add_ancien_siege_flux_data_task.set_upstream(replace_siege_table_task)
    create_historique_etablissement_table_task.set_upstream(
        add_ancien_siege_flux_data_task
    )
    create_date_fermeture_etablissement_table_task.set_upstream(
        create_historique_etablissement_table_task
    )
    insert_date_fermeture_etablissement_task.set_upstream(
        create_date_fermeture_etablissement_table_task
    )

    get_latest_rne_database_task.set_upstream(insert_date_fermeture_etablissement_task)
    inject_rne_unite_legale_data_task.set_upstream(get_latest_rne_database_task)
    inject_rne_siege_data_task.set_upstream(inject_rne_unite_legale_data_task)
    create_dirig_pp_table_task.set_upstream(inject_rne_siege_data_task)
    create_dirig_pm_table_task.set_upstream(create_dirig_pp_table_task)
    create_benef_table_task.set_upstream(create_dirig_pm_table_task)
    create_immatriculation_table_task.set_upstream(create_benef_table_task)

    create_bilan_financier_table_task.set_upstream(create_immatriculation_table_task)
    create_convention_collective_table_task.set_upstream(
        create_bilan_financier_table_task
    )
    create_ess_table_task.set_upstream(create_convention_collective_table_task)
    create_rge_table_task.set_upstream(create_ess_table_task)
    create_finess_table_task.set_upstream(create_rge_table_task)
    create_uai_table_task.set_upstream(create_organisme_formation_table_task)
    create_spectacle_table_task.set_upstream(create_uai_table_task)
    create_egapro_table_task.set_upstream(create_spectacle_table_task)
    create_colter_table_task.set_upstream(create_egapro_table_task)
    create_elu_table_task.set_upstream(create_colter_table_task)
    create_marche_inclusion_table_task.set_upstream(create_elu_table_task)

    send_database_to_minio_task.set_upstream(create_marche_inclusion_table_task)
    create_data_source_last_modified_file_task.set_upstream(send_database_to_minio_task)

    (
        create_data_source_last_modified_file_task
        >> clean_current_tmp_folder()
        >> trigger_indexing_dag
    )


datawarehouse_creation()