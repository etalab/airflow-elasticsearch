import pandas as pd
import logging
import requests
from dag_datalake_sirene.helpers import Notification, DataProcessor
from dag_datalake_sirene.workflows.data_pipelines.finess.config import FINESS_CONFIG
from airflow.operators.python import get_current_context


class FinessProcessor(DataProcessor):
    def __init__(self):
        super().__init__(FINESS_CONFIG)

    def preprocess_data(self):
        r = requests.get(self.config.url)
        with open(f"{self.config.tmp_folder}/finess-download.csv", "wb") as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)

        df_finess = pd.read_csv(
            f"{self.config.tmp_folder}/finess-download.csv",
            dtype=str,
            sep=";",
            encoding="Latin-1",
            skiprows=1,
            header=None,
        )
        df_finess = df_finess[[1, 18, 22]]
        df_finess = df_finess.rename(
            columns={1: "finess", 18: "cat_etablissement", 22: "siret"}
        )
        df_finess = df_finess[df_finess["siret"].notna()]
        df_list_finess = (
            df_finess.groupby(["siret"])["finess"]
            .apply(list)
            .reset_index(name="liste_finess")
        )
        df_list_finess = df_list_finess[["siret", "liste_finess"]]
        df_list_finess["liste_finess"] = df_list_finess["liste_finess"].astype(str)
        df_list_finess.to_csv(f"{self.config.tmp_folder}/finess.csv", index=False)

        unique_count = df_list_finess["siret"].nunique()
        ti = get_current_context()["ti"]
        ti.xcom_push(key="nb_siret_finess", value=str(unique_count))

        logging.info(f"Processed {unique_count} unique SIRET values.")

        del df_finess
        del df_list_finess

    def send_file_to_minio(self):
        super().send_file_to_minio()
        ti = get_current_context()["ti"]
        nb_siret = ti.xcom_pull(key="nb_siret_finess", task_ids="process_finess")
        message = f"{nb_siret} établissements"
        ti.xcom_push(key=Notification.notification_xcom_key, value=message)