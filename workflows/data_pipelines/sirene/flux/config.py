from airflow.models import Variable
from dag_datalake_sirene.config import (
    DataSourceConfig,
    MINIO_BASE_URL,
)


FLUX_SIRENE_CONFIG = DataSourceConfig(
    name="flux_api_sirene",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/sirene/flux/",
    minio_path="insee/flux/",
    url_minio=f"{MINIO_BASE_URL}insee/flux/latest/",
    url_minio_metadata=f"{MINIO_BASE_URL}insee/flux/latest/metadata.json",
    url_api="https://api.insee.fr/api-sirene/3.11/",
    auth_api=Variable.get("SECRET_BEARER_INSEE", None),
)
