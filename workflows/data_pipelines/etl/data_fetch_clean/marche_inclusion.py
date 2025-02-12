import pandas as pd
from dag_datalake_sirene.workflows.data_pipelines.marche_inclusion.config import (
    MARCHE_INCLUSION_CONFIG,
)


def preprocess_marche_inclusion_data(data_dir):
    df_siae = pd.read_csv(
        MARCHE_INCLUSION_CONFIG.url_minio,
        dtype=str,
    )
    df_siae["siren"] = df_siae["siret"].str[0:9]

    df_siae_grouped = (
        df_siae.groupby("siren")["kind"].agg(lambda x: list(set(x))).reset_index()
    )
    df_siae_grouped.rename(columns={"kind": "type_siae"}, inplace=True)
    df_siae_grouped["type_siae"] = df_siae_grouped["type_siae"].astype(str)

    df_siae_grouped["est_siae"] = True

    del df_siae

    return df_siae_grouped
