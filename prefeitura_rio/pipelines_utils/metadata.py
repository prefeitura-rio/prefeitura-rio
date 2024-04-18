# -*- coding: utf-8 -*-
import base64
import json
import os
import re
import time
from typing import Dict, List

import gspread
import pandas as pd
import requests
from google.oauth2 import service_account


class MetadataAPI:
    def __init__(self, base_url: str = None, token: str = None) -> None:
        self.BASE_URL = base_url or "https://meta.dados.rio/api"
        self.access_token = token
        self.headers = {"Authorization": f"Token {self.access_token}"}

    def _get(self, path: str, timeout: int = 120):
        try:
            response = requests.get(f"{self.BASE_URL}{path}", headers=self.headers, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"GET request failed: {e}")
            return None

    def _put(self, path, json_data=None):
        try:
            response = requests.put(f"{self.BASE_URL}{path}", headers=self.headers, json=json_data)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"PUT request failed: {e}")
            return None


def inject_credential(
    credential_path="~/.basedosdados/credentials/prod.json",
):
    with open(credential_path) as f:
        cred = json.load(f)
    os.environ["GCP_SERVICE_ACCOUNT"] = base64.b64encode(
        str(cred).replace("'", '"').encode("utf-8")
    ).decode("utf-8")


def get_credentials_from_env(key: str, scopes: List[str] = None) -> service_account.Credentials:
    """
    Gets credentials from env vars
    """
    env: str = os.getenv(key, "")
    if env == "":
        raise ValueError(f'Enviroment variable "{key}" not set!')
    info: dict = json.loads(base64.b64decode(env))
    cred: service_account.Credentials = service_account.Credentials.from_service_account_info(info)
    if scopes:
        cred = cred.with_scopes(scopes)
    return cred


def final_column_treatment(column: str) -> str:
    """
    Adds an underline before column name if it only has numbers or remove all non alpha numeric
    characters besides underlines ("_").
    """
    try:
        int(column)
        return f"_{column}"
    except ValueError:  # pylint: disable=bare-except
        non_alpha_removed = re.sub(r"[\W]+", "", column)
        return non_alpha_removed


def remove_columns_accents(dataframe: pd.DataFrame) -> list:
    """
    Remove accents from dataframe columns.
    """
    columns = [str(column) for column in dataframe.columns]
    dataframe.columns = columns
    return list(
        dataframe.columns.str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .map(lambda x: x.strip())
        .str.replace(" ", "_")
        .str.replace("/", "_")
        .str.replace("-", "_")
        .str.replace("\a", "_")
        .str.replace("\b", "_")
        .str.replace("\n", "_")
        .str.replace("\t", "_")
        .str.replace("\v", "_")
        .str.replace("\f", "_")
        .str.replace("\r", "_")
        .str.lower()
        .map(final_column_treatment)
    )


def get_gspread_sheet(
    sheet_url: str, google_sheet_credential_env_name: str = "GCP_SERVICE_ACCOUNT"
) -> gspread.Client:

    url_prefix = "https://docs.google.com/spreadsheets/d/"
    if not sheet_url.startswith(url_prefix):
        raise ValueError(
            "URL must start with https://docs.google.com/spreadsheets/d/"
            f"Invalid URL: {sheet_url}"
        )
    credentials = get_credentials_from_env(
        key=google_sheet_credential_env_name,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    gspread_client = gspread.authorize(credentials)
    sheet = gspread_client.open_by_url(sheet_url)
    time.sleep(1.5)
    return sheet


def sheet_to_dataframe(
    worksheet: str,
):
    dataframe = pd.DataFrame(worksheet.get_values())
    new_header = dataframe.iloc[0]  # grab the first row for the header
    dataframe = dataframe[1:]  # take the data less the header row
    dataframe.columns = new_header  # set the header row as the df header
    return dataframe


def get_metadados_rio_dataframe(
    dataset_id: str = "sancao_fornecedor", api_token: str = None
) -> pd.DataFrame:

    m = MetadataAPI(token=api_token)
    r = m._get(path=f"/datasets/?name={dataset_id}")
    df_metadata = pd.DataFrame.from_records(
        r["results"][0]["tables"], columns=["url", "name", "columns"]
    )
    df_columns = pd.json_normalize(df_metadata.explode("columns")["columns"])
    df_columns = df_columns.add_prefix("column_").rename(columns={"column_table": "url"})
    df_metadata = df_metadata.drop(columns=["columns"])
    df_meta_dados_rio = df_metadata.merge(df_columns, on="url", how="outer")
    return df_meta_dados_rio


def get_metadados_from_sheets_dataframe(
    sheets_url: str = "https://docs.google.com/spreadsheets/d/1yycdMqkNMroLx9dA8kKawyYems7ygvt9yFooD_1ODBk",  # noqa
    ignore_sheets_tables: List[str] = ["sancao_fornecedor"],
    sheets_columns_names: Dict = {
        "nome_original_da_coluna": "column_original_name",
        "nome_da_coluna": "nome_da_coluna",
        "tipo_da_coluna": "tipo_da_coluna",
        "descricao_da_coluna": "descricao_da_coluna",
    },
):
    sheets = get_gspread_sheet(sheet_url=sheets_url)

    final_df = pd.DataFrame()
    for sheet in sheets.worksheets():
        if sheet.title in ["Descrição do Conjunto"] + ignore_sheets_tables:
            continue
        dataframe = sheet_to_dataframe(sheet)
        dataframe.columns = remove_columns_accents(dataframe)
        dataframe["nome_original_da_coluna"] = remove_columns_accents(
            pd.DataFrame(columns=dataframe["nome_original_da_coluna"].tolist())
        )
        dataframe["table_id"] = sheet.title
        cols = [
            "table_id",
        ] + list(sheets_columns_names.values())

        dataframe = dataframe[cols]
        final_df = pd.concat([final_df, dataframe])

    df_sheets = final_df[final_df["nome_da_coluna"] != ""]
    return df_sheets.rename(columns=sheets_columns_names)


def get_metadados_from_sheets_and_api(
    dataset_id: str = "educacao_basica_frequencia",
    api_token: str = None,
    credential_path: str = "/home/jovyan/.basedosdados/credentials/prod.json",
    sheets_url: str = "https://docs.google.com/spreadsheets/d/1qKT8gXsQ63wqVWcEG5O-1UnZqmT-Bz_CNSVaPRIHmQE",  # noqa
    ignore_sheets_tables: List[str] = [],
    sheets_columns_names: Dict = {
        "nome_original_da_coluna": "column_original_name",
        "nome_da_coluna": "nome_da_coluna",
        "tipo_da_coluna_em_bigquery": "tipo_da_coluna_em_bigquery",
        "descricao_da_coluna": "descricao_da_coluna",
    },
):

    inject_credential(credential_path=credential_path)
    df_meta_dados_rio = get_metadados_rio_dataframe(dataset_id=dataset_id, api_token=api_token)
    df_sheets = get_metadados_from_sheets_dataframe(
        sheets_url=sheets_url,
        ignore_sheets_tables=ignore_sheets_tables,
        sheets_columns_names=sheets_columns_names,
    )
    final_df = df_sheets.merge(
        df_meta_dados_rio, on=["table_id", "column_original_name"], how="outer"
    )

    return final_df, df_sheets, df_meta_dados_rio
