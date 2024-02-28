# -*- coding: utf-8 -*-
import base64
import json
import os
import time
from os import getenv
from typing import List

import gspread
import pandas as pd
from google.oauth2 import service_account


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
    env: str = getenv(key, "")
    if env == "":
        raise ValueError(f'Enviroment variable "{key}" not set!')
    info: dict = json.loads(base64.b64decode(env))
    cred: service_account.Credentials = service_account.Credentials.from_service_account_info(info)
    if scopes:
        cred = cred.with_scopes(scopes)
    return cred


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
    worksheet: gspread.worksheet.Worksheet,
):
    dataframe = pd.DataFrame(worksheet.get_values())
    new_header = dataframe.iloc[0]  # grab the first row for the header
    dataframe = dataframe[1:]  # take the data less the header row
    dataframe.columns = new_header  # set the header row as the df header

    return dataframe
