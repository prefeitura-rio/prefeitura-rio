# -*- coding: utf-8 -*-
import base64
from os import environ
from typing import List, Literal, Tuple

try:
    from infisical import InfisicalClient
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["infisical"], extras=["pipelines"])

import json
from os import getenv

from google.oauth2 import service_account

from prefeitura_rio.pipelines_utils.env import getenv_or_action
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode


def get_connection_string_from_secret(secret_path: str) -> str:
    """
    Returns a connection string from a secret in Vault.
    """
    return get_secret(secret_path)


def get_infisical_client() -> InfisicalClient:
    """
    Returns an Infisical client using the default settings from environment variables.

    Returns:
        InfisicalClient: The Infisical client.
    """
    token = getenv_or_action("INFISICAL_TOKEN", action="raise")
    site_url = getenv_or_action("INFISICAL_ADDRESS", action="raise")
    log(f"INFISICAL_ADDRESS: {site_url}")
    return InfisicalClient(
        token=token,
        site_url=site_url,
    )


def get_secret_folder(
    secret_path: str = "/",
    secret_name: str = None,
    type: Literal["shared", "personal"] = "personal",
    environment: str = None,
    client: InfisicalClient = None,
) -> dict:
    """
    Fetches secrets from Infisical. If passing only `secret_path` and
    no `secret_name`, returns all secrets inside a folder.

    Args:
        secret_name (str, optional): _description_. Defaults to None.
        secret_path (str, optional): _description_. Defaults to '/'.
        environment (str, optional): _description_. Defaults to 'dev'.

    Returns:
        _type_: _description_
    """
    if client is None:
        client = get_infisical_client()
    if not environment:
        environment = get_flow_run_mode() or environment
    if not secret_path.startswith("/"):
        secret_path = f"/{secret_path}"
    if secret_path and not secret_name:
        secrets = client.get_all_secrets(path=secret_path)
        return {s.secret_name: s.secret_value for s in secrets}

    secret = client.get_secret(
        secret_name=secret_name, path=secret_path, type=type, environment=environment
    )
    return {secret_name: secret.secret_value}


def get_secret(
    secret_name: str,
    environment: str = None,
    type: Literal["shared", "personal"] = "personal",
    path: str = "/",
    client: InfisicalClient = None,
) -> dict:
    """
    Returns the secret with the given name from Infisical.

    Args:
        secret_name (str): The name of the secret to retrieve.
        environment (str): The environment to retrieve the secret from.
        type (Literal["shared", "personal"], optional): The type of secret to retrieve. Defaults to
            "personal".
        path (str, optional): The path to retrieve the secret from. Defaults to "/".
        client (InfisicalClient, optional): The Infisical client to use. Defaults to None.

    Returns:
        str: The value of the secret.
    """
    if client is None:
        client = get_infisical_client()

    if not environment:
        environment = get_flow_run_mode() or environment
    secret = client.get_secret(
        secret_name=secret_name,
        type=type,
        environment=environment,
        path=path,
    )
    return {secret_name: secret.secret_value}


def get_database_username_and_password_from_secret(
    secret_path: str,
    client: InfisicalClient = None,
) -> Tuple[str, str]:
    """
    Returns a username and password from a secret in Vault.
    """
    secrets = []
    for secret_name in [
        "DB_USERNAME",
        "DB_PASSWORD",
    ]:
        secret = get_secret(secret_name=secret_name, path=secret_path, client=client)
        secrets.append(secret.get(secret_name))
    return secrets


def inject_env(
    secret_name: str,
    environment: str = None,
    type: Literal["shared", "personal"] = "personal",
    path: str = "/",
    client: InfisicalClient = None,
) -> None:
    """
    Loads the secret with the given name from Infisical into an environment variable.

    Args:
        secret_name (str): The name of the secret to retrieve.
        environment (str): The environment to retrieve the secret from.
        type (Literal["shared", "personal"], optional): The type of secret to retrieve.
            Defaults to "personal".
        path (str, optional): The path to retrieve the secret from. Defaults to "/".
        client (InfisicalClient, optional): The Infisical client to use. Defaults to None.
    """
    if client is None:
        client = get_infisical_client()

    if not environment:
        environment = get_flow_run_mode() or environment
    log(f"Getting secret: {secret_name}")
    secret_value = client.get_secret(
        secret_name=secret_name,
        type=type,
        environment=environment,
        path=path,
    ).secret_value

    environ[secret_name] = secret_value


def inject_bd_credentials() -> None:
    """
    Loads Base dos Dados credentials from Infisical into environment variables.
    """
    client = get_infisical_client()

    # environment = get_flow_run_mode()
    environment = "staging"
    log(f"ENVIROMENT: {environment}")
    for secret_name in [
        "BASEDOSDADOS_CONFIG",
        "BASEDOSDADOS_CREDENTIALS_PROD",
        "BASEDOSDADOS_CREDENTIALS_STAGING",
    ]:
        inject_env(
            secret_name=secret_name,
            environment=environment,
            client=client,
        )

    service_account_name = f"BASEDOSDADOS_CREDENTIALS_{environment.upper()}"
    service_account = base64.b64decode(environ[service_account_name])
    with open("/tmp/credentials.json", "wb") as credentials_file:
        credentials_file.write(service_account)
    environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/credentials.json"


def get_credentials_from_env(
    mode: str = "prod", scopes: List[str] = None
) -> service_account.Credentials:
    """
    Gets credentials from env vars
    """
    if mode not in ["prod", "staging"]:
        raise ValueError("Mode must be 'prod' or 'staging'")
    env: str = getenv(f"BASEDOSDADOS_CREDENTIALS_{mode.upper()}", "")
    if env == "":
        raise ValueError(f"BASEDOSDADOS_CREDENTIALS_{mode.upper()} env var not set!")
    info: dict = json.loads(base64.b64decode(env))
    cred: service_account.Credentials = service_account.Credentials.from_service_account_info(info)
    if scopes:
        cred = cred.with_scopes(scopes)
    return cred
