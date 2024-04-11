# -*- coding: utf-8 -*-
import base64
from os import environ
from typing import List, Literal, Tuple

try:
    from infisical import InfisicalClient
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["infisical"], extras=["pipelines"])

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


def get_secrets_from_list(secrets_list: List[dict]) -> dict:
    """
    Retrieves secrets based on a list of secret specifications and stores them in a dictionary.

    Each item in the secrets_list is expected to be a dictionary with at least two keys:
    'secret_name' and 'secret_path', which are used to identify and locate the secret.

    Args:
        secrets_list (List[dict]): A list of dictionaries, where each dictionary contains
        the 'secret_name' and 'secret_path' used to retrieve the secret.

    Returns:
        dict: A dictionary of secrets, with each key being the 'secret_name' and the value
        being the retrieved secret.
    """

    secret_dict = {}
    for _secret in secrets_list:
        secret = get_secret(secret_name=_secret["secret_name"], path=_secret["secret_path"])
        secret_dict[_secret["secret_name"]] = secret[_secret["secret_name"]]

    return secret_dict


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
