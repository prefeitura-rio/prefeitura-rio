# -*- coding: utf-8 -*-
import base64
import json
from os import getenv
from typing import List

try:
    from google.oauth2 import service_account
except ImportError:
    pass

from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode
from prefeitura_rio.utils import assert_dependencies


def getenv_or_action(key: str, *, action: str = "raise", default: str = None):
    """
    Returns the value of the environment variable with the given key, or the result of the
    given action if the environment variable is not set.

    Args:
        key (str): The name of the environment variable.
        action (str, optional): The name of the action to perform if neither the environment
            variable nor a default value is set. Valid actions are "raise", "warn" and "ignore".
            Defaults to "raise".
        default (str, optional): The default value to return if the environment variable is
            not set.

    Raises:
        ValueError: If the action is not valid.

    Returns:
        str: The value of the environment variable, or the result of the action.
    """
    if action not in ["raise", "warn", "ignore"]:
        raise ValueError(f"Invalid action: {action}")

    value = getenv(key, default)

    if value is None:
        if action == "raise":
            raise ValueError(f"Environment variable {key} is not set")
        elif action == "warn":
            log(f"WARNING: Environment variable {key} is not set", level="warning")
        elif action == "ignore":
            pass

    return value


@assert_dependencies(["basedosdados"], extras=["pipelines"])
def get_bd_credentials_from_env(
    mode: str = None, scopes: List[str] = None
) -> service_account.Credentials:
    """
    Gets credentials from env vars
    """
    if not mode:
        mode = get_flow_run_mode()
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
