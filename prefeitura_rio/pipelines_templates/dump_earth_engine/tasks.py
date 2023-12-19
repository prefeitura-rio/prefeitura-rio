# -*- coding: utf-8 -*-
import json
from pathlib import Path

try:
    from prefect import task
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["prefect"], extras=["pipelines"])
from prefeitura_rio.pipelines_utils.infisical import get_secret
from prefeitura_rio.pipelines_utils.logging import log


@task
def get_earth_engine_key_from_secret(
    vault_path_earth_engine_key: str,
):
    """
    Get earth engine service account key from vault.
    """
    log(
        f"Getting Earth Engine key from https://vault.dados.rio/ui/vault/secrets/secret/show/{vault_path_earth_engine_key}"  # noqa
    )
    secret = get_secret(vault_path_earth_engine_key)

    service_account_secret_path = Path("/tmp/earth-engine/key.json")
    service_account_secret_path.parent.mkdir(parents=True, exist_ok=True)

    with open(service_account_secret_path, "w", encoding="utf-8") as f:
        json.dump(secret, f, ensure_ascii=False, indent=4)

    return service_account_secret_path
