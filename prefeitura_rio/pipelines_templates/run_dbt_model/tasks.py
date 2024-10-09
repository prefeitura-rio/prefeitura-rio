# -*- coding: utf-8 -*-
import base64
from pathlib import Path
from typing import List

from prefect import task

from prefeitura_rio.pipelines_utils.infisical import get_secret, get_secrets_from_list
from prefeitura_rio.pipelines_utils.io import get_root_path, load_ruamel, load_yaml_file
from prefeitura_rio.pipelines_utils.logging import log


@task
def get_model_secret_parametes(dbt_model_secret_parameters: List[dict]) -> dict:

    if dbt_model_secret_parameters == []:
        return {}

    return get_secrets_from_list(secrets_list=dbt_model_secret_parameters)


@task
def inject_infisical_dbt_credential(secret_dict: dict = None) -> None:

    if secret_dict:

        secret_path = secret_dict["secret_path"]
        secret_name = secret_dict["secret_name"]

        log(msg=f"Getting secret: {secret_path}{secret_name}")

        service_account_dict_b64 = get_secret(path=secret_path, secret_name=secret_name)

        service_account = base64.b64decode(service_account_dict_b64[secret_name])

        with open("/tmp/credentials.json", "wb") as credentials_file:
            credentials_file.write(service_account)

        log(msg="New DBT credential injected")


@task
def inject_dbt_project_materialization(dbt_project_materialization: str = None) -> None:
    if dbt_project_materialization:
        # change project in pipelines/queries/profiles.yml
        root_path = get_root_path()
        profiles_file = Path(root_path / "queries" / "profiles.yml")

        # open the yml ang change the queries[output]
        profiles_yml = load_yaml_file(profiles_file)
        profiles_yml["queries"]["outputs"]["dev"]["project"] = dbt_project_materialization

        ruamel = load_ruamel()
        ruamel.dump(
            profiles_yml,
            open(Path(profiles_file), "w", encoding="utf-8"),
        )

        log(msg=f"New DBT project injected: {dbt_project_materialization}")
