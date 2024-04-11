# -*- coding: utf-8 -*-
from typing import List

from prefect import task

from prefeitura_rio.pipelines_utils.infisical import get_secrets_from_list


@task
def get_model_secret_parametes(dbt_model_secret_parameters: List[dict]) -> dict:

    if dbt_model_secret_parameters == []:
        return {}

    return get_secrets_from_list(secrets_list=dbt_model_secret_parameters)
