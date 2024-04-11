# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from prefeitura_rio.core import settings
from prefeitura_rio.pipelines_templates.run_dbt_model.tasks import (
    get_model_secret_parametes,
)
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import (
    task_rename_current_flow_run_dataset_table,
)
from prefeitura_rio.pipelines_utils.tasks import (
    is_valid_dictionary,
    merge_dictionaries,
    task_run_dbt_model_task,
)

with Flow(
    name=settings.FLOW_NAME_EXECUTE_DBT_MODEL,
) as templates__run_dbt_model__flow:
    # Parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id", default=None, required=False)
    dbt_alias = Parameter("dbt_alias", default=False, required=False)
    upstream = Parameter("upstream", default=None, required=False)
    downstream = Parameter("downstream", default=None, required=False)
    exclude = Parameter("exclude", default=None, required=False)
    flags = Parameter("flags", default=None, required=False)
    dbt_model_parameters = Parameter("dbt_model_parameters", default={}, required=False)
    dbt_model_secret_parameters = Parameter(
        "dbt_model_secret_parameters", default=[], required=False
    )

    rename_flow_run = task_rename_current_flow_run_dataset_table(
        prefix="Materialize: ", dataset_id=dataset_id, table_id=table_id
    )
    # Parse model parameters
    public_model_parameters = is_valid_dictionary(dbt_model_parameters)

    # Get secret model parameters
    secret_model_parameters = get_model_secret_parametes(dbt_model_secret_parameters)

    # Merge parameters
    model_parameters = merge_dictionaries(
        dict1=public_model_parameters, dict2=secret_model_parameters
    )

    run_dbt_model_task_return = task_run_dbt_model_task(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_alias=dbt_alias,
        upstream=upstream,
        downstream=downstream,
        exclude=exclude,
        flags=flags,
        _vars=model_parameters,
    )
    run_dbt_model_task_return.set_upstream(rename_flow_run)

# Storage and run configs
templates__run_dbt_model__flow.storage = GCS("<REPLACE_ME_WHEN_USING>")
templates__run_dbt_model__flow.run_config = KubernetesRun(
    image="<REPLACE_ME_WHEN_USING>",
)
