# -*- coding: utf-8 -*-
try:
    from prefect import Parameter, case
    from prefect.run_configs import KubernetesRun
    from prefect.storage import GCS
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["prefect"], extras=["pipelines"])

from prefeitura_rio.core import settings
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.tasks import (  # TODO: review those tasks
    download_data_to_gcs,
    get_project_id,
    rename_current_flow_run_dataset_table,
    trigger_cron_job,
    update_last_trigger,
)

with Flow(
    name=settings.FLOW_NAME_DUMP_TO_GCS,
) as flow:
    project_id = Parameter("project_id", required=False)
    dataset_id = Parameter("dataset_id")  # dataset_id or dataset_id_staging
    table_id = Parameter("table_id")
    query = Parameter("query", required=False)
    jinja_query_params = Parameter("jinja_query_params", required=False)
    bd_project_mode = Parameter(
        "bd_project_mode", required=False, default="prod"
    )  # prod or staging
    billing_project_id = Parameter("billing_project_id", required=False)
    maximum_bytes_processed = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=settings.GCS_DUMP_MAX_BYTES_PROCESSED_PER_TABLE,
    )
    desired_crontab = Parameter(
        "desired_crontab",
        required=False,
        default="0 0 * * *",
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump to GCS: ", dataset_id=dataset_id, table_id=table_id
    )

    project_id = get_project_id(project_id=project_id, bd_project_mode=bd_project_mode)

    trigger_download, execution_time = trigger_cron_job(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        cron_expression=desired_crontab,
    )

    with case(trigger_download, True):
        download_task = download_data_to_gcs(  # pylint: disable=C0103
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            query=query,
            jinja_query_params=jinja_query_params,
            bd_project_mode=bd_project_mode,
            billing_project_id=billing_project_id,
            maximum_bytes_processed=maximum_bytes_processed,
        )

        update_task = update_last_trigger(  # pylint: disable=C0103
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            execution_time=execution_time,
        )
        update_task.set_upstream(download_task)


flow.storage = GCS("<REPLACE_ME_WHEN_USING")
flow.run_config = KubernetesRun(image="<REPLACE_ME_WHEN_USING>")
