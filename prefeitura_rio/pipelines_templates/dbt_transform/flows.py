# -*- coding: utf-8 -*-
from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from prefeitura_rio.core import settings
from prefeitura_rio.pipelines_utils.custom import Flow

from prefeitura_rio.pipelines_templates.dbt_transform.tasks import (
    check_if_dbt_artifacts_upload_is_needed,
    create_dbt_report,
    download_dbt_artifacts_from_gcs,
    download_repository,
    execute_dbt,
    get_target_from_environment,
    rename_current_flow_run_dbt,
    upload_dbt_artifacts_to_gcs,
)

from prefeitura_rio.pipelines_utils.tasks import (
    get_current_flow_project_name,
)



with Flow(
    name=settings.FLOW_NAME_DBT_TRANSFORM,
) as templates__dbt_transform__flow:

    #####################################
    # Parameters
    #####################################

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=False)
    SEND_DISCORD_REPORT = Parameter("send_discord_report", default=True)

    # DBT
    COMMAND = Parameter("command", default="test", required=False)
    SELECT = Parameter("select", default=None, required=False)
    EXCLUDE = Parameter("exclude", default=None, required=False)
    FLAG = Parameter("flag", default=None, required=False)
    GITHUB_REPO = Parameter("github_repo", default=None, required=True)
    BIGQUERY_PROJECT = Parameter("bigquery_project", default=None, required=True)

    # GCP
    ENVIRONMENT = Parameter("environment", default="dev")
    GCS_BUCKETS = Parameter("gcs_buckets", default=None, required=True)


    #####################################
    # Set environment
    ####################################
    target = get_target_from_environment(environment=ENVIRONMENT)

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run_dbt(command=COMMAND, select=SELECT, exclude=EXCLUDE, target=target)

    download_repository_task = download_repository(git_repository_path=GITHUB_REPO)
    download_repository_task.set_upstream(target)

    install_dbt_packages = execute_dbt(
        repository_path=download_repository_task,
        target=target,
        command="deps",
    )
    install_dbt_packages.set_upstream(download_repository_task)

    download_dbt_artifacts_task = download_dbt_artifacts_from_gcs(
        dbt_path=download_repository_task, environment=ENVIRONMENT, gcs_buckets=GCS_BUCKETS
    )

    ####################################
    # Tasks section #1 - Execute commands in DBT
    #####################################

    running_results = execute_dbt(
        repository_path=download_repository_task,
        state=download_dbt_artifacts_task,
        target=target,
        command=COMMAND,
        select=SELECT,
        exclude=EXCLUDE,
        flag=FLAG,
    )
    running_results.set_upstream([install_dbt_packages, download_dbt_artifacts_task])

    with case(SEND_DISCORD_REPORT, True):
        create_dbt_report_task = create_dbt_report(
            running_results=running_results, 
            repository_path=download_repository_task,
            project_name=BIGQUERY_PROJECT,
        )

    ####################################
    # Task section #2 - Tag BigQuery Tables
    ################################

    # Classify tables
    # Tag tables

    ####################################
    # Tasks section #3 - Upload new artifacts to GCS
    #####################################

    check_if_upload_dbt_artifacts = check_if_dbt_artifacts_upload_is_needed(command=COMMAND)

    with case(check_if_upload_dbt_artifacts, True):
        upload_dbt_artifacts_to_gcs_task = upload_dbt_artifacts_to_gcs(
            dbt_path=download_repository_task, environment=ENVIRONMENT, gcs_buckets=GCS_BUCKETS
        )
        upload_dbt_artifacts_to_gcs_task.set_upstream(running_results)

# Storage and run configs
templates__dbt_transform__flow.storage = GCS("<REPLACE_ME_WHEN_USING>")
templates__dbt_transform__flow.run_config = KubernetesRun(
    image="<REPLACE_ME_WHEN_USING>",
)
