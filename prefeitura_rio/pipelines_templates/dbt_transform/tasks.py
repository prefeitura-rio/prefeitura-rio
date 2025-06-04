# -*- coding: utf-8 -*-
# pylint: disable=C0301
# flake8: noqa: E501
"""
Tasks for execute_dbt
"""

import os
import shutil
from typing import TypedDict

import git
import prefect
from dbt.cli.main import dbtRunner, dbtRunnerResult
from prefect.client import Client
from prefect.engine.signals import FAIL

from prefeitura_rio.pipelines_utils.credential_injector import (
    authenticated_task as task,
)
from prefeitura_rio.pipelines_utils.dbt import Summarizer, log_to_file, process_dbt_logs
from prefeitura_rio.pipelines_utils.googleutils import (
    download_from_cloud_storage,
    upload_to_cloud_storage,
)
from prefeitura_rio.pipelines_utils.infisical import get_secret
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.monitor import send_message


class GcsBucket(TypedDict):
    prod: str
    dev: str


@task
def add_dbt_secrets_to_env(dbt_secrets: list[str]) -> dict[str, str]:
    """
    Loads secrets from Infisical and sets them as environment variables.

    """

    if not dbt_secrets:
        log("No dbt secrets provided. Skipping environment variable setup.")
        return {}

    secrets_dict = {}

    for secret_name in dbt_secrets:
        try:
            secret = get_secret(secret_name=secret_name, path="/dbt-vars")
            value = secret[secret_name]
            os.environ[secret_name] = value
            secrets_dict[secret_name] = value
            log(f"Environment variable {secret_name} set successfully.")
        except KeyError:
            log(f"Secret {secret_name} not found in Infisical.")
            continue
        except Exception as e:
            log(f"Error setting environment variable {secret_name}: {e}")
            continue

    return secrets_dict


@task
def download_repository(git_repository_path: str):
    """
    Downloads the repository specified by the REPOSITORY_URL.

    This function creates a repository folder, clones the repository from the specified URL,
    and logs the success or failure of the download.

    Raises:
        FAIL: If there is an error when creating the repository folder or downloading the repository.
    """

    # create repository folder
    try:
        repository_path = os.path.join(os.getcwd(), "dbt_repository")

        if os.path.exists(repository_path):
            shutil.rmtree(repository_path, ignore_errors=False)
        os.makedirs(repository_path)

        log(f"Repository folder created: {repository_path}")

    except Exception as e:
        raise FAIL(str(f"Error when creating repository folder: {e}")) from e

    # download repository
    try:
        git.Repo.clone_from(git_repository_path, repository_path)
        log(f"Repository downloaded: {git_repository_path}")
    except git.GitCommandError as e:
        raise FAIL(str(f"Error when downloading repository: {e}")) from e

    # check for 'queries' folder
    queries_path = os.path.join(repository_path, "queries")
    if os.path.isdir(queries_path):
        log(f"'queries' folder found at: {queries_path}")
        return queries_path

    return repository_path


@task
def execute_dbt(
    repository_path: str,
    command: str = "run",
    target: str = "dev",
    select="",
    exclude="",
    state="",
    flag="",
    prefect_environment="",
):
    """
    Executes a dbt command with the specified parameters.

    Args:
        repository_path (str): The path to the dbt repository.
        command (str, optional): The dbt command to execute. Defaults to "run".
        target (str, optional): The dbt target to use. Defaults to "dev".
        select (str, optional): The dbt selector to filter models. Defaults to "".
        exclude (str, optional): The dbt selector to exclude models. Defaults to "".

    Returns:
        dbtRunnerResult: The result of the dbt command execution.
    """
    commands = command.split(" ")

    cli_args = commands + ["--profiles-dir", repository_path, "--project-dir", repository_path]

    if command in ("build", "data_test", "run", "test"):
        cli_args.extend(
            [
                "--target",
                target,
            ]
        )

        if select:
            cli_args.extend(["--select", select])
        if exclude:
            cli_args.extend(["--exclude", exclude])
        if state:
            cli_args.extend(["--state", state])
        if flag:
            cli_args.extend([flag])

        log(f"Executing dbt command: {' '.join(cli_args)}", level="info")

    dbt_runner = dbtRunner()
    running_result: dbtRunnerResult = dbt_runner.invoke(cli_args)

    log_path = os.path.join(repository_path, "logs", "dbt.log")

    log("RESULTADOS:")
    log(running_result)

    if command not in ("deps") and not os.path.exists(log_path):
        send_message(
            title="❌ Erro ao executar DBT",
            message="Não foi possível encontrar o arquivo de logs.",
            monitor_slug="dbt-runs",
            prefect_environment=prefect_environment,
        )
        raise FAIL("DBT Run seems not successful. No logs found.")

    return running_result


@task
def create_dbt_report(
    running_results: dbtRunnerResult,
    repository_path: str,
    bigquery_project: str,
    prefect_environment: str,
) -> None:
    """
    Creates a report based on the results of running dbt commands.

    Args:
        running_results (dbtRunnerResult): The results of running dbt commands.
        repository_path (str): The path to the repository.

    Raises:
        FAIL: If there are failures in the dbt commands.

    Returns:
        None
    """

    logs = process_dbt_logs(log_path=os.path.join(repository_path, "logs", "dbt.log"))
    log_path = log_to_file(logs)
    summarizer = Summarizer()

    is_successful, has_warnings = True, False

    general_report = []
    for command_result in running_results.result:
        if command_result.status == "fail":
            is_successful = False
            general_report.append(f"- 🛑 FAIL: {summarizer(command_result)}")
        elif command_result.status == "error":
            is_successful = False
            general_report.append(f"- ❌ ERROR: {summarizer(command_result)}")
        elif command_result.status == "warn":
            has_warnings = True
            general_report.append(f"- ⚠️ WARN: {summarizer(command_result)}")

    # Sort and log the general report
    general_report = sorted(general_report, reverse=True)
    general_report = "**Resumo**:\n" + "\n".join(general_report)
    log(general_report)

    # Get Parameters
    param_report = ["**Parametros**:"]

    parameters = prefect.context.get("parameters")

    if parameters.get("environment") == "dev":
        bigquery_project = "rj-" + bigquery_project + "-dev"
    elif parameters.get("environment") == "prod":
        bigquery_project = "rj-" + bigquery_project

    param_report.append(f"- Projeto BigQuery: `{bigquery_project}`")
    param_report.append(f"- Target dbt: `{parameters.get('environment')}`")
    param_report.append(f"- Comando: `{parameters.get('command')}`")

    if parameters.get("select"):
        param_report.append(f"- Select: `{parameters.get('select')}`")
    if parameters.get("exclude"):
        param_report.append(f"- Exclude: `{parameters.get('exclude')}`")
    if parameters.get("flag"):
        param_report.append(f"- Flag: `{parameters.get('flag')}`")

    param_report.append(
        f"- GitHub Repo: `{parameters.get('github_repo').rsplit('/', 1)[-1].removesuffix('.git')}`"
    )

    param_report = "\n".join(param_report)
    param_report += " \n"

    fully_successful = is_successful and running_results.success
    include_report = has_warnings or (not fully_successful)

    # DBT - Sending Logs to Discord
    command = prefect.context.get("parameters").get("command")
    emoji = "❌" if not fully_successful else "✅"
    complement = "com Erros" if not fully_successful else "sem Erros"
    message = f"{param_report}\n{general_report}" if include_report else param_report

    send_message(
        title=f"{emoji} [{bigquery_project}] - Execução `dbt {command}` finalizada {complement}",
        message=message,
        file_path=log_path,
        monitor_slug="dbt-runs",
        prefect_environment=prefect_environment,
    )

    if not fully_successful:
        raise FAIL(general_report)


@task
def rename_current_flow_run_dbt(
    command: str,
    target: str,
    select: str,
    exclude: str,
) -> None:
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()

    flow_run_name = f"dbt {command}"

    if select:
        flow_run_name += f" --select {select}"
    if exclude:
        flow_run_name += f" --exclude {exclude}"

    flow_run_name += f" --target {target}"

    client.set_flow_run_name(flow_run_id, flow_run_name)
    log(f"Flow run renamed to: {flow_run_name}", level="info")


@task
def get_target_from_environment(environment: str):
    """
    Retrieves the target environment based on the given environment parameter.
    """
    converter = {
        "prod": "prod",
        "local-prod": "prod",
        "staging": "dev",
        "local-staging": "dev",
        "dev": "dev",
    }
    return converter.get(environment, "dev")


@task
def download_dbt_artifacts_from_gcs(dbt_path: str, environment: str, gcs_buckets: GcsBucket):
    """
    Retrieves the dbt artifacts from Google Cloud Storage.
    """

    gcs_bucket = gcs_buckets[environment]

    target_base_path = os.path.join(dbt_path, "target_base")

    if os.path.exists(target_base_path):
        shutil.rmtree(target_base_path, ignore_errors=False)
        os.makedirs(target_base_path)

    try:
        download_from_cloud_storage(target_base_path, gcs_bucket)
        log(f"DBT artifacts downloaded from GCS bucket: {gcs_bucket}", level="info")
        return target_base_path

    except Exception as e:
        log(f"Error when downloading DBT artifacts from GCS: {e}", level="error")
        return None


@task
def upload_dbt_artifacts_to_gcs(dbt_path: str, environment: str, gcs_buckets: GcsBucket):
    """
    Sends the dbt artifacts to Google Cloud Storage.
    """

    dbt_artifacts_path = os.path.join(dbt_path, "target_base")

    gcs_bucket = gcs_buckets[environment]

    upload_to_cloud_storage(dbt_artifacts_path, gcs_bucket)
    log(f"DBT artifacts sent to GCS bucket: {gcs_bucket}", level="info")


@task
def check_if_dbt_artifacts_upload_is_needed(command: str):
    """
    Checks if the upload of dbt artifacts is needed.
    """

    if command in ["build", "source freshness"]:
        return True
