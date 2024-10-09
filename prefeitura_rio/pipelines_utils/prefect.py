# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from typing import List, Union

try:
    import prefect
    from prefect import task
    from prefect.client import Client
    from prefect.schedules.clocks import IntervalClock
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["prefect"], extras=["pipelines"])

from prefeitura_rio.pipelines_utils.constants import NOT_SET
from prefeitura_rio.pipelines_utils.io import query_to_line


def get_flow_run_mode() -> str:
    """
    Returns the mode of the current flow run (either "prod" or "staging").
    """
    project_name = prefect.context.get("project_name") or "dev"
    if project_name not in ["production", "staging", "dev"]:
        raise ValueError(f"Invalid project name: {project_name}")
    if project_name == "production":
        return "prod"
    elif project_name == "staging":
        return "staging"
    else:
        return "dev"


def get_flow_run_url(id: str, prefix: str = "https://prefect-dev.dados.rio") -> str:
    """
    Returns the URL of a flow run.

    Args:
        id (str): Flow run id.

    Returns:
        str: Flow run URL in the format <prefix>/<slug>/flow-run/<id>.
    """
    prefix = prefix.rstrip("/")
    tenant_id = prefect.context.get("config").get("cloud").get("tenant_id")
    tenant_slug = get_tenant_slug(tenant_id)
    url = f"{prefix}/{tenant_slug}/flow-run/{id}"
    return url


def get_tenant_slug(tenant_id: str) -> str:
    """
    Returns the slug of a tenant.

    Args:
        tenant_id (str): Tenant id.

    Returns:
        str: Tenant slug.
    """
    client = Client()
    response = client.graphql(
        query="""
        query ($tenant_id: uuid!) {
            tenant (where: {id: {_eq: $tenant_id}}) {
                slug
            }
        }
        """,
        variables={"tenant_id": tenant_id},
    )
    return response["data"]["tenant"][0]["slug"]


def set_default_parameters(flow: prefect.Flow, default_parameters: dict) -> prefect.Flow:
    """
    Sets default parameters for a flow.
    """
    for parameter in flow.parameters():
        if parameter.name in default_parameters:
            parameter.default = default_parameters[parameter.name]
    return flow


@task
def task_get_current_flow_run_labels() -> List[str]:
    """
    Returns the labels of the current flow run.
    """
    return prefect.context.get("config").get("cloud").get("agent").get("labels")


@task
def task_get_flow_group_id(flow_name: str) -> str:
    """
    Returns the flow group id for the given flow name.
    """
    client = Client()
    response = client.graphql(
        query="""
        query ($flow_name: String!) {
            flow (where: {name: {_eq: $flow_name}}) {
                flow_group {
                    id
                }
            }
        }
        """,
        variables={"flow_name": flow_name},
    )
    return response["data"]["flow"][0]["flow_group"]["id"]


@task
def task_rename_current_flow_run_dataset_table(prefix: str, dataset_id: str, table_id: str) -> None:
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()
    return client.set_flow_run_name(flow_run_id, f"{prefix}{dataset_id}.{table_id}")


def generate_dump_db_schedules(  # pylint: disable=too-many-arguments,too-many-locals
    interval: timedelta,
    start_date: datetime,
    labels: List[str],
    db_database: str,
    db_host: str,
    db_port: Union[str, int],
    db_type: str,
    dataset_id: str,
    infisical_secret_path: str,
    table_parameters: dict,
    biglake_table: bool = True,
    db_charset: str = NOT_SET,
    batch_size: int = 50000,
    runs_interval_minutes: int = 15,
) -> List[IntervalClock]:
    """
    Generates multiple schedules for database dumping.
    """
    db_port = str(db_port)
    clocks = []
    for count, (table_id, parameters) in enumerate(table_parameters.items()):
        parameter_defaults = {
            "batch_size": batch_size,
            "infisical_secret_path": infisical_secret_path,
            "db_database": db_database,
            "db_host": db_host,
            "db_port": db_port,
            "db_type": db_type,
            "dataset_id": dataset_id,
            "table_id": table_id,
            "db_charset": db_charset,
            "biglake_table": biglake_table,
            "dump_mode": parameters["dump_mode"],
            "execute_query": query_to_line(parameters["execute_query"]),
        }

        # Add remaining parameters if value is not None
        for key, value in parameters.items():
            if value is not None and key not in ["interval", "start_date"]:
                parameter_defaults[key] = value

        if "dbt_alias" in parameters:
            parameter_defaults["dbt_alias"] = parameters["dbt_alias"]
        if "dataset_id" in parameters:
            parameter_defaults["dataset_id"] = parameters["dataset_id"]
        new_interval = parameters["interval"] if "interval" in parameters else interval
        new_start_date = (
            parameters["start_date"]
            if "start_date" in parameters
            else start_date + timedelta(minutes=runs_interval_minutes * count)
        )
        clocks.append(
            IntervalClock(
                interval=new_interval,
                start_date=new_start_date,
                labels=labels,
                parameter_defaults=parameter_defaults,
            )
        )
    return clocks


def generate_dump_url_schedules(  # pylint: disable=too-many-arguments,too-many-locals
    interval: timedelta,
    start_date: datetime,
    labels: List[str],
    # db_database: str,
    # db_host: str,
    # db_port: Union[str, int],
    # db_type: str,
    dataset_id: str,
    # vault_secret_path: str,
    table_parameters: dict,
    batch_data_type: str = "csv",
    runs_interval_minutes: int = 15,
) -> List[IntervalClock]:
    """
    Generates multiple schedules for url dumping.
    """
    # db_port = str(db_port)
    clocks = []
    for count, (table_id, parameters) in enumerate(table_parameters.items()):
        parameter_defaults = {
            "batch_data_type": batch_data_type,
            "url": parameters["url"],
            "url_type": parameters["url_type"],
            "dataset_id": dataset_id if dataset_id != "" else parameters["dataset_id"],
            "table_id": table_id,
            "dump_mode": parameters["dump_mode"],
            # "vault_secret_path": vault_secret_path,
            # "db_database": db_database,
            # "db_host": db_host,
            # "db_port": db_port,
            # "db_type": db_type,
            # "execute_query": query_to_line(parameters["execute_query"]),
        }
        if "gsheets_sheet_order" in parameters:
            parameter_defaults["gsheets_sheet_order"] = parameters["gsheets_sheet_order"]
        if "gsheets_sheet_name" in parameters:
            parameter_defaults["gsheets_sheet_name"] = parameters["gsheets_sheet_name"]
        if "gsheets_sheet_range" in parameters:
            parameter_defaults["gsheets_sheet_range"] = parameters["gsheets_sheet_range"]
        if "partition_columns" in parameters:
            parameter_defaults["partition_columns"] = parameters["partition_columns"]
        if "materialize_after_dump" in parameters:
            parameter_defaults["materialize_after_dump"] = parameters["materialize_after_dump"]
        if "materialization_mode" in parameters:
            parameter_defaults["materialization_mode"] = parameters["materialization_mode"]
        if "materialize_to_datario" in parameters:
            parameter_defaults["materialize_to_datario"] = parameters["materialize_to_datario"]
        # if "dbt_model_secret_parameters" in parameters:
        #     parameter_defaults["dbt_model_secret_parameters"] = parameters[
        #         "dbt_model_secret_parameters"
        #     ]
        # if "partition_date_format" in parameters:
        #     parameter_defaults["partition_date_format"] = parameters[
        #         "partition_date_format"
        #     ]
        # if "lower_bound_date" in parameters:
        #     parameter_defaults["lower_bound_date"] = parameters["lower_bound_date"]

        new_interval = parameters["interval"] if "interval" in parameters else interval

        clocks.append(
            IntervalClock(
                interval=new_interval,
                start_date=start_date + timedelta(minutes=runs_interval_minutes * count),
                labels=labels,
                parameter_defaults=parameter_defaults,
            )
        )
    return clocks
