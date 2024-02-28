# -*- coding: utf-8 -*-
from typing import Dict, List

try:
    from prefect.tasks.dbt.dbt import DbtShellTask
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["prefect"], extras=["pipelines"])

from prefeitura_rio.pipelines_utils.io import get_root_path
from prefeitura_rio.pipelines_utils.logging import log


def run_dbt_model(
    dataset_id: str = None,
    table_id: str = None,
    dbt_alias: bool = False,
    upstream: bool = None,
    downstream: bool = None,
    exclude: str = None,
    flags: str = None,
    _vars: dict | List[Dict] = None,
):
    """
    Runs a DBT model.

    Args:
        dataset_id (str): Dataset ID of the dbt model.
        table_id (str, optional): Table ID of the dbt model. If None, the
        whole dataset will be run.
        dbt_alias (bool, optional): If True, the model will be run by
        its alias. Defaults to False.
        upstream (bool, optional): If True, the upstream models will be run.
        downstream (bool, optional): If True, the downstream models will
        be run.
        exclude (str, optional): Models to exclude from the run.
        flags (str, optional): Flags to pass to the dbt run command.
        See:
        https://docs.getdbt.com/reference/dbt-jinja-functions/flags/
        _vars (Union[dict, List[Dict]], optional): Variables to pass to
        dbt. Defaults to None.
    """
    # Set models and upstream/downstream for dbt

    log(f"RUNNING DBT MODEL: {dataset_id}.{table_id}\nDBT_ALIAS: {dbt_alias}")

    run_command = "dbt run --select "

    if upstream:
        run_command += "+"

    if table_id:
        if dbt_alias:
            table_id = f"{dataset_id}.{dataset_id}__{table_id}"
        else:
            table_id = f"{dataset_id}.{table_id}"
    else:
        table_id = dataset_id

    run_command += f"{table_id}"

    if downstream:
        run_command += "+"

    if exclude:
        run_command += f" --exclude {exclude}"

    if _vars:
        if isinstance(_vars, list):
            vars_dict = {}
            for elem in _vars:
                vars_dict.update(elem)
            vars_str = f'"{vars_dict}"'
            run_command += f" --vars {vars_str}"
        else:
            vars_str = f'"{_vars}"'
            run_command += f" --vars {vars_str}"

    if flags:
        run_command += f" {flags}"

    log(f"Running dbt with command: {run_command}")
    root_path = get_root_path()
    queries_dir = str(root_path / "queries")
    dbt_task = DbtShellTask(
        profiles_dir=queries_dir,
        helper_script=f"cd {queries_dir}",
        log_stderr=True,
        return_all=True,
        command=run_command,
    )
    dbt_logs = dbt_task.run()

    log("\n".join(dbt_logs))


def get_basic_treated_query(table):
    """
    generates a basic treated query
    """

    originais = table["original_name"].tolist()
    nomes = table["name"].tolist()
    tipos = table["type"].tolist()

    project_id = table["project_id"].unique()[0]
    dataset_id = table["dataset_id"].unique()[0]
    dataset_id = dataset_id.replace("_staging", "")
    table_id = table["table_id"].unique()[0]

    indent_space = 4 * " "
    query = "SELECT \n"
    for original, nome, tipo in zip(originais, nomes, tipos):
        if tipo == "GEOGRAPHY":
            query += indent_space + f"ST_GEOGFROMTEXT({original}) AS {nome},\n"
        elif "id_" in nome or tipo == "INT64":
            query += (
                indent_space
                + f"SAFE_CAST(REGEXP_REPLACE(TRIM({original}), r'\.0$', '') AS {tipo}) AS {nome},\n"  # noqa
            )
        elif tipo == "DATETIME":
            query += (
                indent_space
                + f"SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', {original}) AS {tipo}) AS {nome},\n"  # noqa
            )
        elif tipo == "DATE":
            query += indent_space + f"SAFE_CAST(DATE({original}) AS {tipo}) AS {nome},\n"
        elif tipo == "FLOAT64":
            query += (
                indent_space
                + f"SAFE_CAST(REGEXP_REPLACE({original}, r',', '.') AS {tipo}) AS {nome},\n"
            )
        else:
            query += indent_space + f"SAFE_CAST(TRIM({original}) AS {tipo}) AS {nome},\n"

    query += f"FROM `{project_id}.{dataset_id}_staging.{table_id}` AS t"

    return query


def generate_basic_treated_queries(dataframe, save=False):
    """
    generates a basic treated queries
    """

    cols_to_rename = {
        "project_id": "project_id",
        "dataset_id": "dataset_id",
        "table_id_x": "table_id",
        "column_name": "original_name",
        "nome_da_coluna": "name",
        "tipo_da_coluna": "type",
    }
    dataframe = dataframe[list(cols_to_rename.keys())]
    dataframe = dataframe.rename(columns=cols_to_rename)
    for table_id in dataframe["table_id"].unique().tolist():
        table = dataframe[dataframe["table_id"] == table_id]
        query = get_basic_treated_query(table)
        print(query)
        print("\n\n")
        if save:
            with open(f"./{table_id}.sql", "w") as f:
                f.write(query)
