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
