# -*- coding: utf-8 -*-

from typing import List, Union

from pipelines_utils.dataplex import DataProfile, DataQuality
from prefect import task


@task
def build_row_filter(field: str, value, operator: str):
    """
    Build a row filter for running parameterized data
    quality scans. The final row filter should a valid
    expression for a SQL `WHERE` clause

    Args:
        field (str): table field for filtering
        value (_type_): value for the table field
        operator (str): the comparison operator for
        the filter. Could be ">", "<=", "IS" or any other
        valid comparison operator valid in a SQL `WHERE`
        clause
    """
    return f"{field}{operator}{value}"


@task
def run_data_quality_scan_parameterized(
    data_scan_id: str, row_filters: Union[List, str], wait_run_completion: bool = True
):
    """
    Runs a data quality scan defined by the `data_scan_id`.
    `row_filters` can be a string for a valid expression to a
    SQL `WHERE` clause or a list of such strings.
    Examples:
        row_filters = "data>='2024-01-01'"
        row_filters = ["data>='2024-01-01'", "<other_field> = <other_value>"]
        row_filters = "data>='2024-01-01' AND <other_field> = <other_value>"
    Args:
        data_scan_id (str): id for the data quality scan defined at dataplex
        row_filters (Union[List, str]): as described above
        wait_run_completion (bool, optional): If this task run should wait for
        the data quality scan to finish running. Defaults to True.

    Returns:
        Union[
            google.cloud.dataplex_v1.RunRequestResponse,
            google.cloud.dataplex_v1.DataScanJob
        ]: objects containing info about the data quality scan run
    """
    scan = DataQuality(data_scan_id=data_scan_id)
    result = scan.run_parameterized(
        row_filters=row_filters, wait_run_completion=wait_run_completion
    )
    return result


@task
def run_data_quality_scan(data_scan_id: str, wait_run_completion: bool = True):
    scan = DataQuality(data_scan_id=data_scan_id)
    result = scan.run(wait_run_completion=wait_run_completion)
    return result


@task
def run_data_profile(data_profile_id: str, wait_run_completion: bool = False):
    """
    Run a data profile defined by the data_profile_id. For defining such profilings
    visit

    Args:
        data_profile_id (str): _description_
        wait_run_completion (bool, optional): _description_. Defaults to False.

    Returns:
        _type_: _description_
    """
    scan = DataProfile(data_scan_id=data_profile_id)
    result = scan.run(wait_run_completion=wait_run_completion)
    return result
