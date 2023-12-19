# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep
from typing import Dict, List, Union
from uuid import uuid4

try:
    import basedosdados as bd
    import jinja2
    import pandas as pd
    import prefect
    from basedosdados.download.base import google_client
    from basedosdados.upload.base import Base
    from google.cloud import bigquery
    from prefect import Client, task
    from prefect.backend import FlowRunView
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["basedosdados", "prefect"], extras=["pipelines"])
try:
    import requests
except ImportError:
    pass

from prefeitura_rio.core import settings
from prefeitura_rio.pipelines_utils.bd import get_project_id as get_project_id_function
from prefeitura_rio.pipelines_utils.dbt import run_dbt_model
from prefeitura_rio.pipelines_utils.gcs import list_blobs_with_prefix
from prefeitura_rio.pipelines_utils.infisical import (
    get_connection_string_from_secret as get_connection_string_from_secret_function,
)
from prefeitura_rio.pipelines_utils.infisical import (
    get_username_and_password_from_secret,
)
from prefeitura_rio.pipelines_utils.io import (
    dataframe_to_csv,
    dataframe_to_parquet,
    determine_whether_to_execute_or_not,
    human_readable,
    to_partitions,
)
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.pandas import (
    clean_dataframe,
    dump_header_to_file,
    parse_date_columns,
    remove_columns_accents,
)
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client
from prefeitura_rio.utils import base_assert_dependencies


@task(
    max_retries=settings.TASK_MAX_RETRIES_DEFAULT,
    retry_delay=timedelta(seconds=settings.TASK_RETRY_DELAY_DEFAULT),
)
def create_table_and_upload_to_gcs(
    data_path: str | Path,
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    biglake_table: bool = True,
) -> None:
    """
    Create table using BD+ and upload to GCS.
    """
    bd_version = bd.__version__
    log(f"USING BASEDOSDADOS {bd_version}")
    # pylint: disable=C0103
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    table_staging = f"{tb.table_full_name['staging']}"
    # pylint: disable=C0103
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
    storage_path_link = (
        f"https://console.cloud.google.com/storage/browser/{st.bucket_name}"
        f"/staging/{dataset_id}/{table_id}"
    )

    # prod datasets is public if the project is datario. staging are private im both projects
    dataset_is_public = tb.client["bigquery_prod"].project == "datario"

    #####################################
    #
    # MANAGEMENT OF TABLE CREATION
    #
    #####################################
    log("STARTING TABLE CREATION MANAGEMENT")
    log(f"GETTING DATA FROM: {data_path}")
    if dump_mode == "append":
        if tb.table_exists(mode="staging"):
            log(f"MODE APPEND: Table ALREADY EXISTS:" f"\n{table_staging}" f"\n{storage_path_link}")
        else:
            # the header is needed to create a table when dosen't exist
            log("MODE APPEND: Table DOSEN'T EXISTS\nStart to CREATE HEADER file")
            header_path = dump_header_to_file(data_path=data_path)
            log("MODE APPEND: Created HEADER file:\n" f"{header_path}")

            tb.create(
                path=header_path,
                if_storage_data_exists="replace",
                if_table_exists="replace",
                biglake_table=biglake_table,
                dataset_is_public=dataset_is_public,
            )

            log(
                "MODE APPEND: Sucessfully CREATED A NEW TABLE:\n"
                f"{table_staging}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301

            st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
            log(
                "MODE APPEND: Sucessfully REMOVED HEADER DATA from Storage:\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301
    elif dump_mode == "overwrite":
        if tb.table_exists(mode="staging"):
            log(
                "MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301
            st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
            log(
                "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301
            # delete only staging table and let DBT overwrite the prod table
            tb.delete(mode="staging")
            log(
                "MODE OVERWRITE: Sucessfully DELETED TABLE:\n" f"{table_staging}\n"
            )  # pylint: disable=C0301

        # the header is needed to create a table when dosen't exist
        # in overwrite mode the header is always created
        log("MODE OVERWRITE: Table DOSEN'T EXISTS\nStart to CREATE HEADER file")
        header_path = dump_header_to_file(data_path=data_path)
        log("MODE OVERWRITE: Created HEADER file:\n" f"{header_path}")

        tb.create(
            path=header_path,
            if_storage_data_exists="replace",
            if_table_exists="replace",
            biglake_table=biglake_table,
            dataset_is_public=dataset_is_public,
        )

        log(
            "MODE OVERWRITE: Sucessfully CREATED TABLE\n"
            f"{table_staging}\n"
            f"{storage_path_link}"
        )

        st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
        log(
            f"MODE OVERWRITE: Sucessfully REMOVED HEADER DATA from Storage\n:"
            f"{storage_path}\n"
            f"{storage_path_link}"
        )  # pylint: disable=C0301

    #####################################
    #
    # Uploads a bunch of files using BD+
    #
    #####################################

    log("STARTING UPLOAD TO GCS")
    if tb.table_exists(mode="staging"):
        # the name of the files need to be the same or the data doesn't get overwritten
        tb.append(filepath=data_path, if_exists="replace")

        log(
            f"STEP UPLOAD: Successfully uploaded {data_path} to Storage:\n"
            f"{storage_path}\n"
            f"{storage_path_link}"
        )
    else:
        # pylint: disable=C0301
        log("STEP UPLOAD: Table does not exist in STAGING, need to create first")

    return data_path


@task
def task_dataframe_to_csv(dataframe: pd.DataFrame, base_path: str | Path, unique_file: bool = True):
    if unique_file:
        now = datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
        filepath = str(Path(base_path) / f"data_{now}.csv")
    else:
        filepath = str(Path(base_path) / "data.csv")

    log(f"Saving dataframe: {filepath}")
    dataframe_to_csv(dataframe=dataframe, filepath=filepath)
    return base_path


@task(
    max_retries=settings.TASK_MAX_RETRIES_DEFAULT,
    retry_delay=timedelta(seconds=settings.TASK_RETRY_DELAY_DEFAULT),
    nout=2,
)
def dump_batches_to_file(
    database,
    batch_size: int,
    prepath: str | Path,
    date_field: str = None,
    date_lower_bound: str = None,
    date_format: str = None,
    batch_data_type: str = "csv",
) -> Path:
    """
    Dumps batches of data to FILE.
    """
    # If either date_field or date_lower_bound is provided, all of them must be set
    if date_field and not (date_lower_bound and date_format):
        raise ValueError(
            "If date_field is provided, date_lower_bound and date_format must be provided as well."
        )
    if date_lower_bound and not (date_field and date_format):
        raise ValueError(
            "If date_lower_bound is provided, date_field and date_format must be provided as well."
        )
    date_lower_bound_datetime = (
        datetime.strptime(date_lower_bound, date_format) if date_lower_bound else None
    )
    # Dump batches
    batch = database.fetch_batch(batch_size, date_field, date_lower_bound_datetime)
    idx = 0
    while len(batch) > 0:
        if idx % 100 == 0:
            log(f"Dumping batch {idx} with size {len(batch)}")
        # Batch -> DataFrame
        dataframe: pd.DataFrame = pd.DataFrame(batch)
        # Clean DataFrame
        old_columns = dataframe.columns.tolist()
        dataframe.columns = remove_columns_accents(dataframe)
        new_columns_dict = dict(zip(old_columns, dataframe.columns.tolist()))
        dataframe = clean_dataframe(dataframe)
        # DataFrame -> File
        if date_field:
            dataframe, date_partition_columns = parse_date_columns(
                dataframe, new_columns_dict[date_field]
            )
            to_partitions(
                data=dataframe,
                partition_columns=date_partition_columns,
                savepath=prepath,
                data_type=batch_data_type,
            )
        elif batch_data_type == "csv":
            dataframe_to_csv(dataframe, prepath / f"{uuid4()}.csv")
        elif batch_data_type == "parquet":
            dataframe_to_parquet(dataframe, prepath / f"{uuid4()}.parquet")
        # Get next batch
        batch = database.fetch_batch(batch_size, date_field, date_lower_bound)
        idx += 1

    log(f"Successfully dumped {idx} batches with size {len(batch)}")

    return prepath, idx


@task(checkpoint=False)
def get_connection_string_from_secret(secret_path: str):
    """
    Returns the connection string for the given secret path.
    """
    log(f"Getting connection string for secret path: {secret_path}")
    return get_connection_string_from_secret_function(secret_path)


@task
def get_current_flow_project_name() -> str:
    """
    Get the project name of the current flow.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    flow_run_view = FlowRunView.from_flow_run_id(flow_run_id)
    return flow_run_view._flow.project_name


@task(
    max_retries=settings.TASK_MAX_RETRIES_DEFAULT,
    retry_delay=timedelta(seconds=settings.TASK_RETRY_DELAY_DEFAULT),
)
def get_datario_geodataframe(
    url: str,  # URL of the data.rio API
    path: str | Path,
):
    """ "
    Save a CSV from data.rio API

    Parameters:
        - url (str): URL of the data.rio API
        - path (Union[str, Path]): Local path to save the file
    """
    base_assert_dependencies(["requests"], extras=["pipelines-templates"])
    # Create the path if it doesn't exist
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)

    # Set the file path to save the data
    file_path = path / "geo_data" / "data.geojson"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Make a request to the API URL to download the data
    req = requests.get(url, stream=True)
    # Save the data to the specified file path
    with open(file_path, "wb") as file:
        for chunk in req.iter_content(chunk_size=1024):
            if chunk:
                file.write(chunk)
                file.flush()

    log("Data saved")

    return file_path


@task(checkpoint=False)
def get_now_datetime():
    """
    Returns the current datetime in YYYY_MM_DD__H_M_S.
    """
    return datetime.now().strftime("%Y_%m_%d__%H_%M_%S")


@task
def get_project_id(
    project_id: str = None,
    bd_project_mode: str = "prod",
):
    """
    Get the project ID.
    """
    if project_id:
        return project_id
    log("Project ID was not provided, trying to get it from environment variable")
    return get_project_id_function(bd_project_mode)


@task(checkpoint=False, nout=2)
def get_user_and_password(secret_path: str, wait=None):
    """
    Returns the user and password for the given secret path.
    """
    log(f"Getting user and password for secret path: {secret_path}")
    return get_username_and_password_from_secret(secret_path)


@task
def greater_than(value, compare_to) -> bool:
    """
    Returns True if value is greater than compare_to.
    """
    return value > compare_to


@task
def parse_comma_separated_string_to_list(text: str) -> List[str]:
    """
    Parses a comma separated string to a list.

    Args:
        text: The text to parse.

    Returns:
        A list of strings.
    """
    if text is None or not text:
        return []
    # Remove extras.
    text = text.replace("\n", "")
    text = text.replace("\r", "")
    text = text.replace("\t", "")
    while ",," in text:
        text = text.replace(",,", ",")
    while text.endswith(","):
        text = text[:-1]
    result = [x.strip() for x in text.split(",")]
    result = [item for item in result if item != "" and item is not None]
    return result


@task
def rename_current_flow_run_dataset_table(prefix: str, dataset_id, table_id) -> None:
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()
    return client.set_flow_run_name(flow_run_id, f"{prefix}{dataset_id}.{table_id}")


@task
def rename_current_flow_run_msg(msg: str, wait=None) -> None:
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()
    return client.set_flow_run_name(flow_run_id, msg)


@task
def task_run_dbt_model_task(
    dataset_id: str = None,
    table_id: str = None,
    dbt_alias: bool = False,
    upstream: bool = None,
    downstream: bool = None,
    exclude: str = None,
    flags: str = None,
    _vars: dict | List[Dict] = None,
) -> None:
    run_dbt_model(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_alias=dbt_alias,
        upstream=upstream,
        downstream=downstream,
        exclude=exclude,
        flags=flags,
        _vars=_vars,
    )


@task
def download_data_to_gcs(  # pylint: disable=R0912,R0913,R0914,R0915
    dataset_id: str,
    table_id: str,
    project_id: str = None,
    query: Union[str, jinja2.Template] = None,
    jinja_query_params: dict = None,
    bd_project_mode: str = "prod",
    billing_project_id: str = None,
    location: str = "US",
    maximum_bytes_processed: float = settings.GCS_DUMP_MAX_BYTES_PROCESSED_PER_TABLE,
):
    """
    Get data from BigQuery.
    """
    # Try to get project_id from environment variable
    if not project_id:
        log("Project ID was not provided, trying to get it from environment variable")
        try:
            bd_base = Base()
            project_id = bd_base.config["gcloud-projects"][bd_project_mode]["name"]
        except KeyError:
            pass
        if not project_id:
            raise ValueError(
                "project_id must be either provided or inferred from environment variables"
            )
        log(f"Project ID was inferred from environment variables: {project_id}")

    # Asserts that dataset_id and table_id are provided
    if not dataset_id or not table_id:
        raise ValueError("dataset_id and table_id must be provided")

    # If query is not provided, build query from it
    if not query:
        query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
        log(f"Query was inferred from dataset_id and table_id: {query}")

    # If query is provided, use it!
    # If it's a template, we must render it.
    if not query:
        if not jinja_query_params:
            jinja_query_params = {}
        if isinstance(query, jinja2.Template):
            try:
                query = query.render(
                    {
                        "project_id": project_id,
                        "dataset_id": dataset_id,
                        "table_id": table_id,
                        **jinja_query_params,
                    }
                )
            except jinja2.TemplateError as exc:
                raise ValueError(f"Error rendering query: {exc}") from exc
            log(f"Query was rendered: {query}")

    # If query is not a string, raise an error
    if not isinstance(query, str):
        raise ValueError("query must be either a string or a Jinja2 template")
    log(f"Query was provided: {query}")

    # Get billing project ID
    if not billing_project_id:
        log("Billing project ID was not provided, trying to get it from environment variable")
        try:
            bd_base = Base()
            billing_project_id = bd_base.config["gcloud-projects"][bd_project_mode]["name"]
        except KeyError:
            pass
        if not billing_project_id:
            raise ValueError(
                "billing_project_id must be either provided or inferred from environment variables"
            )
        log(f"Billing project ID was inferred from environment variables: {billing_project_id}")

    # Checking if data exceeds the maximum allowed size
    log("Checking if data exceeds the maximum allowed size")
    # pylint: disable=E1124
    client = google_client(project_id, billing_project_id, from_file=True, reauth=False)
    job_config = bigquery.QueryJobConfig()
    job_config.dry_run = True
    job = client["bigquery"].query(query, job_config=job_config)
    while not job.done():
        sleep(1)
    table_size = job.total_bytes_processed
    log(f'Table size: {human_readable(table_size, unit="B", unit_divider=1024)}')
    if table_size > maximum_bytes_processed:
        max_allowed_size = human_readable(
            maximum_bytes_processed,
            unit="B",
            unit_divider=1024,
        )
        raise ValueError(f"Table size exceeds the maximum allowed size: {max_allowed_size}")

    # Get data
    log("Querying data from BigQuery")
    job = client["bigquery"].query(query)
    while not job.done():
        sleep(1)
    # pylint: disable=protected-access
    dest_table = job._properties["configuration"]["query"]["destinationTable"]
    dest_project_id = dest_table["projectId"]
    dest_dataset_id = dest_table["datasetId"]
    dest_table_id = dest_table["tableId"]
    log(f"Query results were stored in {dest_project_id}.{dest_dataset_id}.{dest_table_id}")

    blob_path = f"gs://datario/share/{dataset_id}/{table_id}/data*.csv.gz"
    log(f"Loading data to {blob_path}")
    dataset_ref = bigquery.DatasetReference(dest_project_id, dest_dataset_id)
    table_ref = dataset_ref.table(dest_table_id)
    job_config = bigquery.job.ExtractJobConfig(compression="GZIP")
    extract_job = client["bigquery"].extract_table(
        table_ref,
        blob_path,
        location=location,
        job_config=job_config,
    )
    extract_job.result()
    log("Data was loaded successfully")

    # Get the BLOB we've just created and make it public
    blobs = list_blobs_with_prefix("datario", f"share/{dataset_id}/{table_id}/")
    if not blobs:
        raise ValueError(f"No blob found at {blob_path}")
    for blob in blobs:
        log(f"Blob found at {blob.name}")
        blob.make_public()
        log("Blob was made public")


@task(nout=2)
def trigger_cron_job(
    project_id: str,
    dataset_id: str,
    table_id: str,
    cron_expression: str,
):
    """
    Tells whether to trigger a cron job.
    """
    redis_client = get_redis_client()
    key = f"{project_id}__{dataset_id}__{table_id}"
    log(f"Checking if cron job should be triggered for {key}")
    val = redis_client.get(key)
    current_datetime = datetime.now()
    if val and val is dict and "last_trigger" in val:
        last_trigger = val["last_trigger"]
        log(f"Last trigger: {last_trigger}")
        if last_trigger:
            return determine_whether_to_execute_or_not(
                cron_expression, current_datetime, last_trigger
            )
    log(f"No last trigger found for {key}")
    return True, current_datetime


@task
def update_last_trigger(
    project_id: str,
    dataset_id: str,
    table_id: str,
    execution_time: datetime,
):
    """
    Update the last trigger.
    """
    redis_client = get_redis_client()
    key = f"{project_id}__{dataset_id}__{table_id}"
    redis_client.set(key, {"last_trigger": execution_time})
