# -*- coding: utf-8 -*-
import json
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep, time
from typing import Dict, List, Tuple
from uuid import uuid4

try:
    import basedosdados as bd
    import pandas as pd
    import prefect
    from basedosdados.download.base import google_client
    from basedosdados.upload.base import Base
    from geopy.extra.rate_limiter import RateLimiter
    from geopy.geocoders import Nominatim
    from geopy.location import Location
    from google.cloud import bigquery
    from prefect import Client, task
    from prefect.backend import FlowRunView
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["basedosdados", "prefect"], extras=["pipelines"])
try:
    import ee
    import geopandas as gpd
    import requests
    from geojsplit import geojsplit
except ImportError:
    pass

from prefeitura_rio.core import settings
from prefeitura_rio.pipelines_utils.bd import get_project_id as get_project_id_function
from prefeitura_rio.pipelines_utils.bd import get_storage_blobs
from prefeitura_rio.pipelines_utils.geo import check_if_belongs_to_rio

try:
    from prefeitura_rio.pipelines_utils.database_sql import Database
except ImportError:
    pass
from prefeitura_rio.pipelines_utils.gcs import (
    delete_blobs_list,
    list_blobs_with_prefix,
    parse_blobs_to_partition_dict,
)
from prefeitura_rio.pipelines_utils.geo import load_wkt, remove_third_dimension
from prefeitura_rio.pipelines_utils.infisical import (
    get_connection_string_from_secret as get_connection_string_from_secret_function,
)
from prefeitura_rio.pipelines_utils.infisical import (
    get_secret,
    get_username_and_password_from_secret,
)
from prefeitura_rio.pipelines_utils.io import (
    dataframe_to_csv,
    dataframe_to_parquet,
    determine_whether_to_execute_or_not,
    extract_last_partition_date,
    human_readable,
    remove_tabs_from_query,
    to_partitions,
)
from prefeitura_rio.pipelines_utils.logging import log, log_mod
from prefeitura_rio.pipelines_utils.pandas import (
    batch_to_dataframe,
    build_query_new_columns,
    clean_dataframe,
    dump_header_to_file,
    parse_date_columns,
    remove_columns_accents,
)
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client
from prefeitura_rio.utils import assert_dependencies


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
def create_table_asset(
    service_account: str,
    service_account_secret_path: str,
    project_id: str,
    gcs_file_asset_path: str,
    ee_asset_path: str,
):
    """
    Create a table asset in Earth Engine.

    parameters:
        service_account
            Service account email in the format of earth-engine@<project-id>.iam.gserviceaccount.com
        service_account_secret_path
            Path to the .json file containing the service account secret.
        project_id:
            Earth Engine project ID.
        gcs_asset_path
            Path to the asset in Google Cloud Storage in the format of
            "gs://<project-id>/<some-folder>/file.csv"
        ee_asset_path
            Path that the asset will be created in Earth Engine in the format of
            projects/<project-id>/assets/<asset_name> or users/<user-id>/<asset_name>
    """

    credentials = ee.ServiceAccountCredentials(service_account, service_account_secret_path)
    ee.Initialize(credentials=credentials, project=project_id)

    params = {
        "name": ee_asset_path,
        "sources": [{"primaryPath": gcs_file_asset_path, "charset": "UTF-8"}],
    }

    request_id = ee.data.newTaskId(1)[0]
    task_status = ee.data.startTableIngestion(request_id=request_id, params=params)
    log(ee.data.getTaskStatus(task_status["id"]))


@task(
    checkpoint=False,
    max_retries=settings.TASK_MAX_RETRIES_DEFAULT,
    retry_delay=timedelta(seconds=settings.TASK_RETRY_DELAY_DEFAULT),
)
@assert_dependencies(["cx_Oracle", "pymysql", "pyodbc"], extras=["pipelines-templates"])
def database_execute(
    database: "Database",
    query: str,
    wait=None,  # pylint: disable=unused-argument
    flow_name: str = None,
    labels: List[str] = None,
    dataset_id: str = None,
    table_id: str = None,
) -> None:
    """
    Executes a query on the database.

    Args:
        database: The database object.
        query: The query to execute.
    """
    log(f"Query parsed: {query}")
    query = remove_tabs_from_query(query)
    log(f"Executing query line: {query}")
    database.execute_query(query)


@task(
    checkpoint=False,
    max_retries=settings.TASK_MAX_RETRIES_DEFAULT,
    retry_delay=timedelta(seconds=settings.TASK_RETRY_DELAY_DEFAULT),
)
def database_get(
    database_type: str,
    hostname: str,
    port: int,
    user: str,
    password: str,
    database: str,
):
    """
    Returns a database object.

    Args:
        database_type: The type of the database.
        hostname: The hostname of the database.
        port: The port of the database.
        user: The username of the database.
        password: The password of the database.
        database: The database name.

    Returns:
        A database object.
    """
    from prefeitura_rio.pipelines_utils.database_sql import (
        Database,
        MySql,
        Oracle,
        SqlServer,
    )

    DATABASE_MAPPING: Dict[str, Database] = {
        "mysql": MySql,
        "oracle": Oracle,
        "sql_server": SqlServer,
    }

    if database_type not in DATABASE_MAPPING:
        raise ValueError(f"Unknown database type: {database_type}")
    return DATABASE_MAPPING[database_type](
        hostname=hostname,
        port=port,
        user=user,
        password=password,
        database=database,
    )


@task(
    checkpoint=False,
    max_retries=settings.TASK_MAX_RETRIES_DEFAULT,
    retry_delay=timedelta(seconds=settings.TASK_RETRY_DELAY_DEFAULT),
)
def database_get_mongo(
    connection_string: str,
    database: str,
    collection: str,
):
    """
    Returns a Mongo object.

    Args:
        connection_string (str): MongoDB connection string.
        database (str): Database name.
        collection (str): Collection name.

    Returns:
        A database object.
    """
    from prefeitura_rio.pipelines_utils.database_mongo import Mongo

    return Mongo(
        connection_string=connection_string,
        database=database,
        collection=collection,
    )


@task
def download_data_to_gcs(  # pylint: disable=R0912,R0913,R0914,R0915
    project_id: str = None,
    query: str = None,
    gcs_asset_path: str = None,
    bd_project_mode: str = "prod",
    billing_project_id: str = None,
    location: str = "US",
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

    # pylint: disable=E1124
    client = google_client(project_id, billing_project_id, from_file=True, reauth=False)
    job_config = bigquery.QueryJobConfig()
    job_config.dry_run = True
    job = client["bigquery"].query(query, job_config=job_config)
    while not job.done():
        sleep(1)
    # pylint: disable=E1101
    table_size = job.total_bytes_processed
    log(f'Table size: {human_readable(table_size, unit="B", unit_divider=1024)}')

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

    blob_path = f"{gcs_asset_path}/data*.csv.gz"
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
    blobs = list_blobs_with_prefix(project_id, blob_path)
    log(f"{blobs}")
    if not blobs:
        raise ValueError(f"No blob found at {blob_path}")

    return blob_path


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


@task
def dump_upload_batch(
    database: Database,
    batch_size: int,
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    partition_columns: List[str] = None,
    batch_data_type: str = "csv",
    biglake_table: bool = True,
    log_number_of_batches: int = 100,
):
    """
    This task will dump and upload batches of data, sequentially.
    """
    # Log BD version
    bd_version = bd.__version__
    log(f"Using basedosdados@{bd_version}")

    # Keep track of cleared stuff
    prepath = f"data/{uuid4()}/"
    cleared_partitions = set()
    cleared_table = False

    # Get data columns
    columns = database.get_columns()
    log(f"Got columns: {columns}")

    new_query_cols = build_query_new_columns(table_columns=columns)
    log(f"New query columns without accents: {new_query_cols}")

    prepath = Path(prepath)
    log(f"Got prepath: {prepath}")

    if not partition_columns or partition_columns[0] == "":
        partition_column = None
    else:
        partition_column = partition_columns[0]

    if not partition_column:
        log("NO partition column specified! Writing unique files")
    else:
        log(f"Partition column: {partition_column} FOUND!! Write to partitioned files")

    # Now loop until we have no more data.
    batch = database.fetch_batch(batch_size)
    idx = 0
    while len(batch) > 0:
        # Log progress each 100 batches.
        log_mod(
            msg=f"Dumping batch {idx} with size {len(batch)}",
            index=idx,
            mod=log_number_of_batches,
        )

        # Dump batch to file.
        dataframe = batch_to_dataframe(batch, columns)
        old_columns = dataframe.columns.tolist()
        dataframe.columns = remove_columns_accents(dataframe)
        new_columns_dict = dict(zip(old_columns, dataframe.columns.tolist()))
        dataframe = clean_dataframe(dataframe)
        saved_files = []
        if partition_column:
            dataframe, date_partition_columns = parse_date_columns(
                dataframe, new_columns_dict[partition_column]
            )
            partitions = date_partition_columns + [
                new_columns_dict[col] for col in partition_columns[1:]
            ]
            saved_files = to_partitions(
                data=dataframe,
                partition_columns=partitions,
                savepath=prepath,
                data_type=batch_data_type,
                suffix=f"{datetime.now().strftime('%Y%m%d-%H%M%S')}",
            )
        elif batch_data_type == "csv":
            fname = prepath / f"{uuid4()}.csv"
            dataframe_to_csv(dataframe, fname)
            saved_files = [fname]
        elif batch_data_type == "parquet":
            fname = prepath / f"{uuid4()}.parquet"
            dataframe_to_parquet(dataframe, fname)
            saved_files = [fname]
        else:
            raise ValueError(f"Unknown data type: {batch_data_type}")

        # Log progress each 100 batches.

        log_mod(
            msg=f"Batch generated {len(saved_files)} files. Will now upload.",
            index=idx,
            mod=log_number_of_batches,
        )

        # Upload files.
        tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
        table_staging = f"{tb.table_full_name['staging']}"
        st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
        storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
        storage_path_link = (
            f"https://console.cloud.google.com/storage/browser/{st.bucket_name}"
            f"/staging/{dataset_id}/{table_id}"
        )
        dataset_is_public = tb.client["bigquery_prod"].project == "datario"
        # If we have a partition column
        if partition_column:
            # Extract the partition from the filenames
            partitions = []
            for saved_file in saved_files:
                # Remove the prepath and filename. This is the partition.
                partition = str(saved_file).replace(str(prepath), "")
                partition = partition.replace(saved_file.name, "")
                # Strip slashes from beginning and end.
                partition = partition.strip("/")
                # Add to list.
                partitions.append(partition)
            # Remove duplicates.
            partitions = list(set(partitions))
            log_mod(
                msg=f"Got partitions: {partitions}",
                index=idx,
                mod=log_number_of_batches,
            )
            # Loop through partitions and delete files from GCS.
            blobs_to_delete = []
            for partition in partitions:
                if partition not in cleared_partitions:
                    blobs = list_blobs_with_prefix(
                        bucket_name=st.bucket_name,
                        prefix=f"staging/{dataset_id}/{table_id}/{partition}",
                    )
                    blobs_to_delete.extend(blobs)
                cleared_partitions.add(partition)
            if blobs_to_delete:
                delete_blobs_list(bucket_name=st.bucket_name, blobs=blobs_to_delete)
                log_mod(
                    msg=f"Deleted {len(blobs_to_delete)} blobs from GCS: {blobs_to_delete}",
                    index=idx,
                    mod=log_number_of_batches,
                )
        if dump_mode == "append":
            if tb.table_exists(mode="staging"):
                log_mod(
                    msg=(
                        "MODE APPEND: Table ALREADY EXISTS:"
                        + f"\n{table_staging}"
                        + f"\n{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )
            else:
                # the header is needed to create a table when dosen't exist
                log_mod(
                    msg="MODE APPEND: Table DOESN'T EXISTS\nStart to CREATE HEADER file",
                    index=idx,
                    mod=log_number_of_batches,
                )
                header_path = dump_header_to_file(data_path=saved_files[0])
                log_mod(
                    msg="MODE APPEND: Created HEADER file:\n" f"{header_path}",
                    index=idx,
                    mod=log_number_of_batches,
                )

                tb.create(
                    path=header_path,
                    if_storage_data_exists="replace",
                    if_table_exists="replace",
                    biglake_table=biglake_table,
                    dataset_is_public=dataset_is_public,
                )

                log_mod(
                    msg=(
                        "MODE APPEND: Sucessfully CREATED A NEW TABLE:\n"
                        + f"{table_staging}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )  # pylint: disable=C0301

                if not cleared_table:
                    st.delete_table(
                        mode="staging",
                        bucket_name=st.bucket_name,
                        not_found_ok=True,
                    )
                    log_mod(
                        msg=(
                            "MODE APPEND: Sucessfully REMOVED HEADER DATA from Storage:\n"
                            + f"{storage_path}\n"
                            + f"{storage_path_link}"
                        ),
                        index=idx,
                        mod=log_number_of_batches,
                    )  # pylint: disable=C0301
                    cleared_table = True
        elif dump_mode == "overwrite":
            if tb.table_exists(mode="staging") and not cleared_table:
                log_mod(
                    msg=(
                        "MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )  # pylint: disable=C0301
                st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
                log_mod(
                    msg=(
                        "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )  # pylint: disable=C0301
                # delete only staging table and let DBT overwrite the prod table
                tb.delete(mode="staging")
                log_mod(
                    msg=("MODE OVERWRITE: Sucessfully DELETED TABLE:\n" + f"{table_staging}\n"),
                    index=idx,
                    mod=log_number_of_batches,
                )  # pylint: disable=C0301

            if not cleared_table:
                # the header is needed to create a table when dosen't exist
                # in overwrite mode the header is always created
                st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
                log_mod(
                    msg=(
                        "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )  # pylint: disable=C0301

                log_mod(
                    msg="MODE OVERWRITE: Table DOSEN'T EXISTS\nStart to CREATE HEADER file",
                    index=idx,
                    mod=log_number_of_batches,
                )
                header_path = dump_header_to_file(data_path=saved_files[0])
                log_mod(
                    "MODE OVERWRITE: Created HEADER file:\n" f"{header_path}",
                    index=idx,
                    mod=log_number_of_batches,
                )

                tb.create(
                    path=header_path,
                    if_storage_data_exists="replace",
                    if_table_exists="replace",
                    biglake_table=biglake_table,
                    dataset_is_public=dataset_is_public,
                )

                log_mod(
                    msg=(
                        "MODE OVERWRITE: Sucessfully CREATED TABLE\n"
                        + f"{table_staging}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )

                st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
                log_mod(
                    msg=(
                        "MODE OVERWRITE: Sucessfully REMOVED HEADER DATA from Storage\n:"
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )  # pylint: disable=C0301
                cleared_table = True

        log_mod(
            msg="STARTING UPLOAD TO GCS",
            index=idx,
            mod=log_number_of_batches,
        )
        if tb.table_exists(mode="staging"):
            # Upload them all at once
            tb.append(filepath=prepath, if_exists="replace")
            log_mod(
                msg="STEP UPLOAD: Sucessfully uploaded all batch files to Storage",
                index=idx,
                mod=log_number_of_batches,
            )
            for saved_file in saved_files:
                # Delete the files
                saved_file.unlink()
        else:
            # pylint: disable=C0301
            log_mod(
                msg="STEP UPLOAD: Table does not exist in STAGING, need to create first",
                index=idx,
                mod=log_number_of_batches,
            )

        # Get next batch.
        batch = database.fetch_batch(batch_size)
        idx += 1

    log(
        msg=f"Successfully dumped {idx} batches with size {len(batch)}, total of {idx*batch_size}",
    )


@task(
    checkpoint=False,
    max_retries=settings.TASK_MAX_RETRIES_DEFAULT,
    retry_delay=timedelta(seconds=settings.TASK_RETRY_DELAY_DEFAULT),
)
def format_partitioned_query(
    query: str,
    dataset_id: str,
    table_id: str,
    database_type: str,
    partition_columns: List[str] = None,
    lower_bound_date: str = None,
    date_format: str = None,
    wait=None,  # pylint: disable=unused-argument
):
    """
    Formats a query for fetching partitioned data.
    """
    # If no partition column is specified, return the query as is.
    if not partition_columns or partition_columns[0] == "":
        log("NO partition column specified. Returning query as is")
        return query

    partition_column = partition_columns[0]

    # Check if the table already exists in BigQuery.
    table = bd.Table(dataset_id, table_id)

    # If it doesn't, return the query as is, so we can fetch the whole table.
    if not table.table_exists(mode="staging"):
        log("NO tables was found. Returning query as is")
        return query

    blobs = get_storage_blobs(dataset_id, table_id)

    # extract only partitioned folders
    storage_partitions_dict = parse_blobs_to_partition_dict(blobs)
    # get last partition date
    last_partition_date = extract_last_partition_date(storage_partitions_dict, date_format)

    if lower_bound_date == "current_year":
        lower_bound_date = datetime.now().replace(month=1, day=1).strftime("%Y-%m-%d")
        log(f"Using lower_bound_date current_year: {lower_bound_date}")
    elif lower_bound_date == "current_month":
        lower_bound_date = datetime.now().replace(day=1).strftime("%Y-%m-%d")
        log(f"Using lower_bound_date current_month: {lower_bound_date}")
    elif lower_bound_date == "current_day":
        lower_bound_date = datetime.now().strftime("%Y-%m-%d")
        log(f"Using lower_bound_date current_day: {lower_bound_date}")

    if lower_bound_date:
        last_date = min(str(lower_bound_date), str(last_partition_date))
        log(f"Using lower_bound_date: {last_date}")

    else:
        last_date = str(last_partition_date)
        log(f"Using last_date from storage: {last_date}")

    # Using the last partition date, get the partitioned query.
    # `aux_name` must be unique and start with a letter, for better compatibility with
    # multiple DBMSs.
    aux_name = f"a{uuid4().hex}"[:8]

    log(
        f"Partitioned DETECTED: {partition_column}, retuning a NEW QUERY "
        "with partitioned columns and filters"
    )
    if database_type == "oracle":
        oracle_date_format = "YYYY-MM-DD" if date_format == "%Y-%m-%d" else date_format

        return f"""
        with {aux_name} as ({query})
        select * from {aux_name}
        where {partition_column} >= TO_DATE('{last_date}', '{oracle_date_format}')
        """

    return f"""
    with {aux_name} as ({query})
    select * from {aux_name}
    where {partition_column} >= '{last_date}'
    """


@task(checkpoint=False)
def get_connection_string_from_secret(secret_path: str):
    """
    Returns the connection string for the given secret path.
    """
    log(f"Getting connection string for secret path: {secret_path}")
    return get_connection_string_from_secret_function(secret_path)


@task
def get_current_flow_labels() -> List[str]:
    """
    Get the labels of the current flow.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    flow_run_view = FlowRunView.from_flow_run_id(flow_run_id)
    return flow_run_view.labels


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
@assert_dependencies(["requests"], extras=["pipelines-templates"])
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


@task
def get_earth_engine_key_from_secret(
    vault_path_earth_engine_key: str,
):
    """
    Get earth engine service account key from vault.
    """
    log(
        f"Getting Earth Engine key from https://vault.dados.rio/ui/vault/secrets/secret/show/{vault_path_earth_engine_key}"  # noqa
    )
    secret = get_secret(vault_path_earth_engine_key)

    service_account_secret_path = Path("/tmp/earth-engine/key.json")
    service_account_secret_path.parent.mkdir(parents=True, exist_ok=True)

    with open(service_account_secret_path, "w", encoding="utf-8") as f:
        json.dump(secret, f, ensure_ascii=False, indent=4)

    return service_account_secret_path


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


@task(
    max_retries=settings.TASK_MAX_RETRIES_DEFAULT,
    retry_delay=timedelta(seconds=settings.TASK_RETRY_DELAY_DEFAULT),
)
@assert_dependencies(["geojsplit", "geopandas"], extras=["pipelines-templates"])
def transform_geodataframe(
    file_path: str | Path,
    batch_size: int = 50000,
    geometry_column: str = "geometry",
    convert_to_crs_4326: bool = False,
    geometry_3d_to_2d: bool = False,
):  # sourcery skip: convert-to-enumerate
    """ "
    Transform a CSV from data.rio API

    Parameters:
        - file_path (Union[str, Path]): Path to the geojson file to be transformed.
        - batch_size (int): Number of rows to process at once.
        - geometry_column (str): Column containing the geometry data.
        - convert_to_crs_4326 (bool): Convert the geometry data to the crs 4326 projection.
        - geometry_3d_to_2d (bool): Convert the geometry data from 3D to 2D.
    """
    eventid = datetime.now().strftime("%Y%m%d-%H%M%S")

    # move to path file since file_path is path / "geo_data" / "data.geojson"
    save_path = file_path.parent.parent / "csv_data" / f"{eventid}.csv"
    save_path.parent.mkdir(parents=True, exist_ok=True)

    geojson = geojsplit.GeoJSONBatchStreamer(file_path)

    # only print every print_mod batches
    mod = 1000
    count = 1
    for feature_collection in geojson.stream(batch=batch_size):
        geodataframe = gpd.GeoDataFrame.from_features(feature_collection["features"])
        log_mod(
            msg=f"{count} x {batch_size} rows: geodataframe loaded",
            index=count,
            mod=mod,
        )

        # move geometry column to the end
        cols = geodataframe.columns.tolist()
        cols.remove(geometry_column)
        cols.append(geometry_column)
        geodataframe = geodataframe[cols]

        # remove accents from columns
        geodataframe.columns = remove_columns_accents(geodataframe)
        geodataframe["geometry_wkt"] = geodataframe[geometry_column].copy()

        # convert geometry to crs 4326
        if convert_to_crs_4326:
            try:
                geodataframe.crs = "epsg:4326"
                geodataframe[geometry_column] = geodataframe[geometry_column].to_crs("epsg:4326")
            except Exception as err:
                log(f"{count}: error converting to crs 4326: {err}")
                raise err

            log_mod(
                msg=f"{count}: geometry converted to crs 4326",
                index=count,
                mod=mod,
            )

        # convert geometry 3d to 2d
        if geometry_3d_to_2d:
            try:
                geodataframe[geometry_column] = (
                    geodataframe[geometry_column].astype(str).apply(load_wkt)
                )

                geodataframe[geometry_column] = geodataframe[geometry_column].apply(
                    remove_third_dimension
                )
            except Exception as err:
                log(f"{count}: error converting 3d to 2d: {err}")
                raise err

            log_mod(
                msg=f"{count}: geometry converted 3D to 2D",
                index=count,
                mod=mod,
            )

        log_mod(
            msg=f"{count}: new columns: {geodataframe.columns.tolist()}",
            index=count,
            mod=mod,
        )

        # save geodataframe to csv
        geodataframe.to_csv(
            save_path,
            index=False,
            encoding="utf-8",
            mode="a",
            header=not save_path.exists(),
        )

        # clear memory
        del geodataframe

        log_mod(
            msg=f"{count} x {batch_size} rows: Data saved",
            index=count,
            mod=mod,
        )
        count += 1
    log(f"{count} x {batch_size} DATA TRANSFORMED!!!")
    return save_path


@task(nout=2)
def trigger_cron_job(
    project_id: str,
    ee_asset_path: str,
    cron_expression: str,
):
    """
    Tells whether to trigger a cron job.
    """
    redis_client = get_redis_client()
    key = f"{project_id}__{ee_asset_path}"
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
    ee_asset_path: str,
    execution_time: datetime,
):
    """
    Update the last trigger.
    """
    redis_client = get_redis_client()
    key = f"{project_id}__{ee_asset_path}"
    redis_client.set(key, {"last_trigger": execution_time})


@task
def validate_georeference_mode(mode: str) -> None:
    """
    Validates georeference mode
    """
    if mode not in [
        "distinct",
        # insert new modes here
    ]:
        raise ValueError(f"Invalid georeference mode: {mode}. Valid modes are: distinct")


@assert_dependencies(["geojsplit", "geopandas"], extras=["pipelines-templates"])
def georeference_dataframe(new_addresses: pd.DataFrame, log_divider: int = 60) -> pd.DataFrame:
    """
    Georeference all addresses in a dataframe
    """
    start_time = time()

    all_addresses = new_addresses["address"].tolist()
    all_addresses = [f"{address}, Rio de Janeiro" for address in all_addresses]

    geolocator = Nominatim(user_agent="prefeitura-rio")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    log(f"There are {len(all_addresses)} addresses to georeference")

    locations: List[Location] = []
    for i, address in enumerate(all_addresses):
        if i % log_divider == 0:
            log(f"Georeferencing address {i} of {len(all_addresses)}...")
        location = geocode(address)
        locations.append(location)

    geolocated_addresses = [
        {
            "latitude": location.latitude,
            "longitude": location.longitude,
        }
        if location is not None
        else {"latitude": None, "longitude": None}
        for location in locations
    ]

    output = pd.DataFrame(geolocated_addresses)
    output["address"] = new_addresses["address"]
    output[["latitude", "longitude"]] = output.apply(
        lambda x: check_if_belongs_to_rio(x.latitude, x.longitude),
        axis=1,
        result_type="expand",
    )

    log(f"--- {(time() - start_time)} seconds ---")

    return output


@task(nout=2)
@assert_dependencies(["geojsplit", "geopandas"], extras=["pipelines-templates"])
def get_new_addresses(  # pylint: disable=too-many-arguments, too-many-locals
    source_dataset_id: str,
    source_table_id: str,
    source_table_address_column: str,
    destination_dataset_id: str,
    destination_table_id: str,
    georef_mode: str,
    current_flow_labels: List[str],
) -> Tuple[pd.DataFrame, bool]:
    """
    Get new addresses from source table
    """

    new_addresses = pd.DataFrame(columns=["address"])
    exists_new_addresses = False

    source_table_ref = f"{source_dataset_id}.{source_table_id}"
    destination_table_ref = f"{destination_dataset_id}.{destination_table_id}"
    billing_project_id = current_flow_labels[0]

    if georef_mode == "distinct":
        query_source = f"""
        SELECT DISTINCT
            {source_table_address_column}
        FROM
            `{source_table_ref}`
        """

        query_destination = f"""
        SELECT DISTINCT
            address
        FROM
            `{destination_table_ref}`
        """

        source_addresses = bd.read_sql(
            query_source, billing_project_id=billing_project_id, from_file=True
        )
        source_addresses.columns = ["address"]
        try:
            destination_addresses = bd.read_sql(
                query_destination, billing_project_id=billing_project_id, from_file=True
            )
            destination_addresses.columns = ["address"]
        except Exception:  # pylint: disable=broad-except
            destination_addresses = pd.DataFrame(columns=["address"])

        # pylint: disable=invalid-unary-operand-type
        new_addresses = source_addresses[~source_addresses.isin(destination_addresses)].dropna()
        exists_new_addresses = not new_addresses.empty

    return new_addresses, exists_new_addresses
