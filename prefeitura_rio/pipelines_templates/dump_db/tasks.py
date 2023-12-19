# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List
from uuid import uuid4

try:
    import basedosdados as bd
    from prefect import task
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["basedosdados", "prefect"], extras=["pipelines"])

from prefeitura_rio.core import settings
from prefeitura_rio.pipelines_utils.bd import get_storage_blobs
from prefeitura_rio.pipelines_utils.gcs import (
    delete_blobs_list,
    list_blobs_with_prefix,
    parse_blobs_to_partition_dict,
)
from prefeitura_rio.pipelines_utils.io import (
    dataframe_to_csv,
    dataframe_to_parquet,
    extract_last_partition_date,
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
from prefeitura_rio.utils import base_assert_dependencies

try:
    from prefeitura_rio.pipelines_utils.database_sql import (
        Database,
        MySql,
        Oracle,
        SqlServer,
    )
except ImportError:
    # TODO: remove this when fix pyodbc Oracle docker configuration.
    # base_assert_dependencies(["cx_Oracle", "pymysql", "pyodbc"], extras=["pipelines-templates"])
    pass


@task(
    checkpoint=False,
    max_retries=settings.TASK_MAX_RETRIES_DEFAULT,
    retry_delay=timedelta(seconds=settings.TASK_RETRY_DELAY_DEFAULT),
)
def database_execute(
    database,
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
    base_assert_dependencies(["cx_Oracle", "pymysql", "pyodbc"], extras=["pipelines-templates"])
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


@task
def dump_upload_batch(
    database,
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