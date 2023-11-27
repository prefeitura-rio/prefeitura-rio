# -*- coding: utf-8 -*-
from datetime import timedelta

try:
    from prefect import Parameter, case
    from prefect.run_configs import KubernetesRun
    from prefect.storage import GCS
    from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["prefect"], extras=["pipelines"])

from prefeitura_rio.core import settings
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.tasks import (
    database_execute,
    database_get,
    dump_upload_batch,
    format_partitioned_query,
    get_current_flow_labels,
    get_current_flow_project_name,
    get_user_and_password,
    parse_comma_separated_string_to_list,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name=settings.FLOW_NAME_DUMP_DB,
) as flow:
    #####################################
    #
    # Parameters
    #
    #####################################

    # DBMS parameters
    hostname = Parameter("db_host")
    port = Parameter("db_port")
    database = Parameter("db_database")
    database_type = Parameter("db_type")
    query = Parameter("execute_query")
    partition_columns = Parameter("partition_columns", required=False, default="")
    partition_date_format = Parameter("partition_date_format", required=False, default="%Y-%m-%d")
    lower_bound_date = Parameter("lower_bound_date", required=False)

    # Materialization parameters
    materialize_after_dump = Parameter("materialize_after_dump", default=False, required=False)
    materialization_mode = Parameter("materialization_mode", default="dev", required=False)
    materialize_to_datario = Parameter("materialize_to_datario", default=False, required=False)

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    dump_to_gcs = Parameter("dump_to_gcs", default=False, required=False)
    maximum_bytes_processed = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=settings.GCS_DUMP_MAX_BYTES_PROCESSED_PER_TABLE,
    )

    # Use Vault for credentials
    secret_path = Parameter("vault_secret_path")

    # Data file parameters
    batch_size = Parameter("batch_size", default=50000, required=False)

    # BigQuery parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    dump_mode = Parameter("dump_mode", default="append", required=False)  # overwrite or append
    batch_data_type = Parameter("batch_data_type", default="csv", required=False)  # csv or parquet
    dbt_model_secret_parameters = Parameter(
        "dbt_model_secret_parameters", default={}, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)
    biglake_table = Parameter("biglake_table", default=False, required=False)
    log_number_of_batches = Parameter("log_number_of_batches", default=100, required=False)

    #####################################
    #
    # Rename flow run
    #
    #####################################
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )
    #####################################
    #
    # Tasks section #0 - Get credentials
    #
    #####################################

    # Get credentials from Vault
    user, password = get_user_and_password(secret_path=secret_path)

    #####################################
    #
    # Tasks section #1 - Create table
    #
    #####################################

    # Get current flow labels
    current_flow_labels = get_current_flow_labels()

    # Parse partition columns
    partition_columns = parse_comma_separated_string_to_list(text=partition_columns)

    # Execute query on SQL Server
    db_object = database_get(
        database_type=database_type,
        hostname=hostname,
        port=port,
        user=user,
        password=password,
        database=database,
    )

    # Format partitioned query if required
    formated_query = format_partitioned_query(
        query=query,
        dataset_id=dataset_id,
        table_id=table_id,
        database_type=database_type,
        partition_columns=partition_columns,
        lower_bound_date=lower_bound_date,
        date_format=partition_date_format,
    )
    formated_query.set_upstream(db_object)

    db_execute = database_execute(  # pylint: disable=invalid-name
        database=db_object,
        query=formated_query,
        flow_name="dump_db",
        labels=current_flow_labels,
        dataset_id=dataset_id,
        table_id=table_id,
    )
    db_execute.set_upstream(formated_query)

    # Dump batches to files
    dump_upload = dump_upload_batch(
        database=db_object,
        batch_size=batch_size,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        partition_columns=partition_columns,
        batch_data_type=batch_data_type,
        biglake_table=biglake_table,
        log_number_of_batches=log_number_of_batches,
    )
    dump_upload.set_upstream(db_execute)

    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        materialization_flow = create_flow_run(
            flow_name=settings.FLOW_NAME_EXECUTE_DBT_MODEL,
            project_name=get_current_flow_project_name(),
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
                "mode": materialization_mode,
                "materialize_to_datario": materialize_to_datario,
                "dbt_model_secret_parameters": dbt_model_secret_parameters,
                "dbt_alias": dbt_alias,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id}",
        )
        materialization_flow.set_upstream(dump_upload)
        wait_for_materialization = wait_for_flow_run(
            materialization_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )
        wait_for_materialization.max_retries = settings.TASK_MAX_RETRIES_DEFAULT
        wait_for_materialization.retry_delay = timedelta(seconds=settings.TASK_RETRY_DELAY_DEFAULT)

        with case(dump_to_gcs, True):
            # Trigger Dump to GCS flow run with project id as datario
            dump_to_gcs_flow = create_flow_run(
                flow_name=settings.FLOW_NAME_DUMP_TO_GCS,
                project_name=get_current_flow_project_name(),
                parameters={
                    "project_id": "datario",
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "maximum_bytes_processed": maximum_bytes_processed,
                },
                labels=[
                    "datario",
                ],
                run_name=f"Dump to GCS {dataset_id}.{table_id}",
            )
            dump_to_gcs_flow.set_upstream(wait_for_materialization)

            wait_for_dump_to_gcs = wait_for_flow_run(
                dump_to_gcs_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )


flow.storage = GCS("<REPLACE_ME_WHEN_USING")
flow.run_config = KubernetesRun(image="<REPLACE_ME_WHEN_USING>")
