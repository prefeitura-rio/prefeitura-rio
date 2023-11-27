# -*- coding: utf-8 -*-
from datetime import timedelta
from uuid import uuid4

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
from prefeitura_rio.pipelines_utils.tasks import create_table_and_upload_to_gcs
from prefeitura_rio.pipelines_utils.tasks import database_get_mongo as database_get
from prefeitura_rio.pipelines_utils.tasks import (
    dump_batches_to_file,
    get_connection_string_from_secret,
    get_current_flow_labels,
    get_current_flow_project_name,
    greater_than,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name=settings.FLOW_NAME_DUMP_MONGODB,
    parallelism=3,
) as flow:
    #####################################
    #
    # Parameters
    #
    #####################################

    # DBMS
    db_host = Parameter("db_host")
    db_port = Parameter("db_port")
    db_database = Parameter("db_database")
    db_collection = Parameter("db_collection")
    db_connection_string_secret_path = Parameter("db_connection_string_secret_path")

    # Filtering and partitioning
    date_field = Parameter("date_field", required=False)
    date_format = Parameter("date_format", default="%Y-%m-%d", required=False)
    date_lower_bound = Parameter("date_lower_bound", required=False)

    # Dumping to files
    dump_batch_size = Parameter("dump_batch_size", default=50000, required=False)

    # Uploading to BigQuery
    bq_dataset_id = Parameter("bq_dataset_id")
    bq_table_id = Parameter("bq_table_id")
    bq_upload_mode = Parameter("bq_upload_mode", default="append", required=False)
    bq_biglake_table = Parameter("bq_biglake_table", default=False, required=False)
    bq_batch_data_type = Parameter(
        "bq_batch_data_type", default="csv", required=False
    )  # csv or parquet

    # Materialization
    materialize_after_dump = Parameter("materialize_after_dump", default=False, required=False)
    materialize_mode = Parameter("materialize_mode", default="dev", required=False)  # dev or prod
    materialize_to_datario = Parameter("materialize_to_datario", default=False, required=False)
    materialize_dbt_model_secret_parameters = Parameter(
        "materialize_dbt_model_secret_parameters", default={}, required=False
    )
    materialize_dbt_alias = Parameter("materialize_dbt_alias", default=False, required=False)

    # Dumping to GCS
    gcs_dump = Parameter("gcs_dump", default=False, required=False)
    gcs_maximum_bytes_processed = Parameter(
        "gcs_maximum_bytes_processed",
        required=False,
        default=settings.GCS_DUMP_MAX_BYTES_PROCESSED_PER_TABLE,
    )

    #####################################
    #
    # Rename flow run
    #
    #####################################
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=bq_dataset_id, table_id=bq_table_id
    )

    #####################################
    #
    # Tasks section #0 - Get credentials
    #
    #####################################

    # Get credentials from Vault
    connection_string = get_connection_string_from_secret(
        secret_path=db_connection_string_secret_path
    )

    #####################################
    #
    # Tasks section #1 - Create table
    #
    #####################################

    # Get current flow labels
    current_flow_labels = get_current_flow_labels()

    # Execute query on SQL Server
    db_object = database_get(
        connection_string=connection_string,
        database=db_database,
        collection=db_collection,
    )

    # Dump batches to files
    batches_path, num_batches = dump_batches_to_file(
        database=db_object,
        batch_size=dump_batch_size,
        prepath=f"data/{uuid4()}/",
        date_field=date_field,
        date_lower_bound=date_lower_bound,
        date_format=date_format,
        batch_data_type=bq_batch_data_type,
    )

    data_exists = greater_than(num_batches, 0)

    with case(data_exists, True):
        upload_table = create_table_and_upload_to_gcs(
            data_path=batches_path,
            dataset_id=bq_dataset_id,
            table_id=bq_table_id,
            dump_mode=bq_upload_mode,
            biglake_table=bq_biglake_table,
        )

        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            materialization_flow = create_flow_run(
                flow_name=settings.FLOW_NAME_EXECUTE_DBT_MODEL,
                project_name=get_current_flow_project_name(),
                parameters={
                    "dataset_id": bq_dataset_id,
                    "table_id": bq_table_id,
                    "mode": materialize_mode,
                    "materialize_to_datario": materialize_to_datario,
                    "dbt_model_secret_parameters": materialize_dbt_model_secret_parameters,
                    "dbt_alias": materialize_dbt_alias,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {bq_dataset_id}.{bq_table_id}",
            )
            materialization_flow.set_upstream(upload_table)
            wait_for_materialization = wait_for_flow_run(
                materialization_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )
            wait_for_materialization.max_retries = settings.TASK_MAX_RETRIES_DEFAULT
            wait_for_materialization.retry_delay = timedelta(
                seconds=settings.TASK_RETRY_DELAY_DEFAULT
            )

            with case(gcs_dump, True):
                # Trigger Dump to GCS flow run with project id as datario
                dump_to_gcs_flow = create_flow_run(
                    flow_name=settings.FLOW_NAME_DUMP_TO_GCS,
                    project_name=get_current_flow_project_name(),
                    parameters={
                        "project_id": "datario",
                        "dataset_id": bq_dataset_id,
                        "table_id": bq_table_id,
                        "maximum_bytes_processed": gcs_maximum_bytes_processed,
                    },
                    labels=[
                        "datario",
                    ],
                    run_name=f"Dump to GCS {bq_dataset_id}.{bq_table_id}",
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
