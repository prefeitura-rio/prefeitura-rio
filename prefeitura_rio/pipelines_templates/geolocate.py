# -*- coding: utf-8 -*-
# pylint: disable=invalid-name
"""
Flow for georeferencing tables
"""

from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from prefeitura_rio.core import settings
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.tasks import (
    create_table_and_upload_to_gcs,
    dataframe_to_csv,
    georeference_dataframe,
    get_current_flow_labels,
    get_new_addresses,
    validate_georeference_mode,
)

with Flow(
    settings.FLOW_NAME_GEOLOCATE,
) as utils_geolocate_flow:
    ################################
    #
    # Parameters
    #
    ################################

    # Table parameters
    source_dataset_id = Parameter("source_dataset_id")
    source_table_id = Parameter("source_table_id")
    source_table_address_column = Parameter("source_table_address_column")
    destination_dataset_id = Parameter("destination_dataset_id")
    destination_table_id = Parameter("destination_table_id")

    # Georeference parameters
    georeference_mode = Parameter("georeference_mode", default="distinct", required=False)

    # Materialization parameters
    materialize = Parameter("materialize", default=False, required=False)
    materialization_mode = Parameter("materialization_mode", default="dev", required=False)
    materialize_to_datario = Parameter("materialize_to_datario", default=False, required=False)

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    dump_to_gcs = Parameter("dump_to_gcs", default=False, required=False)
    maximum_bytes_processed = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=settings.MAX_BYTES_PROCESSED_PER_TABLE,
    )
    biglake_table = Parameter("biglake_table", default=False, required=False)

    # Validate the georeference mode
    georef_mode_valid = validate_georeference_mode(mode=georeference_mode)

    # Get agent labels
    current_flow_labels = get_current_flow_labels()
    current_flow_labels.set_upstream(georef_mode_valid)

    # Checks if there are new addresses
    new_addresses, exists_new_addresses = get_new_addresses(
        source_dataset_id=source_dataset_id,
        source_table_id=source_table_id,
        source_table_address_column=source_table_address_column,
        destination_dataset_id=destination_dataset_id,
        destination_table_id=destination_table_id,
        georef_mode=georeference_mode,
        current_flow_labels=current_flow_labels,
    )

    with case(exists_new_addresses, True):
        # Georeference the table
        georeferenced_table = georeference_dataframe(new_addresses=new_addresses)
        base_path = dataframe_to_csv(dataframe=georeferenced_table)
        create_staging_table = create_table_and_upload_to_gcs(
            data_path=base_path,
            dataset_id=destination_dataset_id,
            table_id=destination_table_id,
            biglake_table=biglake_table,
            dump_mode="append",
        )

        with case(materialize, True):
            # Trigger DBT flow run
            materialization_flow = create_flow_run(
                flow_name=settings.FLOW_EXECUTE_DBT_MODEL_NAME,
                project_name=settings.PREFECT_DEFAULT_PROJECT,
                parameters={
                    "dataset_id": destination_dataset_id,
                    "table_id": destination_table_id,
                    "mode": materialization_mode,
                    "materialize_to_datario": materialize_to_datario,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {destination_dataset_id}.{destination_table_id}",
            )
            materialization_flow.set_upstream(create_staging_table)

            wait_for_materialization = wait_for_flow_run(
                materialization_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )

            wait_for_materialization.max_retries = settings.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS
            wait_for_materialization.retry_delay = timedelta(
                seconds=settings.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL
            )

            with case(dump_to_gcs, True):
                # Trigger Dump to GCS flow run with project id as datario
                dump_to_gcs_flow = create_flow_run(
                    flow_name=settings.FLOW_DUMP_TO_GCS_NAME,
                    project_name=settings.PREFECT_DEFAULT_PROJECT,
                    parameters={
                        "project_id": "datario",
                        "dataset_id": destination_dataset_id,
                        "table_id": destination_table_id,
                        "maximum_bytes_processed": maximum_bytes_processed,
                    },
                    labels=[
                        "datario",
                    ],
                    run_name=f"Dump to GCS {destination_dataset_id}.{destination_table_id}",
                )
                dump_to_gcs_flow.set_upstream(wait_for_materialization)

                wait_for_dump_to_gcs = wait_for_flow_run(
                    dump_to_gcs_flow,
                    stream_states=True,
                    stream_logs=True,
                    raise_final_state=True,
                )


utils_geolocate_flow.storage = GCS("<REPLACE_ME_WHEN_USING")
utils_geolocate_flow.run_config = KubernetesRun(
    image="<REPLACE_ME_WHEN_USING",
)
