# -*- coding: utf-8 -*-
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
from prefeitura_rio.pipelines_utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    get_current_flow_project_name,
    get_datario_geodataframe,
    rename_current_flow_run_dataset_table,
    transform_geodataframe,
)

with Flow(name=settings.FLOW_NAME_DUMP_DATARIO) as flow:
    #####################################
    #
    # Parameters
    #
    #####################################

    # Datario
    url = Parameter("url")
    geometry_column = Parameter("geometry_column", default="geometry", required=False)
    convert_to_crs_4326 = Parameter("convert_to_crs_4326", default=False, required=False)
    geometry_3d_to_2d = Parameter("geometry_3d_to_2d", default=False, required=False)
    batch_size = Parameter("batch_size", default=100, required=False)

    # BigQuery parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    # overwrite or append
    dump_mode = Parameter("dump_mode", default="overwrite")
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
    biglake_table = Parameter("biglake_table", default=False, required=False)
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
    # Tasks section #1 - Create table
    #
    #####################################

    file_path = get_datario_geodataframe(
        url=url,
        path=f"data/{uuid4()}/",
    )
    file_path.set_upstream(rename_flow_run)

    datario_path = transform_geodataframe(
        file_path=file_path,
        batch_size=batch_size,
        geometry_column=geometry_column,
        convert_to_crs_4326=convert_to_crs_4326,
        geometry_3d_to_2d=geometry_3d_to_2d,
    )
    datario_path.set_upstream(file_path)

    CREATE_TABLE_AND_UPLOAD_TO_GCS_DONE = create_table_and_upload_to_gcs(
        data_path=datario_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )
    CREATE_TABLE_AND_UPLOAD_TO_GCS_DONE.set_upstream(datario_path)

    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=settings.FLOW_NAME_EXECUTE_DBT_MODEL,
            project_name=get_current_flow_project_name(),
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
                "mode": materialization_mode,
                "materialize_to_datario": materialize_to_datario,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id}",
        )
        materialization_flow.set_upstream(CREATE_TABLE_AND_UPLOAD_TO_GCS_DONE)

        wait_for_materialization = wait_for_flow_run(
            materialization_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

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
