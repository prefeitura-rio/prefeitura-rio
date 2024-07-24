# -*- coding: utf-8 -*-
from time import sleep, time
from typing import List, Tuple

try:
    import basedosdados as bd
    import pandas as pd
    from basedosdados import Base
    from prefect import task
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["basedosdados", "prefect"], extras=["pipelines"])

from prefeitura_rio.pipelines_utils.geo import Geolocator
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.utils import base_assert_dependencies


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


@task
def georeference_dataframe(
    new_addresses: pd.DataFrame,
    address_column="address",
    log_divider: int = 1,
    language="pt",
    timeout=10,
    viewbox=None,
    sulfix=None,
    retry_request_number=1,
    retry_request_time=60,
    time_between_requests=1,
) -> pd.DataFrame:
    """
    Georeference all addresses in a dataframe
    """
    start_time = time()

    all_addresses = new_addresses[address_column].tolist()
    all_addresses = [f"{address}{sulfix}" if sulfix else address for address in all_addresses]
    log(f"There are {len(all_addresses)} addresses to georeference")

    geolocate_address = Geolocator()
    latitudes = []
    longitudes = []
    for i, address in enumerate(all_addresses):
        if i % log_divider == 0:
            log(f"Georeferencing address {i} of {len(all_addresses)}...")

        # retry request
        for _ in range(retry_request_number):
            try:
                latitude, longitude = geolocate_address.geopy_nominatim(
                    address=address,
                    language=language,
                    timeout=timeout,
                    viewbox=viewbox,
                )
                break
            except Exception as err:
                log(f"Error georeferencing address {address}: {err}")
                log(f"Waiting {retry_request_time} seconds before retrying...")
                sleep(retry_request_time)
                continue
        sleep(time_between_requests)
        latitudes.append(latitude)
        longitudes.append(longitude)
    new_addresses["latitude"] = latitudes
    new_addresses["longitude"] = longitudes
    log(f"--- {(time() - start_time)} seconds ---")
    cols = [address_column, "latitude", "longitude"]
    return new_addresses[cols]


@task(nout=2)
def get_new_addresses(  # pylint: disable=too-many-arguments, too-many-locals
    source_dataset_id: str,
    source_table_id: str,
    source_table_address_column: str,
    source_table_address_query: str,
    use_source_table_address_query: bool,
    destination_dataset_id: str,
    destination_table_id: str,
    georef_mode: str,
    current_flow_labels: List[str],
) -> Tuple[pd.DataFrame, bool]:
    """
    Get new addresses from source table
    """
    base_assert_dependencies(["geojsplit", "geopandas"], extras=["pipelines-templates"])

    new_addresses = pd.DataFrame(columns=["address"])
    exists_new_addresses = False

    billing_project_id = Base().config["gcloud-projects"]["prod"]["name"]
    source_table_ref = f"{billing_project_id}.{source_dataset_id}.{source_table_id}"
    destination_table_ref = f"{billing_project_id}.{destination_dataset_id}.{destination_table_id}"
    if georef_mode == "distinct":
        if use_source_table_address_query:
            query_source = source_table_address_query
        else:
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
        log(f"Source query: {query_source}")
        log(f"Destination query: {query_destination}")
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
        log(f"Source lengh: {len(source_addresses)}")
        log(f"Destination lengh: {len(destination_addresses)}")
        # pylint: disable=invalid-unary-operand-type
        mask = source_addresses["address"].isin(destination_addresses["address"].tolist())
        new_addresses = source_addresses[~mask].dropna()
        log(f"new_addresses lengh: {len(new_addresses)}")

        exists_new_addresses = not new_addresses.empty

    return new_addresses, exists_new_addresses
