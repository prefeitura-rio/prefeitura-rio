# -*- coding: utf-8 -*-
from typing import List

try:
    from google.cloud import storage
    from google.cloud.storage.blob import Blob
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["google.cloud.storage"], extras=["pipelines"])

from prefeitura_rio.pipelines_utils.env import get_bd_credentials_from_env
from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode


def get_gcs_client(mode: str = None) -> storage.Client:
    """
    Get a GCS client with the credentials from the environment.
    Mode needs to be "prod" or "staging"

    Args:
        mode (str): The mode to filter by (prod or staging).

    Returns:
        storage.Client: The GCS client.
    """
    if not mode:
        mode = get_flow_run_mode()
    credentials = get_bd_credentials_from_env(mode=mode)
    return storage.Client(credentials=credentials)


def list_blobs_with_prefix(bucket_name: str, prefix: str, mode: str = None) -> List[Blob]:
    """
    Lists all the blobs in the bucket that begin with the prefix.
    This can be used to list all blobs in a "folder", e.g. "public/".
    Mode needs to be "prod" or "staging"

    Args:
        bucket_name (str): The name of the bucket.
        prefix (str): The prefix to filter by.
        mode (str): The mode to filter by (prod or staging).

    Returns:
        List[Blob]: The list of blobs.
    """
    if not mode:
        mode = get_flow_run_mode()
    storage_client = get_gcs_client(mode=mode)
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    return list(blobs)


def parse_blobs_to_partition_list(blobs: List[Blob]) -> List[str]:
    """
    Extracts the partition information from the blobs.
    """
    partitions = []
    for blob in blobs:
        for folder in blob.name.split("/"):
            if "=" in folder:
                key = folder.split("=")[0]
                value = folder.split("=")[1]
                if key == "data_particao":
                    partitions.append(value)
    return partitions
