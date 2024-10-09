# -*- coding: utf-8 -*-
import base64
from typing import Dict


def secret_to_base64(secret_dict: Dict) -> str:
    """
    Converts a dictionary to a JSON-formatted string, encodes it in Base64,
    and returns the Base64-encoded string.

    Args:
    secret_dict (Dict): A dictionary to be encoded.

    Returns:
    str: A Base64-encoded string.
    """
    input_string = str(secret_dict).replace("'", '"')
    bytes_data = input_string.encode("utf-8")
    base64_data = base64.b64encode(bytes_data)
    base64_string = base64_data.decode("utf-8")
    return base64_string


def get_base64_bd_config(projec_id: str) -> str:
    """
    Generates a Base64-encoded configuration string for a project ID,
    formatted for use with a data bucket and Google Cloud configurations.

    Args:
    projec_id (str): The project ID used in the configuration.

    Returns:
    str: A Base64-encoded configuration string.
    """

    string = f"""# What is the bucket that you are saving all the data? It should be
                # an unique name.
                bucket_name = "{projec_id}"

                [gcloud-projects]

                [gcloud-projects.staging]
                    credentials_path = "~/.basedosdados/credentials/staging.json"
                    name = "{projec_id}"

                [gcloud-projects.prod]
                    credentials_path = "~/.basedosdados/credentials/prod.json"
                    name = "{projec_id}"

                [api]
                url = "https://api.dados.rio/api/v1/graphql"
            """.replace(
        "                ", ""
    )

    string_bytes = string.encode("utf-8")

    encoded_string = base64.b64encode(string_bytes).decode("utf-8")

    return encoded_string


def base64_to_string(base64_string: str) -> str:
    """
    Decodes a Base64-encoded string back to a regular string.

    Args:
    base_64 (str): The Base64 string to be decoded.

    Returns:
    str: The decoded string.
    """
    base64_bytes = base64_string.encode("utf-8")
    message_bytes = base64.b64decode(base64_bytes)
    message = message_bytes.decode("utf-8")
    return message
