# -*- coding: utf-8 -*-
import base64


def secret_to_base64(secret_dict):

    input_string = str(secret_dict).replace("'", '"')
    # Encode the string to bytes
    bytes_data = input_string.encode("utf-8")

    # Encode the bytes in Base64
    base64_data = base64.b64encode(bytes_data)

    # Convert the bytes to a string
    base64_string = base64_data.decode("utf-8")

    return base64_string


def get_base64_bd_config(projec_id):
    string = f"""# What is the bucket that you are saving all the data? It should be
                # an unique name.
                bucket_name = {projec_id}

                [gcloud-projects]

                [gcloud-projects.staging]
                    credentials_path = "~/.basedosdados/credentials/staging.json"
                    name = {projec_id}

                [gcloud-projects.prod]
                    credentials_path = "~/.basedosdados/credentials/prod.json"
                    name = {projec_id}

                [api]
                url = "https://api.dados.rio/api/v1/graphql"
            """.replace(
        "                ", ""
    )

    string_bytes = string.encode("utf-8")

    encoded_string = base64.b64encode(string_bytes).decode("utf-8")

    return encoded_string


def base_64_to_string(base_64):
    base_64_bytes = base_64.encode("utf-8")
    message_bytes = base64.b64decode(base_64_bytes)
    message = message_bytes.decode("utf-8")
    return message
