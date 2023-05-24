# -*- coding: utf-8 -*-
from typing import Any, Dict, List

from loguru import logger
import requests
from requests.adapters import HTTPAdapter, Retry

from prefeitura_rio import settings


def add_token_to_body(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Adds the token to the body.

    Args:
        data (Dict[str, Any]): The body.

    Returns:
        Dict[str, Any]: The body with the token.
    """
    return {**data, "token": settings.SGRC_BODY_TOKEN}


def get_headers() -> Dict[str, str]:
    """
    Get the headers for each request to SGRC.

    Returns:
        Dict[str, str]: The headers.
    """
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": settings.SGRC_AUTHORIZATION_HEADER,
    }


def post(
    url: str,
    data: Dict[str, Any],
    add_token: bool = True,
    retry_attempts: int = 5,
    retry_backoff_factor: float = 1,
    retry_status_forcelist: List[int] = [502, 503, 504],
) -> Dict[str, Any]:
    """
    Makes a POST request to SGRC.

    Args:
        url (str): The URL.
        data (Dict[str, Any]): The body.
        add_token (bool, optional): Whether to add the token to the body. Defaults to `True`.
        retry_attempts (int, optional): The number of retry attempts. Defaults to `5`.
        retry_backoff_factor (float, optional): The backoff factor. Defaults to `1`.
        retry_status_forcelist (List[int], optional): The status codes to retry.

    Returns:
        requests.Response: The response.
    """
    session = requests.Session()
    retries = Retry(
        total=retry_attempts,
        backoff_factor=retry_backoff_factor,
        status_forcelist=retry_status_forcelist,
    )
    session.mount("http://", HTTPAdapter(max_retries=retries))
    if add_token:
        data = add_token_to_body(data)
    headers = get_headers()
    logger.debug(f"Making POST request to {url} with data {data} and headers {headers}")
    response = session.post(url, json=data, headers=headers)
    response.raise_for_status()
    return response.json()
