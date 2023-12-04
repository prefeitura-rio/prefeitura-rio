# -*- coding: utf-8 -*-
from os import getenv
from typing import Union

from loguru import logger


def getenv_or_action(key: str, default: str = None, action: str = "raise") -> Union[str, None]:
    """
    Gets an environment variable or executes an action.

    Args:
        key (str): The environment variable key.
        default (str, optional): The default value. Defaults to `None`.
        action (str, optional): The action to execute. Must be one of `'raise'`,
            `'warn'` or `'ignore'`. Defaults to `'raise'`.

    Returns:
        Union[str, None]: The environment variable value or the default value.
    """
    if action not in ("raise", "warn", "ignore"):
        raise ValueError(f"Invalid action '{action}'. Must be one of 'raise', 'warn' or 'ignore'.")
    value = getenv(key, default)
    if value is None:
        if action == "raise":
            raise ValueError(f"Environment variable '{key}' not found.")
        elif action == "warn":
            logger.warning(f"Environment variable '{key}' not found.")
    return value


class Settings:
    __slots__ = ()

    @property
    def SGRC_URL(self) -> str:
        url = getenv_or_action("SGRC_URL", action="raise")
        if url.endswith("/"):
            url = url[:-1]
        return url

    @property
    def SGRC_URL_NEW_TICKET(self) -> str:
        return self.SGRC_URL + "/api/WSPortalWeb/NovoChamado"

    @property
    def SGRC_URL_GET_PROTOCOLS(self) -> str:
        return self.SGRC_URL + "/api/WSPortalWeb/RetornaProtocolos"

    @property
    def SGRC_URL_GET_PROTOCOL_TICKETS(self) -> str:
        return self.SGRC_URL + "/api/WSPortalWeb/RetornaChamadosProtocolo"

    @property
    def SGRC_URL_GET_TICKET_DETAILS(self) -> str:
        return self.SGRC_URL + "/api/WSPortalWeb/RetornaDetalhesChamado"

    @property
    def SGRC_URL_GET_PROTOCOL_TICKETS_DETAILS(self) -> str:
        return self.SGRC_URL + "/api/WSPortalWeb/RetornaChamadosDetalhadosProtocolo"

    @property
    def SGRC_URL_VERSION(self) -> str:
        return self.SGRC_URL + "/api/WSPortalWeb/VersaoDoServico"

    @property
    def SGRC_AUTHORIZATION_HEADER(self) -> str:
        return getenv_or_action("SGRC_AUTHORIZATION_HEADER", action="raise")

    @property
    def SGRC_BODY_TOKEN(self) -> str:
        return getenv_or_action("SGRC_BODY_TOKEN", action="raise")

    @property
    def FLOW_NAME_DUMP_DATARIO(self) -> str:
        return "[TEMPLATE] Ingerir tabela do data.rio"

    @property
    def FLOW_NAME_DUMP_DB(self) -> str:
        return "[TEMPLATE] Ingerir tabela do banco de dados"

    @property
    def FLOW_NAME_DUMP_EARTH_ENGINE(self) -> str:
        return "[TEMPLATE] Ingerir tabela do Google Earth Engine"

    @property
    def FLOW_NAME_DUMP_MONGODB(self) -> str:
        return "[TEMPLATE] Ingerir tabela do MongoDB"

    @property
    def FLOW_NAME_DUMP_TO_GCS(self) -> str:
        return "[TEMPLATE] Fazer dump de tabela para o GCS"

    @property
    def FLOW_NAME_EXECUTE_DBT_MODEL(self) -> str:
        return "[TEMPLATE] Executar modelo DBT"

    @property
    def FLOW_NAME_GEOLOCATE(self) -> str:
        return "[TEMPLATE] Geolocalizar tabela"

    @property
    def GCS_DUMP_MAX_BYTES_PROCESSED_PER_TABLE(self) -> int:
        return 5 * 1024 * 1024 * 1024  # 5GB

    @property
    def TASK_MAX_RETRIES_DEFAULT(self) -> int:
        return 3

    @property
    def TASK_RETRY_DELAY_DEFAULT(self) -> int:
        return 30

    @property
    def WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS(self) -> int:
        return 3

    @property
    def WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL(self) -> int:
        return 5

    @property
    def PREFECT_DEFAULT_PROJECT(self) -> str:
        return "main"


settings = Settings()
