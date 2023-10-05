# -*- coding: utf-8 -*-
from datetime import datetime
from typing import Any, Dict, List, Union

try:
    import pendulum
    import pytz
except ImportError:
    pass

from prefeitura_rio import settings
from prefeitura_rio.integrations.sgrc.exceptions import (
    BaseSGRCException,
    SGRCBusinessRuleException,
    SGRCDuplicateTicketException,
    SGRCEquivalentTicketException,
    SGRCInternalErrorException,
    SGRCInvalidBodyException,
    SGRCMalformedBodyException,
)
from prefeitura_rio.integrations.sgrc.models import (
    Address,
    Classification,
    Diagnosis,
    NewTicket,
    Progress,
    Reclassification,
    Requester,
    Ticket,
    TicketDetails,
    TicketSummary,
)
from prefeitura_rio.integrations.sgrc.utils import apost, post
from prefeitura_rio.utils import assert_dependencies


def pre_new_ticket(
    classification_code: str,
    description: str,
    address: Address = None,
    date_time: Union[datetime, str] = None,
    requester: Requester = None,
    occurrence_origin_code: str = "28",
    specific_attributes: Dict[str, Any] = None,
) -> dict:
    if not isinstance(classification_code, str):
        try:
            classification_code = str(classification_code)
        except Exception:
            raise ValueError(
                "'classification_code' must be a string or an object that implements the __str__ method."  # noqa
            )
    if not isinstance(description, str):
        try:
            description = str(description)
        except Exception:
            raise ValueError(
                "'description' must be a string or an object that implements the __str__ method."  # noqa
            )
    if address and not isinstance(address, Address):
        raise ValueError("'address' must be an Address object.")
    if date_time is None:
        date_time = pendulum.now(tz=pytz.timezone("America/Sao_Paulo")).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
    elif isinstance(date_time, datetime):
        date_time = date_time.strftime("%Y-%m-%dT%H:%M:%S")
    elif not isinstance(date_time, str):
        raise ValueError(
            "'date_time' must be a datetime object, a string or an object that implements the __str__ method."  # noqa
        )
    if requester is None:
        requester = Requester()
    elif not isinstance(requester, Requester):
        raise ValueError("'requester' must be a Requester object.")
    if occurrence_origin_code is None:
        occurrence_origin_code = ""
    data = {
        "codigoClassificacao": classification_code,
        "dataHora": date_time,
        "descricao": description,
        "solicitantePortalWeb": requester.to_dict(),
        "codigoOrigemOcorrencia": occurrence_origin_code,
    }
    if address:
        data["endereco"] = address.to_dict()
    if specific_attributes is not None:
        parsed_specific_attributes = []
        for key, value in specific_attributes.items():
            parsed_specific_attributes.append(
                {
                    "nomeAtributoEspecifico": key,
                    "valorAtributoEspecifico": value,
                }
            )
        data["atributosEspecificos"] = parsed_specific_attributes
    return data


def after_new_ticket(response: Dict[str, Any]) -> NewTicket:
    if "codigo" not in response or "descricao" not in response:
        raise BaseSGRCException(
            "Unexpected response from SGRC. 'codigo' or 'descricao' key is missing."
        )

    code = response["codigo"]
    try:
        code = int(code)
    except Exception as exc:  # noqa
        raise BaseSGRCException(f"Unexpected return code '{code}' from SGRC.") from exc

    if code == 1:
        if "protocolo" not in response or "codigoChamado" not in response:
            raise BaseSGRCException(
                "Unexpected response from SGRC. 'protocolo' or 'codigoChamado' key is missing."
            )
        return NewTicket(
            protocol_id=response["protocolo"],
            ticket_id=response["codigoChamado"],
        )
    elif code == 2:
        raise SGRCMalformedBodyException(response["descricao"])
    elif code == 3:
        raise SGRCInvalidBodyException(response["descricao"])
    elif code == 4:
        raise SGRCBusinessRuleException(response["descricao"])
    elif code == 5:
        raise SGRCInternalErrorException(response["descricao"])
    elif code == 6:
        raise SGRCEquivalentTicketException(response["descricao"])
    elif code == 7:
        raise SGRCDuplicateTicketException(response["descricao"])


@assert_dependencies(["pendulum", "pytz"], extras=["sgrc"])
def new_ticket(
    classification_code: str,
    description: str,
    address: Address = None,
    date_time: Union[datetime, str] = None,
    requester: Requester = None,
    occurrence_origin_code: str = "28",
    specific_attributes: Dict[str, Any] = None,
) -> NewTicket:
    """
    Creates a new ticket.

    Args:
        classification_code (str): The classification code.
        description (str): The description of the occurrence.
        address (Address, optional): The address of the occurrence.
        date_time (Union[datetime, str], optional): The date and time of the occurrence. When
            converted to string, it must be in the following format: "%Y-%m-%dT%H:%M:%S". Defaults
            to `None`, which will be replaced by the current date and time.
        requester (Requester, optional): The requester information. Defaults to `None`, which will
            be replaced by an empty `Requester` object.
        occurrence_origin_code (str, optional): The occurrence origin code (e.g. "13" for
            "Web App"). Defaults to "28".

    Returns:
        NewTicket: The new ticket.

    Raises:
        BaseSGRCException: If an unexpected exception occurs.
        SGRCBusinessRuleException: If the request violates a business rule.
        SGRCDuplicateTicketException: If the request tries to create a duplicate ticket.
        SGRCEquivalentTicketException: If the request tries to create an equivalent ticket.
        SGRCInternalErrorException: If the request causes an internal error.
        SGRCInvalidBodyException: If the request body is invalid.
        SGRCMalformedBodyException: If the request body is malformed.
        ValueError: If any of the arguments is invalid.
    """
    data = pre_new_ticket(
        classification_code=classification_code,
        description=description,
        address=address,
        date_time=date_time,
        requester=requester,
        occurrence_origin_code=occurrence_origin_code,
        specific_attributes=specific_attributes,
    )
    try:
        response = post(settings.SGRC_URL_NEW_TICKET, data)
    except Exception as exc:
        raise BaseSGRCException("Unexpected exception when trying to create a new ticket.") from exc
    return after_new_ticket(response)


@assert_dependencies(["pendulum", "pytz"], extras=["sgrc"])
async def async_new_ticket(
    classification_code: str,
    description: str,
    address: Address = None,
    date_time: Union[datetime, str] = None,
    requester: Requester = None,
    occurrence_origin_code: str = "28",
    specific_attributes: Dict[str, Any] = None,
) -> NewTicket:
    data = pre_new_ticket(
        classification_code=classification_code,
        description=description,
        address=address,
        date_time=date_time,
        requester=requester,
        occurrence_origin_code=occurrence_origin_code,
        specific_attributes=specific_attributes,
    )
    try:
        response = await apost(settings.SGRC_URL_NEW_TICKET, data)
    except Exception as exc:
        raise BaseSGRCException("Unexpected exception when trying to create a new ticket.") from exc
    return after_new_ticket(response)


def get_protocols() -> List[str]:
    raise NotImplementedError()


async def async_get_protocols() -> List[str]:
    raise NotImplementedError()


def pre_get_protocol_tickets(protocol_id: str) -> dict:
    if not isinstance(protocol_id, str):
        try:
            protocol_id = str(protocol_id)
        except Exception:
            raise ValueError(
                "'protocol_id' must be a string or an object that implements the __str__ method."
            )
    data = {"codigo": protocol_id}
    return data


def after_get_protocol_tickets(response: Dict[str, Any]) -> List[TicketSummary]:
    if "codigo" not in response or "descricao" not in response:
        raise BaseSGRCException(
            "Unexpected response from SGRC. 'codigo' or 'descricao' key is missing."
        )

    code = response["codigo"]
    try:
        code = int(code)
    except Exception as exc:  # noqa
        raise BaseSGRCException(f"Unexpected return code '{code}' from SGRC.") from exc

    if code == 1:
        if "chamados" not in response:
            raise BaseSGRCException("Unexpected response from SGRC. 'chamados' key is missing")
        chamados: List[TicketSummary] = []
        for ticket_dict in response["chamados"]:
            chamado = TicketSummary(
                ticket_id=ticket_dict["codigoChamado"],
                category=ticket_dict["categoria"],
                type=ticket_dict["tipo"],
                subtype=ticket_dict["subTipo"],
                description=ticket_dict["descricao"],
                status=ticket_dict["status"],
            )
            chamados.append(chamado)
        return chamados
    elif code == 2:
        raise SGRCMalformedBodyException(response["descricao"])
    elif code == 3:
        raise SGRCInvalidBodyException(response["descricao"])
    elif code == 4:
        raise SGRCBusinessRuleException(response["descricao"])
    elif code == 5:
        raise SGRCInternalErrorException(response["descricao"])
    elif code == 6:
        raise SGRCEquivalentTicketException(response["descricao"])
    elif code == 7:
        raise SGRCDuplicateTicketException(response["descricao"])


def get_protocol_tickets(protocol_id: str) -> List[TicketSummary]:
    """
    Get the tickets of a protocol.

    Args:
        protocol_id (str): The protocol ID.

    Returns:
        List[TicketSummary]: The tickets of the protocol.

    Raises:
        BaseSGRCException: If an unexpected exception occurs.
        SGRCBusinessRuleException: If the request violates a business rule.
        SGRCInternalErrorException: If the request causes an internal error.
        SGRCInvalidBodyException: If the request body is invalid.
        SGRCMalformedBodyException: If the request body is malformed.
        ValueError: If any of the arguments is invalid.
    """
    data = pre_get_protocol_tickets(protocol_id)
    try:
        response = post(settings.SGRC_URL_GET_PROTOCOL_TICKETS, data)
    except Exception as exc:
        raise BaseSGRCException("Unexpected exception when trying to create a new ticket.") from exc
    return after_get_protocol_tickets(response)


async def async_get_protocol_tickets(protocol_id: str) -> List[TicketSummary]:
    """
    Get the tickets of a protocol.

    Args:
        protocol_id (str): The protocol ID.

    Returns:
        List[TicketSummary]: The tickets of the protocol.

    Raises:
        BaseSGRCException: If an unexpected exception occurs.
        SGRCBusinessRuleException: If the request violates a business rule.
        SGRCInternalErrorException: If the request causes an internal error.
        SGRCInvalidBodyException: If the request body is invalid.
        SGRCMalformedBodyException: If the request body is malformed.
        ValueError: If any of the arguments is invalid.
    """
    data = pre_get_protocol_tickets(protocol_id)
    try:
        response = await apost(settings.SGRC_URL_GET_PROTOCOL_TICKETS, data)
    except Exception as exc:
        raise BaseSGRCException("Unexpected exception when trying to create a new ticket.") from exc
    return after_get_protocol_tickets(response)


def pre_get_ticket_details(ticket_id: str) -> dict:
    if not isinstance(ticket_id, str):
        try:
            ticket_id = str(ticket_id)
        except Exception:
            raise ValueError(
                "'ticket_id' must be a string or an object that implements the __str__ method."
            )
    data = {"codigo": ticket_id}
    return data


def after_get_ticket_details(response: Dict[str, Any]) -> Ticket:
    if "codigo" not in response or "descricao" not in response:
        raise BaseSGRCException(
            "Unexpected response from SGRC. 'codigo' or 'descricao' key is missing."
        )

    code = response["codigo"]
    try:
        code = int(code)
    except Exception as exc:  # noqa
        raise BaseSGRCException(f"Unexpected return code '{code}' from SGRC.") from exc

    if code == 1:
        if "chamado" not in response:
            raise BaseSGRCException("Unexpected response from SGRC. 'chamado' key is missing")
        ticket_dict = response["chamado"]
        classification = Classification(
            category_code=ticket_dict["classificacao"]["codigoCategoria"],
            category=ticket_dict["classificacao"]["categoria"],
            type_code=ticket_dict["classificacao"]["codigoTipo"],
            type=ticket_dict["classificacao"]["tipo"],
            subtype_code=ticket_dict["classificacao"]["codigoSubtipo"],
            subtype=ticket_dict["classificacao"]["subtipo"],
        )
        address = Address(
            locality=ticket_dict["endereco"]["localidade"],
            street_code=ticket_dict["endereco"]["codigoLogradouro"],
            street=ticket_dict["endereco"]["logradouro"],
            number=ticket_dict["endereco"]["numero"],
            neighborhood_code=ticket_dict["endereco"]["codigoBairro"],
            neighborhood=ticket_dict["endereco"]["bairro"],
            complement=ticket_dict["endereco"]["complemento"],
            zip_code=ticket_dict["endereco"]["cep"],
            x_coordinate=ticket_dict["endereco"]["coordenadaX"],
            y_coordinate=ticket_dict["endereco"]["coordenadaY"],
            photo_url=ticket_dict["endereco"]["urlFoto"],
        )
        ticket = Ticket(
            ticket_id=ticket_dict["codigoChamado"],
            organizational_unit=ticket_dict["unidadeOrganizacional"],
            date_time=ticket_dict["dataHora"],
            classification=classification,
            address=address,
            status=ticket_dict["status"],
        )
        return ticket
    elif code == 2:
        raise SGRCMalformedBodyException(response["descricao"])
    elif code == 3:
        raise SGRCInvalidBodyException(response["descricao"])
    elif code == 4:
        raise SGRCBusinessRuleException(response["descricao"])
    elif code == 5:
        raise SGRCInternalErrorException(response["descricao"])
    elif code == 6:
        raise SGRCEquivalentTicketException(response["descricao"])
    elif code == 7:
        raise SGRCDuplicateTicketException(response["descricao"])


def get_ticket_details(ticket_id: str) -> Ticket:
    """
    Gets the details of a ticket.

    Args:
        ticket_id (str): The ticket ID.

    Returns:
        Ticket: The ticket details.

    Raises:
        BaseSGRCException: If an unexpected exception occurs.
        SGRCBusinessRuleException: If the request violates a business rule.
        SGRCInternalErrorException: If the request causes an internal error.
        SGRCInvalidBodyException: If the request body is invalid.
        SGRCMalformedBodyException: If the request body is malformed.
        ValueError: If any of the arguments is invalid.
    """
    data = pre_get_ticket_details(ticket_id)
    try:
        response = post(settings.SGRC_URL_GET_TICKET_DETAILS, data)
    except Exception as exc:
        raise BaseSGRCException("Unexpected exception when trying to create a new ticket.") from exc
    return after_get_ticket_details(response)


async def async_get_ticket_details(ticket_id: str) -> Ticket:
    """
    Gets the details of a ticket.

    Args:
        ticket_id (str): The ticket ID.

    Returns:
        Ticket: The ticket details.

    Raises:
        BaseSGRCException: If an unexpected exception occurs.
        SGRCBusinessRuleException: If the request violates a business rule.
        SGRCInternalErrorException: If the request causes an internal error.
        SGRCInvalidBodyException: If the request body is invalid.
        SGRCMalformedBodyException: If the request body is malformed.
        ValueError: If any of the arguments is invalid.
    """
    data = pre_get_ticket_details(ticket_id)
    try:
        response = await apost(settings.SGRC_URL_GET_TICKET_DETAILS, data)
    except Exception as exc:
        raise BaseSGRCException("Unexpected exception when trying to create a new ticket.") from exc
    return after_get_ticket_details(response)


def pre_get_protocol_tickets_details(protocol_id: str) -> dict:
    if not isinstance(protocol_id, str):
        try:
            protocol_id = str(protocol_id)
        except Exception:
            raise ValueError(
                "'protocol_id' must be a string or an object that implements the __str__ method."
            )
    data = {"codigo": protocol_id}
    return data


def after_get_protocol_tickets_details(response: Dict[str, Any]) -> List[TicketDetails]:
    if "codigo" not in response or "descricao" not in response:
        raise BaseSGRCException(
            "Unexpected response from SGRC. 'codigo' or 'descricao' key is missing."
        )

    code = response["codigo"]
    try:
        code = int(code)
    except Exception as exc:  # noqa
        raise BaseSGRCException(f"Unexpected return code '{code}' from SGRC.") from exc

    if code == 1:
        if "chamados" not in response:
            raise BaseSGRCException("Unexpected response from SGRC. 'chamados' key is missing")
        tickets = []
        for ticket_dict in response["chamados"]:
            classification = Classification(
                category_code=ticket_dict["classificacao"]["codigoCategoria"],
                category=ticket_dict["classificacao"]["categoria"],
                type_code=ticket_dict["classificacao"]["codigoTipo"],
                type=ticket_dict["classificacao"]["tipo"],
                subtype_code=ticket_dict["classificacao"]["codigoSubtipo"],
                subtype=ticket_dict["classificacao"]["subtipo"],
            )
            address = Address(
                locality=ticket_dict["endereco"]["localidade"],
                street_code=ticket_dict["endereco"]["codigoLogradouro"],
                street=ticket_dict["endereco"]["logradouro"],
                number=ticket_dict["endereco"]["numero"],
                neighborhood_code=ticket_dict["endereco"]["codigoBairro"],
                neighborhood=ticket_dict["endereco"]["bairro"],
                complement=ticket_dict["endereco"]["complemento"],
                zip_code=ticket_dict["endereco"]["cep"],
                x_coordinate=ticket_dict["endereco"]["coordenadaX"],
                y_coordinate=ticket_dict["endereco"]["coordenadaY"],
                photo_url=ticket_dict["endereco"]["urlFoto"],
            )
            progresses: List[Progress] = []
            for progress_dict in ticket_dict["andamentos"]:
                progress = Progress(
                    insertion_date=progress_dict["dataInsercao"],
                    status=progress_dict["status"],
                    reason=progress_dict["motivoJustificativa"],
                    description=progress_dict["descricao"],
                )
                progresses.append(progress)
            reclassifications: List[Reclassification] = []
            for reclassification_dict in ticket_dict["reclassificacoes"]:
                reclassification = Reclassification(
                    reclassification_date=reclassification_dict["dataReclassificacao"],
                    previous_type=reclassification_dict["tipoAnterior"],
                    previous_subtype=reclassification_dict["subTipoAnterior"],
                    new_type=reclassification_dict["tipoNovo"],
                    new_subtype=reclassification_dict["subTipoNovo"],
                    reason=reclassification_dict["motivoJustificativa"],
                    previous_sla_forecast=reclassification_dict["previsaoAnteriorSLA"],
                )
                reclassifications.append(reclassification)
            diagnoses: List[Diagnosis] = []
            for diagnosis_dict in ticket_dict["diagnosticosProgramados"]:
                diagnosis = Diagnosis(
                    diagnosis_date=diagnosis_dict["dataDiagnostico"],
                    forecast=diagnosis_dict["previsaoAtendimento"],
                    status=diagnosis_dict["status"],
                    reason=diagnosis_dict["motivoJustificativa"],
                    description=diagnosis_dict["descricao"],
                )
                diagnoses.append(diagnosis)
            ticket = TicketDetails(
                ticket_id=ticket_dict["codigoChamado"],
                organizational_unit=ticket_dict["unidadeOrganizacional"],
                opening_date=ticket_dict["dataAbertura"],
                sla_forecast=ticket_dict["previsaoSLA"],
                classification=classification,
                description=ticket_dict["descricao"],
                address=address,
                status=ticket_dict["status"],
                progresses=progresses,
                reclassifications=reclassifications,
                scheduled_diagnosis=diagnoses,
            )
            tickets.append(ticket)
        return tickets

    elif code == 2:
        raise SGRCMalformedBodyException(response["descricao"])
    elif code == 3:
        raise SGRCInvalidBodyException(response["descricao"])
    elif code == 4:
        raise SGRCBusinessRuleException(response["descricao"])
    elif code == 5:
        raise SGRCInternalErrorException(response["descricao"])
    elif code == 6:
        raise SGRCEquivalentTicketException(response["descricao"])
    elif code == 7:
        raise SGRCDuplicateTicketException(response["descricao"])


def get_protocol_tickets_details(protocol_id: str) -> List[TicketDetails]:
    """
    Gets the details of the tickets of a protocol.

    Args:
        protocol_id (str): The protocol ID.

    Returns:
        List[TicketDetails]: The details of the tickets of the protocol.

    Raises:
        BaseSGRCException: If an unexpected exception occurs.
        SGRCBusinessRuleException: If the request violates a business rule.
        SGRCInternalErrorException: If the request causes an internal error.
        SGRCInvalidBodyException: If the request body is invalid.
        SGRCMalformedBodyException: If the request body is malformed.
        ValueError: If any of the arguments is invalid.
    """
    data = pre_get_protocol_tickets_details(protocol_id)
    try:
        response = post(settings.SGRC_URL_GET_PROTOCOL_TICKETS_DETAILS, data)
    except Exception as exc:
        raise BaseSGRCException("Unexpected exception when trying to create a new ticket.") from exc
    return after_get_protocol_tickets_details(response)


async def async_get_protocol_tickets_details(protocol_id: str) -> List[TicketDetails]:
    """
    Gets the details of the tickets of a protocol.

    Args:
        protocol_id (str): The protocol ID.

    Returns:
        List[TicketDetails]: The details of the tickets of the protocol.

    Raises:
        BaseSGRCException: If an unexpected exception occurs.
        SGRCBusinessRuleException: If the request violates a business rule.
        SGRCInternalErrorException: If the request causes an internal error.
        SGRCInvalidBodyException: If the request body is invalid.
        SGRCMalformedBodyException: If the request body is malformed.
        ValueError: If any of the arguments is invalid.
    """
    data = pre_get_protocol_tickets_details(protocol_id)
    try:
        response = await apost(settings.SGRC_URL_GET_PROTOCOL_TICKETS_DETAILS, data)
    except Exception as exc:
        raise BaseSGRCException("Unexpected exception when trying to create a new ticket.") from exc
    return after_get_protocol_tickets_details(response)


def version() -> Dict[str, Any]:
    """
    Gets the version of the SGRC API.
    """
    return post(settings.SGRC_URL_VERSION, {}, add_token=False)


async def async_version() -> Dict[str, Any]:
    """
    Gets the version of the SGRC API.
    """
    return await apost(settings.SGRC_URL_VERSION, {}, add_token=False)
