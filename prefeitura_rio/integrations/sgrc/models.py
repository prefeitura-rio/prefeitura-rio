# -*- coding: utf-8 -*-
from typing import List


class SGRCModel:
    _attrs_dict = {}

    def to_dict(self):
        data = {}
        for k, v in self._attrs_dict.items():
            # Cast v to the type of the attribute
            if issubclass(v[1], SGRCModel):
                v = getattr(self, v[0]).to_dict()
            elif issubclass(v[1], list):
                # Get type of list elements
                try:
                    list_type = type(getattr(self, v[0])[0])
                except IndexError:
                    # Empty list
                    list_type = type(None)
                # Cast each element of the list to the type of the attribute
                if issubclass(list_type, SGRCModel):
                    v = [i.to_dict() for i in getattr(self, v[0])]
                else:
                    v = [list_type(i) for i in getattr(self, v[0])]
            else:
                v = getattr(self, v[0])
            # Get actual key
            k = k
            data[k] = v
        return data

    def __str__(self):
        class_name = self.__class__.__name__
        attrs = ", ".join([f"{k}={v}" for k, v in self.to_dict().items()])
        return f"{class_name}({attrs})"

    def __repr__(self) -> str:
        return str(self)

    def __getitem__(self, key):
        actual_key = self._attrs_dict[key][0]
        return getattr(self, actual_key)

    def __setitem__(self, key, value):
        actual_key = self._attrs_dict[key][0]
        return setattr(self, actual_key, value)


class SpecificAttribute(SGRCModel):
    """
    Wrapper for implementing "atributoEspecifico".
    """

    _attrs_dict = {
        "nomeAtributoEspecifico": ("name", str),
        "valorAtributoEspecifico": ("value", str),
    }

    def __init__(self, name: str, value: str):
        self.name = name
        self.value = value


class Address(SGRCModel):
    """
    Wrapper for implementing "Endereco".
    """

    _attrs_dict = {
        "localidade": ("locality", str),
        "codigoLogradouro": ("street_code", str),
        "logradouro": ("street", str),
        "numero": ("number", str),
        "codigoBairro": ("neighborhood_code", str),
        "bairro": ("neighborhood", str),
        "complemento": ("complement", str),
        "cep": ("zip_code", str),
        "coordenadaX": ("x_coordinate", str),
        "coordenadaY": ("y_coordinate", str),
        "urlFoto": ("photo_url", str),
        "tipoEndereco": ("address_type", str),
    }

    def __init__(
        self,
        locality: str = None,
        street_code: str = None,
        street: str = None,
        number: str = None,
        neighborhood_code: str = None,
        neighborhood: str = None,
        complement: str = None,
        zip_code: str = None,
        x_coordinate: str = None,
        y_coordinate: str = None,
        photo_url: str = None,
        address_type: str = None,
    ):
        self.locality = locality or ""
        self.street_code = street_code or ""
        self.street = street or ""
        self.number = number or ""
        self.neighborhood_code = neighborhood_code or ""
        self.neighborhood = neighborhood or ""
        self.complement = complement or ""
        self.zip_code = zip_code or ""
        self.x_coordinate = x_coordinate or ""
        self.y_coordinate = y_coordinate or ""
        self.photo_url = photo_url or ""
        self.address_type = address_type or ""


class Phones(SGRCModel):
    """
    Wrapper for implementing "Telefones".
    """

    _attrs_dict = {
        "telefoneR": ("mobile_phone", str),
        "telefone1": ("residential_phone", str),
        "telefone2": ("other_phone", str),
    }

    def __init__(
        self,
        mobile_phone: str = None,
        residential_phone: str = None,
        other_phone: str = None,
    ):
        self.mobile_phone = mobile_phone or ""
        self.residential_phone = residential_phone or ""
        self.other_phone = other_phone or ""


class Classification(SGRCModel):
    """
    Wrapper for implementing "classificacao".
    """

    _attrs_dict = {
        "codigoCategoria": ("category_code", str),
        "categoria": ("category", str),
        "codigoTipo": ("type_code", str),
        "tipo": ("type", str),
        "codigoSubtipo": ("subtype_code", str),
        "subtipo": ("subtype", str),
    }

    def __init__(
        self,
        category_code: str,
        category: str,
        type_code: str,
        type: str,
        subtype_code: str,
        subtype: str,
    ):
        self.category_code = category_code
        self.category = category
        self.type_code = type_code
        self.type = type
        self.subtype_code = subtype_code
        self.subtype = subtype


class Progress(SGRCModel):
    """
    Wrapper for implementing "andamento".
    """

    _attrs_dict = {
        "dataInsercao": ("insertion_date", str),
        "status": ("status", str),
        "motivoJustificativa": ("reason", str),
        "description": ("description", str),
    }

    def __init__(
        self,
        insertion_date: str,
        status: str,
        reason: str,
        description: str,
    ):
        self.insertion_date = insertion_date
        self.status = status
        self.reason = reason
        self.description = description


class Reclassification(SGRCModel):
    """
    Wrapper for implementing "reclassificacao".
    """

    _attrs_dict = {
        "dataReclassificacao": ("reclassification_date", str),
        "tipoAnterior": ("previous_type", str),
        "subTipoAnterior": ("previous_subtype", str),
        "tipoNovo": ("new_type", str),
        "subTipoNovo": ("new_subtype", str),
        "justificativa": ("reason", str),
        "previsaoAnteriorSLA": ("previous_sla_forecast", str),
    }

    def __init__(
        self,
        reclassification_date: str,
        previous_type: str,
        previous_subtype: str,
        new_type: str,
        new_subtype: str,
        reason: str,
        previous_sla_forecast: str,
    ):
        self.reclassification_date = reclassification_date
        self.previous_type = previous_type
        self.previous_subtype = previous_subtype
        self.new_type = new_type
        self.new_subtype = new_subtype
        self.reason = reason
        self.previous_sla_forecast = previous_sla_forecast


class Diagnosis(SGRCModel):
    """
    Wrapper for implementing "diagnostico".
    """

    _attrs_dict = {
        "dataDiagnostico": ("diagnosis_date", str),
        "previsaoAtendimento": ("forecast", str),
        "status": ("status", str),
        "motivoJustificativa": ("reason", str),
        "descricao": ("description", str),
    }

    def __init__(
        self,
        diagnosis_date: str,
        forecast: str,
        status: str,
        reason: str,
        description: str,
    ):
        self.diagnosis_date = diagnosis_date
        self.forecast = forecast
        self.status = status
        self.reason = reason
        self.description = description


class Requester(SGRCModel):
    """
    Wrapper for implementing "solicitantePortalWeb".
    """

    _attrs_dict = {
        "nome": ("name", str),
        "email": ("email", str),
        "codigoIdioma": ("language_code", str),
        "codigoPaisDDI": ("country_code", str),
        "cpf": ("cpf", str),
        "identidade": ("identity", str),
        "sexo": ("gender", str),
        "dataNascimento": ("birth_date", str),
        "endereco": ("address", Address),
        "telefones": ("phones", Phones),
    }

    def __init__(
        self,
        name: str = None,
        email: str = None,
        language_code: str = None,
        country_code: str = None,
        cpf: str = None,
        identity: str = None,
        gender: str = None,
        birth_date: str = None,
        address: Address = None,
        phones: Phones = None,
    ):
        self.name = name or ""
        self.email = email or ""
        self.language_code = language_code or ""
        self.country_code = country_code or ""
        self.cpf = cpf or ""
        self.identity = identity or ""
        self.gender = gender or ""
        self.birth_date = birth_date or ""
        self.address = address or Address()
        self.phones = phones or Phones()


class NewTicket(SGRCModel):
    """
    Wrapper for implementing the response from the "new ticket" endpoint.
    """

    _attrs_dict = {
        "protocolo": ("protocol_id", str),
        "codigoChamado": ("ticket_id", str),
    }

    def __init__(self, protocol_id: str, ticket_id: str):
        self.protocol_id = protocol_id
        self.ticket_id = ticket_id


class TicketSummary(SGRCModel):
    """
    Wrapper for implementing the summary of a ticket.
    """

    _attrs_dict = {
        "codigoChamado": ("ticket_id", str),
        "categoria": ("category", str),
        "tipo": ("type", str),
        "subTipo": ("subtype", str),
        "descricao": ("description", str),
        "status": ("status", str),
    }

    def __init__(
        self,
        ticket_id: str,
        category: str,
        type: str,
        subtype: str,
        description: str,
        status: str,
    ):
        self.ticket_id = ticket_id
        self.category = category
        self.type = type
        self.subtype = subtype
        self.description = description
        self.status = status


class Ticket(SGRCModel):
    """
    Wrapper for implementing the ticket information.
    """

    _attrs_dict = {
        "codigoChamado": ("ticket_id", str),
        "unidadeOrganizacional": ("organizational_unit", str),
        "dataHora": ("date_time", str),
        "classificacao": ("classification", Classification),
        "endereco": ("address", Address),
        "status": ("status", str),
    }

    def __init__(
        self,
        ticket_id: str,
        organizational_unit: str,
        date_time: str,
        classification: Classification,
        address: Address,
        status: str,
    ):
        self.ticket_id = ticket_id
        self.organizational_unit = organizational_unit
        self.date_time = date_time
        self.classification = classification
        self.address = address
        self.status = status


class TicketDetails(SGRCModel):
    """
    Wrapper for implementing the ticket details.
    """

    _attrs_dict = {
        "codigoChamado": ("ticket_id", str),
        "unidadeOrganizacional": ("organizational_unit", str),
        "dataAbertura": ("opening_date", str),
        "previsaoSLA": ("sla_forecast", str),
        "classificacao": ("classification", Classification),
        "descricao": ("description", str),
        "endereco": ("address", Address),
        "status": ("status", str),
        "andamentos": ("progresses", list),
        "reclassificacoes": ("reclassifications", list),
        "diagnosticosProgramados": ("scheduled_diagnosis", list),
    }

    def __init__(
        self,
        ticket_id: str,
        organizational_unit: str,
        opening_date: str,
        sla_forecast: str,
        classification: Classification,
        description: str,
        address: Address,
        status: str,
        progresses: List[Progress],
        reclassifications: List[Reclassification],
        scheduled_diagnosis: List[Diagnosis],
    ):
        self.ticket_id = ticket_id
        self.organizational_unit = organizational_unit
        self.opening_date = opening_date
        self.sla_forecast = sla_forecast
        self.classification = classification
        self.description = description
        self.address = address
        self.status = status
        self.progresses = progresses
        self.reclassifications = reclassifications
        self.scheduled_diagnosis = scheduled_diagnosis
