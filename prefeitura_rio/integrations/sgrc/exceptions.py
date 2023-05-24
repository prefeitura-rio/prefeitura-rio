# -*- coding: utf-8 -*-
class BaseSGRCException(Exception):
    pass


class SGRCMalformedBodyException(BaseSGRCException):
    pass


class SGRCInvalidBodyException(BaseSGRCException):
    pass


class SGRCBusinessRuleException(BaseSGRCException):
    pass


class SGRCInternalErrorException(BaseSGRCException):
    pass


class SGRCEquivalentTicketException(BaseSGRCException):
    pass


class SGRCDuplicateTicketException(BaseSGRCException):
    pass
