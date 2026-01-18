"""Custom exceptions for pydatomic."""


class PydatomicError(Exception):
    """Base exception for pydatomic."""


class DatomicClientError(PydatomicError):
    """HTTP client errors."""


class DatomicConnectionError(PydatomicError):
    """Connection/network errors."""


class EDNParseError(PydatomicError):
    """EDN parsing errors."""
