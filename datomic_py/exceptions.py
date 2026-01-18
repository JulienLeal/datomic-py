"""Custom exceptions for datomic-py."""


class DatomicPyError(Exception):
    """Base exception for datomic-py."""


class DatomicClientError(DatomicPyError):
    """HTTP client errors."""


class DatomicConnectionError(DatomicPyError):
    """Connection/network errors."""


class EDNParseError(DatomicPyError):
    """EDN parsing errors."""
