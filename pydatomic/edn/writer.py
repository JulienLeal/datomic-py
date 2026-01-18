"""EDN writer/serializer implementation."""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydatomic.exceptions import EDNParseError


def dumps(obj: Any, *, indent: int | None = None) -> str:
    """Serialize a Python object to an EDN string.

    Args:
        obj: The Python object to serialize.
        indent: Optional indentation level (not currently used, reserved for future).

    Returns:
        The EDN string representation of the object.

    Raises:
        EDNParseError: If the object cannot be serialized to EDN.

    Examples:
        >>> dumps({"name": "Alice", "age": 30})
        '{:name "Alice" :age 30}'
        >>> dumps([1, 2, 3])
        '[1 2 3]'
    """
    return _serialize(obj)


def _serialize(obj: Any) -> str:
    """Serialize a Python object to EDN format."""
    if obj is None:
        return "nil"

    if isinstance(obj, bool):
        return "true" if obj else "false"

    if isinstance(obj, int):
        return str(obj)

    if isinstance(obj, float):
        return str(obj)

    if isinstance(obj, str):
        # Check if it's a keyword (starts with :)
        if obj.startswith(":"):
            return obj
        return _serialize_string(obj)

    if isinstance(obj, datetime):
        return _serialize_datetime(obj)

    if isinstance(obj, UUID):
        return f'#uuid "{obj}"'

    if isinstance(obj, (list, tuple)):
        return _serialize_vector(obj)

    if isinstance(obj, (set, frozenset)):
        return _serialize_set(obj)

    if isinstance(obj, dict):
        return _serialize_map(obj)

    raise EDNParseError(f"Cannot serialize type {type(obj).__name__} to EDN")


def _serialize_string(s: str) -> str:
    """Serialize a string with proper escaping."""
    escaped = s.replace("\\", "\\\\")
    escaped = escaped.replace('"', '\\"')
    escaped = escaped.replace("\n", "\\n")
    escaped = escaped.replace("\r", "\\r")
    escaped = escaped.replace("\t", "\\t")
    return f'"{escaped}"'


def _serialize_datetime(dt: datetime) -> str:
    """Serialize a datetime to EDN #inst format."""
    # Format as ISO 8601 with timezone
    if dt.tzinfo is not None:
        # Has timezone info
        iso_str = dt.isoformat()
    else:
        # No timezone, assume UTC and append Z
        if dt.microsecond:
            iso_str = dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
        else:
            iso_str = dt.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    return f'#inst "{iso_str}"'


def _serialize_vector(items: list | tuple) -> str:
    """Serialize a list/tuple as an EDN vector."""
    elements = " ".join(_serialize(item) for item in items)
    return f"[{elements}]"


def _serialize_set(items: set | frozenset) -> str:
    """Serialize a set/frozenset as an EDN set."""
    elements = " ".join(_serialize(item) for item in items)
    return f"#{{{elements}}}"


def _serialize_map(mapping: dict) -> str:
    """Serialize a dict as an EDN map."""
    pairs = []
    for key, value in mapping.items():
        key_str = _serialize(key)
        value_str = _serialize(value)
        pairs.append(f"{key_str} {value_str}")
    return "{" + " ".join(pairs) + "}"
