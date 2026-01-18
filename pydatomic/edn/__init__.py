"""EDN (Extensible Data Notation) parser and serializer for Python.

This module provides functions to parse EDN strings into Python objects
and serialize Python objects to EDN strings.

Example usage:
    >>> from pydatomic.edn import loads, dumps
    >>> data = loads('{:name "Alice" :age 30}')
    >>> data
    {':name': 'Alice', ':age': 30}
    >>> dumps(data)
    '{:name "Alice" :age 30}'
"""

from typing import Any

from pydatomic.exceptions import EDNParseError
from pydatomic.edn.types import SKIP, EDNValue, NAMED_CHARS
from pydatomic.edn.reader import EdnReader
from pydatomic.edn.writer import dumps
from pydatomic.edn.tags import TagRegistry, default_registry
from pydatomic.edn.datetime_utils import parse_datetime


def loads(
    s: str | bytes,
    max_depth: int = 100,
    tag_registry: TagRegistry | None = None,
) -> EDNValue | None:
    """Load an EDN string and return the parsed Python object.

    Args:
        s: The EDN string (or bytes) to parse.
        max_depth: Maximum nesting depth allowed (default 100).
        tag_registry: Optional custom tag registry for handling tags.

    Returns:
        The parsed Python object. Returns None for both empty input
        and EDN nil value.

    Raises:
        EDNParseError: If the input is invalid EDN or contains invalid UTF-8.

    Examples:
        >>> loads('[1 2 3]')
        (1, 2, 3)
        >>> loads('{:a 1 :b 2}')
        {':a': 1, ':b': 2}
        >>> loads('#inst "2023-01-15T10:30:00Z"')
        datetime.datetime(2023, 1, 15, 10, 30)
    """
    if isinstance(s, bytes):
        try:
            s = s.decode("utf-8")
        except UnicodeDecodeError as e:
            raise EDNParseError(f"Invalid UTF-8 encoding: {e}") from e

    reader = EdnReader(s, max_depth=max_depth, tag_registry=tag_registry)
    return reader.read_value()


__all__ = [
    "loads",
    "dumps",
    "EdnReader",
    "EDNParseError",
    "EDNValue",
    "SKIP",
    "NAMED_CHARS",
    "TagRegistry",
    "default_registry",
    "parse_datetime",
]
