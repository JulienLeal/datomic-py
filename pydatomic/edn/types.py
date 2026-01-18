"""EDN type definitions and sentinels."""

from datetime import datetime
from typing import Union
from uuid import UUID


class _Skip:
    """Sentinel for values that should be skipped (unknown tags)."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "SKIP"


SKIP = _Skip()

# Named characters mapping
NAMED_CHARS: dict[str, str] = {
    "newline": "\n",
    "space": " ",
    "tab": "\t",
    "return": "\r",
}

# EDN value type - represents all possible EDN values
EDNValue = Union[
    None,
    bool,
    int,
    float,
    str,
    datetime,
    UUID,
    tuple["EDNValue", ...],
    frozenset["EDNValue"],
    dict[str, "EDNValue"],
]
