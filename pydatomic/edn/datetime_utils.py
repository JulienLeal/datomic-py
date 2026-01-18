"""EDN datetime parsing utilities."""

from datetime import datetime, timezone

from pydatomic.exceptions import EDNParseError


def parse_datetime(value: str, pos: int | None = None) -> datetime:
    """Parse an ISO 8601 datetime string.

    Uses Python's fromisoformat with preprocessing for EDN-specific formats.
    Supports Z suffix and various timezone offset formats.

    Args:
        value: The datetime string to parse.
        pos: Optional position in source for error messages.

    Returns:
        A timezone-aware datetime object. Datetimes without timezone info
        are assumed to be UTC.

    Raises:
        EDNParseError: If the datetime format is invalid.
    """
    pos_info = f" at position {pos}" if pos is not None else ""

    # Normalize the datetime string for fromisoformat
    normalized = value

    # Replace Z suffix with +00:00 for fromisoformat compatibility
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    # Handle -00:00 which is equivalent to +00:00 (UTC)
    if normalized.endswith("-00:00"):
        normalized = normalized[:-6] + "+00:00"

    try:
        dt = datetime.fromisoformat(normalized)
        # If no timezone info, assume UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        pass

    # Fallback to strptime for edge cases
    formats = [
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(value, fmt)
            # If no timezone info, assume UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue

    raise EDNParseError(f"Invalid datetime format: {value}{pos_info}")
