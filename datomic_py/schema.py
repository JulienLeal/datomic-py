"""Datomic schema definition helpers."""

# Cardinality constants
ONE = ":db.cardinality/one"
MANY = ":db.cardinality/many"

# Uniqueness constants
IDENTITY = ":db.unique/identity"
VALUE = ":db.unique/value"

# Value type constants
STRING = ":db.type/string"
BOOLEAN = ":db.type/boolean"
LONG = ":db.type/long"
BIGINT = ":db.type/bigint"
FLOAT = ":db.type/float"
DOUBLE = ":db.type/double"
BIGDEC = ":db.type/bigdec"
INSTANT = ":db.type/instant"
UUID = ":db.type/uuid"
URI = ":db.type/uri"
KEYWORD = ":db.type/keyword"
REF = ":db.type/ref"
BYTES = ":db.type/bytes"


def Attribute(
    ident: str,
    valueType: str,
    doc: str | None = None,
    cardinality: str = ONE,
    unique: str | None = None,
    index: bool = False,
    fulltext: bool = False,
    noHistory: bool = False,
) -> str:
    """
    Create a Datomic attribute definition.

    Creates an EDN map representing a Datomic schema attribute.
    Only includes optional attributes when they have non-default values.

    Args:
        ident: The attribute identifier (e.g., ":person/name").
        valueType: The attribute type (e.g., STRING, BOOLEAN).
        doc: Optional documentation string.
        cardinality: The cardinality (ONE or MANY).
        unique: Optional uniqueness constraint (IDENTITY or VALUE).
        index: Whether to index the attribute.
        fulltext: Whether to enable fulltext search.
        noHistory: Whether to exclude from history.

    Returns:
        An EDN string representing the attribute definition.

    """
    parts = [f":db/ident {ident}"]
    parts.append(f":db/valueType {valueType}")
    parts.append(f":db/cardinality {cardinality}")
    if doc is not None:
        parts.append(f":db/doc {doc}")
    if unique is not None:
        parts.append(f":db/unique {unique}")
    if index:
        parts.append(":db/index true")
    if fulltext:
        parts.append(":db/fulltext true")
    if noHistory:
        parts.append(":db/noHistory true")
    return "{" + "\n ".join(parts) + "}"


def Schema(*attributes: str) -> tuple[str, ...]:
    """Create a schema from multiple attributes."""
    return attributes
