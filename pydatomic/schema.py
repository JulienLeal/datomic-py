"""Datomic schema definition helpers."""

ONE = ":db.cardinality/one"
MANY = ":db.cardinality/many"
IDENTITY = ":db.unique/identity"
VALUE = ":db.unique/value"
STRING = ":db.type/string"
BOOLEAN = ":db.type/boolean"


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


if __name__ == "__main__":
    schema = Schema(
        Attribute(":task/name", STRING, cardinality=ONE),
        Attribute(":task/closed", BOOLEAN),
        Attribute(":data/user", STRING),
    )
    for a in schema:
        print(a)
