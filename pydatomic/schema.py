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

    Arguments which require clojure nil take Python None.
    """
    parts = [":db/id #db/id[:db.part/db]"]
    parts.append(f":db/ident {ident}")
    parts.append(f":db/valueType {valueType}")
    parts.append(f":db/cardinality {cardinality}")
    if doc is not None:
        parts.append(":db/doc " + doc)
    unique_map = {IDENTITY: IDENTITY, VALUE: VALUE, None: "nil"}
    parts.append(f":db/unique {unique_map[unique]}")
    parts.append(f":db/index {'true' if index else 'false'}")
    parts.append(f":db/fulltext {'true' if fulltext else 'false'}")
    parts.append(f":db/noHistory {'true' if noHistory else 'false'}")
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
