"""Row and entity factory implementations."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import fields as dataclass_fields, is_dataclass
from typing import Any, Generic, NamedTuple, Protocol, TypeVar, runtime_checkable

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


@runtime_checkable
class RowFactory(Protocol[T_co]):
    """Protocol for row factory callables."""

    def __call__(self, row: tuple[Any, ...], columns: Sequence[str]) -> T_co:
        """Convert a raw row tuple to the target type."""
        ...


@runtime_checkable
class EntityFactory(Protocol[T_co]):
    """Protocol for entity factory callables."""

    def __call__(self, entity: dict[str, Any]) -> T_co:
        """Convert a raw entity dict to the target type."""
        ...


# --- Row Factories for Query Results ---


def tuple_row(row: tuple[Any, ...], columns: Sequence[str]) -> tuple[Any, ...]:
    """Identity factory - returns raw tuples (default behavior)."""
    return row


def dict_row(row: tuple[Any, ...], columns: Sequence[str]) -> dict[str, Any]:
    """Convert row to dict with column names as keys."""
    return dict(zip(columns, row, strict=True))


class NamedTupleRowFactory(Generic[T]):
    """
    Factory that produces namedtuples for each row.

    Caches the namedtuple class for performance.

    Example:
        factory = namedtuple_row("Person")
        result = factory((1, "Alice"), ["id", "name"])
        # -> Person(id=1, name='Alice')
    """

    __slots__ = ("_nt_class", "_columns", "_name")

    def __init__(self, name: str = "Row") -> None:
        self._nt_class: type | None = None
        self._columns: tuple[str, ...] | None = None
        self._name = name

    def __call__(self, row: tuple[Any, ...], columns: Sequence[str]) -> Any:
        columns_tuple = tuple(columns)
        if self._nt_class is None or self._columns != columns_tuple:
            # Create new namedtuple class for these columns
            # Sanitize column names for namedtuple (remove ? prefix, replace / with _)
            field_names = tuple(
                col.lstrip("?:").replace("/", "_").replace("-", "_") for col in columns
            )
            # Use typing.NamedTuple for proper type support
            self._nt_class = NamedTuple(self._name, [(n, Any) for n in field_names])  # type: ignore[misc]
            self._columns = columns_tuple
        return self._nt_class(*row)


def namedtuple_row(name: str = "Row") -> NamedTupleRowFactory[Any]:
    """
    Create a namedtuple row factory.

    Args:
        name: The name of the namedtuple class to create.

    Returns:
        A NamedTupleRowFactory instance.

    Example:
        results = db.query(q, row_factory=namedtuple_row("Person"))
        for person in results:
            print(person.name, person.email)
    """
    return NamedTupleRowFactory(name)


class DataclassRowFactory(Generic[T]):
    """
    Factory that produces dataclass instances for each row.

    Requires column names to match dataclass field names.

    Example:
        @dataclass
        class Person:
            name: str
            email: str

        factory = dataclass_row(Person)
        result = factory(("Alice", "alice@example.com"), ["name", "email"])
    """

    __slots__ = ("_cls", "_field_mapping", "_dc_fields")

    def __init__(self, cls: type[T], field_mapping: dict[str, str] | None = None) -> None:
        if not is_dataclass(cls):
            raise TypeError(f"{cls.__name__} is not a dataclass")
        self._cls = cls
        # Map Datomic attr names to dataclass field names
        self._field_mapping = field_mapping or {}
        self._dc_fields = {f.name for f in dataclass_fields(cls)}

    def __call__(self, row: tuple[Any, ...], columns: Sequence[str]) -> T:
        kwargs: dict[str, Any] = {}
        for col, val in zip(columns, row, strict=True):
            # Apply field mapping if provided
            field_name = self._field_mapping.get(col, col)
            # Sanitize: remove ? and : prefix, replace / and - with _
            field_name = field_name.lstrip("?:").replace("/", "_").replace("-", "_")
            if field_name in self._dc_fields:
                kwargs[field_name] = val
        return self._cls(**kwargs)


def dataclass_row(
    cls: type[T], field_mapping: dict[str, str] | None = None
) -> DataclassRowFactory[T]:
    """
    Create a dataclass row factory.

    Args:
        cls: The dataclass type to instantiate.
        field_mapping: Optional mapping from column names to field names.

    Returns:
        A DataclassRowFactory instance.

    Example:
        @dataclass
        class Person:
            person_name: str
            person_email: str

        results = db.query(q, row_factory=dataclass_row(Person, {
            "name": "person_name",
            "email": "person_email"
        }))
    """
    return DataclassRowFactory(cls, field_mapping)


# --- Entity Factories for Entity Results ---


def dict_entity(entity: dict[str, Any]) -> dict[str, Any]:
    """Identity factory - returns raw dicts (default behavior)."""
    return entity


class CleanDictEntityFactory:
    """
    Factory that converts Datomic entity dicts to clean Python dicts.

    Transforms keys from `:namespace/attr` to `attr` or `namespace_attr`.

    Example:
        factory = clean_dict_entity(include_namespace=False)
        result = factory({":person/name": "Alice", ":person/email": "alice@example.com"})
        # -> {"name": "Alice", "email": "alice@example.com"}
    """

    __slots__ = ("_include_namespace", "_key_transform")

    def __init__(
        self,
        include_namespace: bool = False,
        key_transform: Callable[[str], str] | None = None,
    ) -> None:
        self._include_namespace = include_namespace
        self._key_transform = key_transform

    def __call__(self, entity: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in entity.items():
            if self._key_transform:
                new_key = self._key_transform(key)
            else:
                new_key = self._transform_key(key)
            result[new_key] = value
        return result

    def _transform_key(self, key: str) -> str:
        """Transform Datomic key to Python-friendly key."""
        key = key.lstrip(":")
        if "/" in key:
            ns, attr = key.split("/", 1)
            if self._include_namespace:
                return f"{ns}_{attr}".replace("-", "_")
            return attr.replace("-", "_")
        return key.replace("-", "_")


def clean_dict_entity(
    include_namespace: bool = False,
    key_transform: Callable[[str], str] | None = None,
) -> CleanDictEntityFactory:
    """
    Create a clean dict entity factory.

    Args:
        include_namespace: If True, include namespace in key (e.g., "person_name").
                          If False, use just the attribute name (e.g., "name").
        key_transform: Optional custom key transformation function.

    Returns:
        A CleanDictEntityFactory instance.

    Example:
        entity = db.entity(123, entity_factory=clean_dict_entity())
        print(entity["name"])  # Instead of entity[":person/name"]
    """
    return CleanDictEntityFactory(include_namespace, key_transform)


class DataclassEntityFactory(Generic[T]):
    """
    Factory that produces dataclass instances from entities.

    Example:
        @dataclass
        class Person:
            name: str
            email: str

        factory = dataclass_entity(Person, {":person/name": "name", ":person/email": "email"})
        person = factory({":person/name": "Alice", ":person/email": "alice@example.com"})
    """

    __slots__ = ("_cls", "_field_mapping", "_strict", "_reverse_mapping", "_dc_fields")

    def __init__(
        self,
        cls: type[T],
        field_mapping: dict[str, str] | None = None,
        strict: bool = False,
    ) -> None:
        if not is_dataclass(cls):
            raise TypeError(f"{cls.__name__} is not a dataclass")
        self._cls = cls
        self._field_mapping = field_mapping or {}
        self._strict = strict
        # Build reverse mapping from Datomic attr to field name
        self._reverse_mapping = {v: k for k, v in self._field_mapping.items()}
        self._dc_fields = {f.name for f in dataclass_fields(cls)}

    def __call__(self, entity: dict[str, Any]) -> T:
        kwargs: dict[str, Any] = {}

        for key, value in entity.items():
            # Skip :db/id unless explicitly mapped
            if key == ":db/id" and ":db/id" not in self._reverse_mapping:
                continue

            # Apply reverse mapping
            field_name = self._reverse_mapping.get(key)
            if field_name is None:
                # Auto-derive field name
                field_name = key.lstrip(":").replace("/", "_").replace("-", "_")

            if field_name in self._dc_fields:
                kwargs[field_name] = value
            elif self._strict:
                raise ValueError(f"No field for Datomic attribute {key}")

        return self._cls(**kwargs)


def dataclass_entity(
    cls: type[T],
    field_mapping: dict[str, str] | None = None,
    strict: bool = False,
) -> DataclassEntityFactory[T]:
    """
    Create a dataclass entity factory.

    Args:
        cls: The dataclass type to instantiate.
        field_mapping: Optional mapping from field names to Datomic attribute names.
                      e.g., {"name": ":person/name", "email": ":person/email"}
        strict: If True, raise ValueError for unmapped attributes.

    Returns:
        A DataclassEntityFactory instance.

    Example:
        @dataclass
        class Person:
            name: str
            email: str

        entity = db.entity(123, entity_factory=dataclass_entity(Person, {
            "name": ":person/name",
            "email": ":person/email"
        }))
    """
    return DataclassEntityFactory(cls, field_mapping, strict)
