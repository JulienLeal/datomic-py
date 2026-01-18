"""
Optional Pydantic model support for datomic-py.

This module provides row and entity factories that produce Pydantic model instances.
Pydantic is NOT a required dependency - this module gracefully handles its absence.

Usage:
    from datomic_py.serialization.pydantic_support import pydantic_row, pydantic_entity
    from pydantic import BaseModel

    class Person(BaseModel):
        name: str
        email: str

    results = db.query(q, row_factory=pydantic_row(Person))
    entity = db.entity(123, entity_factory=pydantic_entity(Person))
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Generic, TypeVar

try:
    from pydantic import BaseModel

    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False
    BaseModel = object  # type: ignore[misc, assignment]

if TYPE_CHECKING:
    from pydantic import BaseModel as BaseModelType

T = TypeVar("T", bound="BaseModelType")


class PydanticRowFactory(Generic[T]):
    """
    Row factory that produces Pydantic model instances.

    Example:
        from pydantic import BaseModel

        class Person(BaseModel):
            name: str
            email: str

        factory = pydantic_row(Person)
        result = factory(("Alice", "alice@example.com"), ["name", "email"])
        # -> Person(name='Alice', email='alice@example.com')

    """

    __slots__ = ("_model", "_field_mapping", "_model_fields")

    def __init__(
        self,
        model: type[T],
        field_mapping: dict[str, str] | None = None,
    ) -> None:
        if not HAS_PYDANTIC:
            raise ImportError(
                "Pydantic is required for PydanticRowFactory. "
                "Install it with: pip install pydantic"
            )
        self._model = model
        self._field_mapping = field_mapping or {}
        self._model_fields = set(model.model_fields.keys())

    def __call__(self, row: tuple[Any, ...], columns: Sequence[str]) -> T:
        kwargs: dict[str, Any] = {}
        for col, val in zip(columns, row, strict=True):
            field_name = self._field_mapping.get(col, col)
            # Sanitize: remove ? and : prefix, replace / and - with _
            field_name = field_name.lstrip("?:").replace("/", "_").replace("-", "_")
            if field_name in self._model_fields:
                kwargs[field_name] = val
        return self._model(**kwargs)


class PydanticEntityFactory(Generic[T]):
    """
    Entity factory that produces Pydantic model instances.

    Example:
        from pydantic import BaseModel

        class Person(BaseModel):
            name: str
            email: str

        factory = pydantic_entity(Person, {":person/name": "name", ":person/email": "email"})
        person = factory({":person/name": "Alice", ":person/email": "alice@example.com"})
        # -> Person(name='Alice', email='alice@example.com')

    """

    __slots__ = ("_model", "_field_mapping", "_validate", "_model_fields", "_reverse_mapping")

    def __init__(
        self,
        model: type[T],
        field_mapping: dict[str, str] | None = None,
        validate: bool = True,
    ) -> None:
        if not HAS_PYDANTIC:
            raise ImportError(
                "Pydantic is required for PydanticEntityFactory. "
                "Install it with: pip install pydantic"
            )
        self._model = model
        self._field_mapping = field_mapping or {}
        self._validate = validate
        self._model_fields = set(model.model_fields.keys())
        # Build reverse mapping from Datomic attr to field name
        self._reverse_mapping = {v: k for k, v in self._field_mapping.items()}

    def __call__(self, entity: dict[str, Any]) -> T:
        kwargs: dict[str, Any] = {}

        for key, value in entity.items():
            # Skip :db/id unless explicitly mapped
            if key == ":db/id" and ":db/id" not in self._reverse_mapping:
                continue

            field_name = self._reverse_mapping.get(key)
            if field_name is None:
                # Auto-derive field name
                field_name = key.lstrip(":").replace("/", "_").replace("-", "_")

            if field_name in self._model_fields:
                kwargs[field_name] = value

        if self._validate:
            return self._model(**kwargs)
        return self._model.model_construct(**kwargs)


def pydantic_row(
    model: type[T],
    field_mapping: dict[str, str] | None = None,
) -> PydanticRowFactory[T]:
    """
    Create a Pydantic row factory.

    Args:
        model: The Pydantic model class to instantiate.
        field_mapping: Optional mapping from column names to field names.

    Returns:
        A PydanticRowFactory instance.

    Raises:
        ImportError: If Pydantic is not installed.

    Example:
        from pydantic import BaseModel

        class Person(BaseModel):
            name: str
            email: str

        results = db.query(q, row_factory=pydantic_row(Person))

    """
    return PydanticRowFactory(model, field_mapping)


def pydantic_entity(
    model: type[T],
    field_mapping: dict[str, str] | None = None,
    validate: bool = True,
) -> PydanticEntityFactory[T]:
    """
    Create a Pydantic entity factory.

    Args:
        model: The Pydantic model class to instantiate.
        field_mapping: Optional mapping from field names to Datomic attribute names.
                      e.g., {"name": ":person/name", "email": ":person/email"}
        validate: If True (default), validate input data.
                 If False, use model_construct() for faster creation without validation.

    Returns:
        A PydanticEntityFactory instance.

    Raises:
        ImportError: If Pydantic is not installed.

    Example:
        from pydantic import BaseModel

        class Person(BaseModel):
            name: str
            email: str

        entity = db.entity(123, entity_factory=pydantic_entity(Person, {
            "name": ":person/name",
            "email": ":person/email"
        }))

    """
    return PydanticEntityFactory(model, field_mapping, validate)
