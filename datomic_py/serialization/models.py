"""Datomic model definitions with field descriptors."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    Self,
    TypeVar,
    get_type_hints,
)

if TYPE_CHECKING:
    from datomic_py.datomic import Database

T = TypeVar("T")


class RefStrategy(Enum):
    """Strategy for handling reference attributes."""

    ID_ONLY = "id"  # Return just the entity ID
    EAGER = "eager"  # Fetch and deserialize immediately
    LAZY = "lazy"  # Return a lazy proxy


class Cardinality(Enum):
    """Datomic cardinality values."""

    ONE = ":db.cardinality/one"
    MANY = ":db.cardinality/many"


# Convenience aliases
ONE = Cardinality.ONE
MANY = Cardinality.MANY


@dataclass(frozen=True, slots=True)
class FieldDescriptor:
    """
    Descriptor for a Datomic attribute field.

    Attributes:
        attr: The Datomic attribute name (e.g., ":person/name")
        cardinality: ONE or MANY
        ref: Whether this is a reference to another entity
        ref_model: The model class for references (for type checking)
        ref_strategy: How to handle references
        converter: Optional custom converter function
        default: Default value if attribute is missing
        default_factory: Factory for default value

    """

    attr: str
    cardinality: Cardinality = Cardinality.ONE
    ref: bool = False
    ref_model: type | str | None = None  # str for forward references
    ref_strategy: RefStrategy = RefStrategy.ID_ONLY
    converter: Callable[[Any], Any] | None = None
    default: Any = None
    default_factory: Callable[[], Any] | None = None


def Field(
    attr: str,
    *,
    cardinality: Cardinality = Cardinality.ONE,
    ref: bool = False,
    ref_model: type | str | None = None,
    ref_strategy: RefStrategy = RefStrategy.ID_ONLY,
    converter: Callable[[Any], Any] | None = None,
    default: Any = None,
    default_factory: Callable[[], Any] | None = None,
) -> Any:
    """
    Define a field that maps to a Datomic attribute.

    Args:
        attr: The Datomic attribute name (e.g., ":person/name")
        cardinality: ONE (default) or MANY
        ref: True if this references another entity
        ref_model: Model class for references (can be string for forward refs)
        ref_strategy: How to handle references (ID_ONLY, EAGER, LAZY)
        converter: Optional custom converter function
        default: Default value if attribute is missing
        default_factory: Factory function for mutable defaults

    Returns:
        A FieldDescriptor (used at class definition time)

    Example:
        class Person(DatomicModel):
            name: str = Field(":person/name")
            friends: list[int] = Field(":person/friends", cardinality=MANY, ref=True)

    """
    return FieldDescriptor(
        attr=attr,
        cardinality=cardinality,
        ref=ref,
        ref_model=ref_model,
        ref_strategy=ref_strategy,
        converter=converter,
        default=default,
        default_factory=default_factory,
    )


class ModelMeta(type):
    """
    Metaclass for DatomicModel that processes field definitions.

    Collects FieldDescriptors and builds the mapping from Datomic
    attributes to Python attributes.
    """

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> ModelMeta:
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)

        if name == "DatomicModel":
            return cls

        # Collect field descriptors
        fields: dict[str, FieldDescriptor] = {}
        attr_to_field: dict[str, str] = {}  # :datomic/attr -> python_attr

        # Get type hints for the class
        try:
            hints = get_type_hints(cls)
        except Exception:
            hints = {}

        for attr_name, value in namespace.items():
            if isinstance(value, FieldDescriptor):
                fields[attr_name] = value
                attr_to_field[value.attr] = attr_name

        # Also check inherited fields
        for base in bases:
            if hasattr(base, "__datomic_fields__"):
                for k, v in base.__datomic_fields__.items():
                    if k not in fields:
                        fields[k] = v
                        attr_to_field[v.attr] = k

        cls.__datomic_fields__ = fields  # type: ignore[attr-defined]
        cls.__attr_to_field__ = attr_to_field  # type: ignore[attr-defined]
        cls.__type_hints__ = hints  # type: ignore[attr-defined]

        return cls


class DatomicModel(metaclass=ModelMeta):
    """
    Base class for Datomic entity models.

    Subclass and define fields using the Field() function:

        class Person(DatomicModel):
            __namespace__ = "person"

            name: str = Field(":person/name")
            email: str = Field(":person/email")
            friends: list[int] = Field(":person/friends", cardinality=MANY, ref=True)

    Models can be used as entity factories:

        person = Person.from_entity(entity_dict)
        person = Person.from_row(row_tuple, columns)
    """

    # Class variables set by metaclass
    __datomic_fields__: ClassVar[dict[str, FieldDescriptor]]
    __attr_to_field__: ClassVar[dict[str, str]]
    __type_hints__: ClassVar[dict[str, type]]

    # Optional namespace for auto-generating attribute names
    __namespace__: ClassVar[str] = ""

    # Entity ID (always present for entities)
    db_id: int | None = None

    def __init__(self, **kwargs: Any) -> None:
        """Initialize model with keyword arguments."""
        for name, descriptor in self.__datomic_fields__.items():
            if name in kwargs:
                value = kwargs[name]
            elif descriptor.default_factory is not None:
                value = descriptor.default_factory()
            else:
                value = descriptor.default
            setattr(self, name, value)

        # Set db_id if provided
        self.db_id = kwargs.get("db_id")

    @classmethod
    def from_entity(
        cls,
        entity: dict[str, Any],
        *,
        ref_strategy: RefStrategy | None = None,
        db: Database | None = None,
    ) -> Self:
        """
        Create a model instance from a Datomic entity dict.

        Args:
            entity: The entity dict from db.entity()
            ref_strategy: Override default ref strategy for all fields
            db: Database connection for eager/lazy loading refs

        Returns:
            A new model instance

        """
        kwargs: dict[str, Any] = {}

        # Extract db/id
        if ":db/id" in entity:
            kwargs["db_id"] = entity[":db/id"]

        for field_name, descriptor in cls.__datomic_fields__.items():
            datomic_attr = descriptor.attr

            if datomic_attr not in entity:
                continue

            value = entity[datomic_attr]

            # Apply custom converter if present
            if descriptor.converter is not None:
                value = descriptor.converter(value)

            # Handle cardinality many
            if descriptor.cardinality == Cardinality.MANY:
                if not isinstance(value, (list, tuple, set, frozenset)):
                    value = [value]
                else:
                    value = list(value)

            # Handle references
            if descriptor.ref:
                strategy = ref_strategy or descriptor.ref_strategy
                value = cls._resolve_ref(value, descriptor, strategy, db)

            kwargs[field_name] = value

        return cls(**kwargs)

    @classmethod
    def _resolve_ref(
        cls,
        value: Any,
        descriptor: FieldDescriptor,
        strategy: RefStrategy,
        db: Database | None,
    ) -> Any:
        """Resolve a reference according to strategy."""
        if strategy == RefStrategy.ID_ONLY:
            # Value is already the ID or dict with :db/id
            if isinstance(value, dict):
                return value.get(":db/id", value)
            if isinstance(value, (list, tuple)):
                return [v.get(":db/id", v) if isinstance(v, dict) else v for v in value]
            return value

        if strategy == RefStrategy.EAGER:
            if db is None:
                raise ValueError("Database connection required for eager loading")
            # Fetch and convert referenced entity
            ref_model = descriptor.ref_model
            if isinstance(ref_model, str):
                # Forward reference - need to resolve from registry
                from datomic_py.serialization.registry import model_registry

                ref_model = model_registry.get(ref_model)

            if isinstance(value, (list, tuple)):
                return [cls._fetch_ref(v, ref_model, db) for v in value]
            return cls._fetch_ref(value, ref_model, db)

        if strategy == RefStrategy.LAZY:
            # Return a lazy proxy
            if isinstance(value, (list, tuple)):
                return [LazyRef(v, descriptor.ref_model, db) for v in value]
            return LazyRef(value, descriptor.ref_model, db)

        return value

    @classmethod
    def _fetch_ref(cls, value: Any, ref_model: type | None, db: Database) -> Any:
        """Fetch a referenced entity."""
        eid = value.get(":db/id", value) if isinstance(value, dict) else value
        entity = db.entity(eid)
        if ref_model is not None and isinstance(ref_model, type) and issubclass(ref_model, DatomicModel):
            return ref_model.from_entity(entity, db=db)
        return entity

    @classmethod
    def from_row(
        cls,
        row: tuple[Any, ...],
        columns: Sequence[str],
        *,
        ref_strategy: RefStrategy | None = None,
        db: Database | None = None,
    ) -> Self:
        """
        Create a model instance from a query row.

        Args:
            row: The tuple from query results
            columns: Column names from :find clause
            ref_strategy: Override default ref strategy
            db: Database connection for eager/lazy loading

        Returns:
            A new model instance

        """
        entity = dict(zip(columns, row, strict=True))
        return cls.from_entity(entity, ref_strategy=ref_strategy, db=db)

    def to_dict(self, *, include_none: bool = False) -> dict[str, Any]:
        """
        Convert model to dict suitable for Datomic transaction.

        Args:
            include_none: Whether to include None values

        Returns:
            Dict with Datomic attribute names as keys

        """
        result: dict[str, Any] = {}

        if self.db_id is not None:
            result[":db/id"] = self.db_id

        for field_name, descriptor in self.__datomic_fields__.items():
            value = getattr(self, field_name, None)
            if value is None and not include_none:
                continue
            result[descriptor.attr] = value

        return result

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        fields = ", ".join(
            f"{name}={getattr(self, name, None)!r}" for name in self.__datomic_fields__
        )
        if self.db_id is not None:
            return f"{cls_name}(db_id={self.db_id}, {fields})"
        return f"{cls_name}({fields})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented
        if self.db_id is not None and other.db_id is not None:
            return self.db_id == other.db_id
        # Compare all fields
        for name in self.__datomic_fields__:
            if getattr(self, name, None) != getattr(other, name, None):
                return False
        return True

    def __hash__(self) -> int:
        if self.db_id is not None:
            return hash((self.__class__.__name__, self.db_id))
        # Hash based on all field values
        return hash(
            (
                self.__class__.__name__,
                tuple(getattr(self, name, None) for name in self.__datomic_fields__),
            )
        )


class LazyRef(Generic[T]):
    """
    Lazy reference proxy that fetches entity on first access.

    Usage:
        author: LazyRef[Person] = Field(":article/author", ref=True, ref_strategy=LAZY)

        # Later...
        author_entity = article.author.resolve()  # Fetches from DB
    """

    __slots__ = ("_id", "_model", "_db", "_cached")

    def __init__(
        self,
        entity_id: int | dict[str, Any],
        model: type[T] | str | None,
        db: Database | None,
    ) -> None:
        if isinstance(entity_id, dict):
            self._id: int = entity_id.get(":db/id", entity_id)  # type: ignore[assignment]
        else:
            self._id = entity_id
        self._model = model
        self._db = db
        self._cached: T | None = None

    @property
    def id(self) -> int:
        """Get the entity ID without fetching."""
        return self._id

    def resolve(self) -> T:
        """Fetch and return the referenced entity."""
        if self._cached is not None:
            return self._cached

        if self._db is None:
            raise ValueError("No database connection for lazy loading")

        entity = self._db.entity(self._id)

        if self._model is not None:
            if isinstance(self._model, str):
                from datomic_py.serialization.registry import model_registry

                model_cls = model_registry.get(self._model)
            else:
                model_cls = self._model

            if model_cls is not None and issubclass(model_cls, DatomicModel):
                self._cached = model_cls.from_entity(entity, db=self._db)  # type: ignore[assignment]
                return self._cached  # type: ignore[return-value]

        self._cached = entity  # type: ignore[assignment]
        return self._cached  # type: ignore[return-value]

    def __repr__(self) -> str:
        if self._cached is not None:
            return f"LazyRef({self._id}, resolved={self._cached!r})"
        return f"LazyRef({self._id})"
