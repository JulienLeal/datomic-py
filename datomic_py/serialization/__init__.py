"""
Datomic Python object serialization.

This module provides tools for converting between Datomic query/entity
results and Python objects like dicts, namedtuples, dataclasses, and
custom model classes.

Usage:
    from datomic_py.serialization import (
        # Row factories for query results
        dict_row,
        namedtuple_row,
        dataclass_row,

        # Entity factories
        dict_entity,
        clean_dict_entity,
        dataclass_entity,

        # Model-based serialization
        DatomicModel,
        Field,
        ONE,
        MANY,
        RefStrategy,

        # Type conversion
        TypeConverter,

        # Registry
        model_registry,
        register_model,
    )

Example:
    from datomic_py import Datomic
    from datomic_py.serialization import dict_row, DatomicModel, Field, MANY

    conn = Datomic("http://localhost:8998", "dev")
    db = conn.db("mydb")

    # Simple: dict rows
    results = db.query(
        "[:find ?name ?email :where [?e :person/name ?name] [?e :person/email ?email]]",
        row_factory=dict_row
    )

    # Full model system
    class Person(DatomicModel):
        name: str = Field(":person/name")
        email: str = Field(":person/email")
        friends: list[int] = Field(":person/friends", cardinality=MANY, ref=True)

    entity = db.entity(123)
    person = Person.from_entity(entity)
"""

from datomic_py.serialization.converters import (
    CompiledConverter,
    TypeConverter,
    default_converter,
)
from datomic_py.serialization.factories import (
    CleanDictEntityFactory,
    DataclassEntityFactory,
    DataclassRowFactory,
    EntityFactory,
    NamedTupleRowFactory,
    RowFactory,
    clean_dict_entity,
    dataclass_entity,
    dataclass_row,
    dict_entity,
    dict_row,
    namedtuple_row,
    tuple_row,
)
from datomic_py.serialization.models import (
    MANY,
    ONE,
    Cardinality,
    DatomicModel,
    Field,
    FieldDescriptor,
    LazyRef,
    RefStrategy,
)
from datomic_py.serialization.registry import (
    ModelRegistry,
    model_registry,
    register_model,
)

__all__ = [
    # Row factories
    "RowFactory",
    "tuple_row",
    "dict_row",
    "namedtuple_row",
    "dataclass_row",
    "NamedTupleRowFactory",
    "DataclassRowFactory",
    # Entity factories
    "EntityFactory",
    "dict_entity",
    "clean_dict_entity",
    "dataclass_entity",
    "CleanDictEntityFactory",
    "DataclassEntityFactory",
    # Models
    "DatomicModel",
    "Field",
    "FieldDescriptor",
    "Cardinality",
    "ONE",
    "MANY",
    "RefStrategy",
    "LazyRef",
    # Type conversion
    "TypeConverter",
    "CompiledConverter",
    "default_converter",
    # Registry
    "ModelRegistry",
    "model_registry",
    "register_model",
]
