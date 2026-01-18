"""Tests for the serialization module."""

import importlib.util
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from datomic_py.serialization import (
    MANY,
    ONE,
    Cardinality,
    CompiledConverter,
    DatomicModel,
    Field,
    FieldDescriptor,
    LazyRef,
    ModelRegistry,
    RefStrategy,
    TypeConverter,
    clean_dict_entity,
    dataclass_entity,
    dataclass_row,
    default_converter,
    dict_entity,
    dict_row,
    model_registry,
    namedtuple_row,
    register_model,
    tuple_row,
)


class TestTypeConverter:
    """Tests for TypeConverter class."""

    def test_default_converters_registered(self):
        """Test that default converters are registered."""
        converter = TypeConverter()
        assert converter.get_converter(":db.type/string") is not None
        assert converter.get_converter(":db.type/boolean") is not None
        assert converter.get_converter(":db.type/long") is not None
        assert converter.get_converter(":db.type/instant") is not None
        assert converter.get_converter(":db.type/uuid") is not None
        assert converter.get_converter(":db.type/ref") is not None

    def test_convert_string(self):
        """Test converting string values."""
        converter = TypeConverter()
        assert converter.convert("hello", ":db.type/string") == "hello"

    def test_convert_boolean(self):
        """Test converting boolean values."""
        converter = TypeConverter()
        assert converter.convert(True, ":db.type/boolean") is True
        assert converter.convert(False, ":db.type/boolean") is False
        assert converter.convert("true", ":db.type/boolean") is True
        assert converter.convert("false", ":db.type/boolean") is False

    def test_convert_long(self):
        """Test converting long values."""
        converter = TypeConverter()
        assert converter.convert(42, ":db.type/long") == 42
        assert converter.convert("42", ":db.type/long") == 42

    def test_convert_float(self):
        """Test converting float values."""
        converter = TypeConverter()
        assert converter.convert(3.14, ":db.type/float") == 3.14
        assert converter.convert("3.14", ":db.type/float") == 3.14

    def test_convert_decimal(self):
        """Test converting decimal values."""
        converter = TypeConverter()
        result = converter.convert("3.14159", ":db.type/bigdec")
        assert isinstance(result, Decimal)
        assert result == Decimal("3.14159")

    def test_convert_instant_datetime(self):
        """Test converting instant values that are already datetime."""
        converter = TypeConverter()
        dt = datetime(2023, 1, 15, 10, 30, 0, tzinfo=UTC)
        result = converter.convert(dt, ":db.type/instant")
        assert result == dt

    def test_convert_instant_string(self):
        """Test converting instant values from string."""
        converter = TypeConverter()
        result = converter.convert("2023-01-15T10:30:00Z", ":db.type/instant")
        assert isinstance(result, datetime)
        assert result.year == 2023
        assert result.month == 1
        assert result.day == 15

    def test_convert_uuid(self):
        """Test converting UUID values."""
        converter = TypeConverter()
        uuid = UUID("550e8400-e29b-41d4-a716-446655440000")
        result = converter.convert(uuid, ":db.type/uuid")
        assert result == uuid

    def test_convert_uuid_string(self):
        """Test converting UUID values from string."""
        converter = TypeConverter()
        result = converter.convert("550e8400-e29b-41d4-a716-446655440000", ":db.type/uuid")
        assert isinstance(result, UUID)
        assert str(result) == "550e8400-e29b-41d4-a716-446655440000"

    def test_convert_none(self):
        """Test converting None values."""
        converter = TypeConverter()
        assert converter.convert(None, ":db.type/string") is None
        assert converter.convert(None, ":db.type/long") is None

    def test_convert_unknown_type(self):
        """Test converting with unknown type returns value unchanged."""
        converter = TypeConverter()
        assert converter.convert("hello", ":db.type/unknown") == "hello"

    def test_register_custom_converter(self):
        """Test registering a custom converter."""
        converter = TypeConverter()
        converter.register(":custom/type", lambda x: f"custom:{x}")
        assert converter.convert("value", ":custom/type") == "custom:value"

    def test_get_datomic_type(self):
        """Test getting Datomic type for Python type."""
        converter = TypeConverter()
        # Note: keyword is registered after string, so str maps to keyword
        assert converter.get_datomic_type(str) == ":db.type/keyword"
        # long is registered after bigint, so int maps to ref (last one)
        assert converter.get_datomic_type(int) == ":db.type/ref"
        assert converter.get_datomic_type(bool) == ":db.type/boolean"
        assert converter.get_datomic_type(Decimal) == ":db.type/bigdec"


class TestCompiledConverter:
    """Tests for CompiledConverter class."""

    def test_convert_row(self):
        """Test converting a row dict."""
        compiled = CompiledConverter({
            "name": ":db.type/string",
            "age": ":db.type/long",
        })
        row = {"name": "Alice", "age": "30"}
        result = compiled.convert_row(row)
        assert result["name"] == "Alice"
        assert result["age"] == 30

    def test_convert_row_with_missing_field(self):
        """Test converting a row with missing fields."""
        compiled = CompiledConverter({
            "name": ":db.type/string",
            "age": ":db.type/long",
        })
        row = {"name": "Alice"}
        result = compiled.convert_row(row)
        assert result["name"] == "Alice"
        assert "age" not in result

    def test_convert_value(self):
        """Test converting a single value."""
        compiled = CompiledConverter({
            "age": ":db.type/long",
        })
        assert compiled.convert_value("age", "30") == 30
        assert compiled.convert_value("unknown", "value") == "value"
        assert compiled.convert_value("age", None) is None


class TestRowFactories:
    """Tests for row factory functions."""

    def test_tuple_row(self):
        """Test tuple_row returns raw tuple."""
        row = (1, "Alice", "alice@example.com")
        columns = ["id", "name", "email"]
        result = tuple_row(row, columns)
        assert result == row

    def test_dict_row(self):
        """Test dict_row converts to dict."""
        row = (1, "Alice", "alice@example.com")
        columns = ["id", "name", "email"]
        result = dict_row(row, columns)
        assert result == {"id": 1, "name": "Alice", "email": "alice@example.com"}

    def test_namedtuple_row(self):
        """Test namedtuple_row creates namedtuple."""
        factory = namedtuple_row("Person")
        row = (1, "Alice")
        columns = ["id", "name"]
        result = factory(row, columns)
        assert result.id == 1
        assert result.name == "Alice"
        assert result[0] == 1
        assert result[1] == "Alice"

    def test_namedtuple_row_sanitizes_columns(self):
        """Test namedtuple_row sanitizes column names."""
        factory = namedtuple_row("Person")
        row = (1, "Alice")
        columns = ["?id", ":person/name"]
        result = factory(row, columns)
        assert result.id == 1
        assert result.person_name == "Alice"

    def test_namedtuple_row_caches_class(self):
        """Test namedtuple_row caches the namedtuple class."""
        factory = namedtuple_row("Person")
        columns = ["id", "name"]
        result1 = factory((1, "Alice"), columns)
        result2 = factory((2, "Bob"), columns)
        assert type(result1) is type(result2)

    def test_dataclass_row(self):
        """Test dataclass_row creates dataclass instance."""
        @dataclass
        class Person:
            id: int
            name: str

        factory = dataclass_row(Person)
        row = (1, "Alice")
        columns = ["id", "name"]
        result = factory(row, columns)
        assert isinstance(result, Person)
        assert result.id == 1
        assert result.name == "Alice"

    def test_dataclass_row_with_mapping(self):
        """Test dataclass_row with field mapping."""
        @dataclass
        class Person:
            person_id: int
            person_name: str

        factory = dataclass_row(Person, {"id": "person_id", "name": "person_name"})
        row = (1, "Alice")
        columns = ["id", "name"]
        result = factory(row, columns)
        assert result.person_id == 1
        assert result.person_name == "Alice"

    def test_dataclass_row_sanitizes_columns(self):
        """Test dataclass_row sanitizes column names."""
        @dataclass
        class Person:
            person_name: str
            person_email: str

        factory = dataclass_row(Person)
        row = ("Alice", "alice@example.com")
        columns = [":person/name", ":person/email"]
        result = factory(row, columns)
        assert result.person_name == "Alice"
        assert result.person_email == "alice@example.com"

    def test_dataclass_row_not_dataclass_raises(self):
        """Test dataclass_row raises for non-dataclass."""
        class NotDataclass:
            pass

        with pytest.raises(TypeError, match="is not a dataclass"):
            dataclass_row(NotDataclass)


class TestEntityFactories:
    """Tests for entity factory functions."""

    def test_dict_entity(self):
        """Test dict_entity returns raw dict."""
        entity = {":db/id": 123, ":person/name": "Alice"}
        result = dict_entity(entity)
        assert result == entity

    def test_clean_dict_entity_default(self):
        """Test clean_dict_entity without namespace."""
        factory = clean_dict_entity()
        entity = {":db/id": 123, ":person/name": "Alice", ":person/email": "alice@example.com"}
        result = factory(entity)
        # :db/id becomes "id" (namespace "db" stripped when include_namespace=False)
        assert result == {"id": 123, "name": "Alice", "email": "alice@example.com"}

    def test_clean_dict_entity_with_namespace(self):
        """Test clean_dict_entity with namespace included."""
        factory = clean_dict_entity(include_namespace=True)
        entity = {":db/id": 123, ":person/name": "Alice"}
        result = factory(entity)
        assert result == {"db_id": 123, "person_name": "Alice"}

    def test_clean_dict_entity_custom_transform(self):
        """Test clean_dict_entity with custom key transform."""
        factory = clean_dict_entity(
            key_transform=lambda k: k.upper().replace(":", "").replace("/", "_")
        )
        entity = {":person/name": "Alice"}
        result = factory(entity)
        assert result == {"PERSON_NAME": "Alice"}

    def test_dataclass_entity(self):
        """Test dataclass_entity creates dataclass instance."""
        @dataclass
        class Person:
            name: str
            email: str

        factory = dataclass_entity(Person, {"name": ":person/name", "email": ":person/email"})
        entity = {":db/id": 123, ":person/name": "Alice", ":person/email": "alice@example.com"}
        result = factory(entity)
        assert isinstance(result, Person)
        assert result.name == "Alice"
        assert result.email == "alice@example.com"

    def test_dataclass_entity_auto_mapping(self):
        """Test dataclass_entity with auto-derived field names."""
        @dataclass
        class Person:
            person_name: str
            person_email: str

        factory = dataclass_entity(Person)
        entity = {":person/name": "Alice", ":person/email": "alice@example.com"}
        result = factory(entity)
        assert result.person_name == "Alice"
        assert result.person_email == "alice@example.com"

    def test_dataclass_entity_strict_mode(self):
        """Test dataclass_entity in strict mode."""
        @dataclass
        class Person:
            name: str

        factory = dataclass_entity(Person, strict=True)
        entity = {":person/name": "Alice", ":person/unknown": "value"}
        with pytest.raises(ValueError, match="No field for Datomic attribute"):
            factory(entity)

    def test_dataclass_entity_not_dataclass_raises(self):
        """Test dataclass_entity raises for non-dataclass."""
        class NotDataclass:
            pass

        with pytest.raises(TypeError, match="is not a dataclass"):
            dataclass_entity(NotDataclass)


class TestFieldDescriptor:
    """Tests for FieldDescriptor dataclass."""

    def test_field_descriptor_defaults(self):
        """Test FieldDescriptor default values."""
        field = Field(":person/name")
        assert isinstance(field, FieldDescriptor)
        assert field.attr == ":person/name"
        assert field.cardinality == Cardinality.ONE
        assert field.ref is False
        assert field.ref_model is None
        assert field.ref_strategy == RefStrategy.ID_ONLY
        assert field.converter is None
        assert field.default is None
        assert field.default_factory is None

    def test_field_descriptor_cardinality_many(self):
        """Test FieldDescriptor with cardinality many."""
        field = Field(":person/tags", cardinality=MANY)
        assert field.cardinality == Cardinality.MANY

    def test_field_descriptor_ref(self):
        """Test FieldDescriptor for reference fields."""
        field = Field(":article/author", ref=True, ref_model="Person")
        assert field.ref is True
        assert field.ref_model == "Person"

    def test_field_descriptor_converter(self):
        """Test FieldDescriptor with custom converter."""
        field = Field(":person/name", converter=str.upper)
        assert field.converter is not None
        assert field.converter("hello") == "HELLO"

    def test_field_descriptor_default(self):
        """Test FieldDescriptor with default value."""
        field = Field(":person/status", default="active")
        assert field.default == "active"

    def test_field_descriptor_default_factory(self):
        """Test FieldDescriptor with default factory."""
        field = Field(":person/tags", default_factory=list)
        assert field.default_factory is not None
        assert field.default_factory() == []


class TestDatomicModel:
    """Tests for DatomicModel base class."""

    def test_model_definition(self):
        """Test basic model definition."""
        class Person(DatomicModel):
            name: str = Field(":person/name")
            email: str = Field(":person/email")

        assert "name" in Person.__datomic_fields__
        assert "email" in Person.__datomic_fields__
        assert Person.__datomic_fields__["name"].attr == ":person/name"

    def test_model_init(self):
        """Test model initialization."""
        class Person(DatomicModel):
            name: str = Field(":person/name")
            email: str = Field(":person/email", default="")

        person = Person(name="Alice")
        assert person.name == "Alice"
        assert person.email == ""

    def test_model_init_with_db_id(self):
        """Test model initialization with db_id."""
        class Person(DatomicModel):
            name: str = Field(":person/name")

        person = Person(name="Alice", db_id=123)
        assert person.db_id == 123

    def test_model_from_entity(self):
        """Test creating model from entity dict."""
        class Person(DatomicModel):
            name: str = Field(":person/name")
            email: str = Field(":person/email")

        entity = {
            ":db/id": 123,
            ":person/name": "Alice",
            ":person/email": "alice@example.com",
        }
        person = Person.from_entity(entity)
        assert person.db_id == 123
        assert person.name == "Alice"
        assert person.email == "alice@example.com"

    def test_model_from_entity_with_converter(self):
        """Test model from entity with custom converter."""
        class Person(DatomicModel):
            name: str = Field(":person/name", converter=str.upper)

        entity = {":person/name": "alice"}
        person = Person.from_entity(entity)
        assert person.name == "ALICE"

    def test_model_from_entity_cardinality_many(self):
        """Test model from entity with cardinality many."""
        class Article(DatomicModel):
            tags: list[str] = Field(":article/tags", cardinality=MANY)

        # Single value should become list
        entity = {":article/tags": "python"}
        article = Article.from_entity(entity)
        assert article.tags == ["python"]

        # Multiple values should remain list
        entity = {":article/tags": ("python", "datomic")}
        article = Article.from_entity(entity)
        assert article.tags == ["python", "datomic"]

    def test_model_from_entity_ref_id_only(self):
        """Test model from entity with ref (ID only strategy)."""
        class Article(DatomicModel):
            author: int = Field(":article/author", ref=True, ref_strategy=RefStrategy.ID_ONLY)

        # Value is entity ID
        entity = {":article/author": 456}
        article = Article.from_entity(entity)
        assert article.author == 456

        # Value is dict with :db/id
        entity = {":article/author": {":db/id": 789}}
        article = Article.from_entity(entity)
        assert article.author == 789

    def test_model_from_row(self):
        """Test creating model from query row."""
        class Person(DatomicModel):
            name: str = Field(":person/name")
            email: str = Field(":person/email")

        row = ("Alice", "alice@example.com")
        columns = (":person/name", ":person/email")
        person = Person.from_row(row, columns)
        assert person.name == "Alice"
        assert person.email == "alice@example.com"

    def test_model_to_dict(self):
        """Test converting model to dict."""
        class Person(DatomicModel):
            name: str = Field(":person/name")
            email: str = Field(":person/email")

        person = Person(name="Alice", email="alice@example.com", db_id=123)
        result = person.to_dict()
        assert result == {
            ":db/id": 123,
            ":person/name": "Alice",
            ":person/email": "alice@example.com",
        }

    def test_model_to_dict_exclude_none(self):
        """Test converting model to dict excluding None values."""
        class Person(DatomicModel):
            name: str = Field(":person/name")
            email: str = Field(":person/email")

        person = Person(name="Alice")
        result = person.to_dict()
        assert result == {":person/name": "Alice"}

    def test_model_to_dict_include_none(self):
        """Test converting model to dict including None values."""
        class Person(DatomicModel):
            name: str = Field(":person/name")
            email: str = Field(":person/email")

        person = Person(name="Alice")
        result = person.to_dict(include_none=True)
        assert result == {":person/name": "Alice", ":person/email": None}

    def test_model_repr(self):
        """Test model string representation."""
        class Person(DatomicModel):
            name: str = Field(":person/name")

        person = Person(name="Alice", db_id=123)
        assert "Person" in repr(person)
        assert "Alice" in repr(person)
        assert "123" in repr(person)

    def test_model_equality_by_id(self):
        """Test model equality by db_id."""
        class Person(DatomicModel):
            name: str = Field(":person/name")

        p1 = Person(name="Alice", db_id=123)
        p2 = Person(name="Alice", db_id=123)
        p3 = Person(name="Alice", db_id=456)
        assert p1 == p2
        assert p1 != p3

    def test_model_equality_by_fields(self):
        """Test model equality by fields when no db_id."""
        class Person(DatomicModel):
            name: str = Field(":person/name")

        p1 = Person(name="Alice")
        p2 = Person(name="Alice")
        p3 = Person(name="Bob")
        assert p1 == p2
        assert p1 != p3

    def test_model_hash(self):
        """Test model hashing."""
        class Person(DatomicModel):
            name: str = Field(":person/name")

        p1 = Person(name="Alice", db_id=123)
        p2 = Person(name="Alice", db_id=123)
        assert hash(p1) == hash(p2)

        # Can be used in sets
        people = {p1, p2}
        assert len(people) == 1

    def test_model_inheritance(self):
        """Test model inheritance."""
        class Entity(DatomicModel):
            name: str = Field(":entity/name")

        class Person(Entity):
            email: str = Field(":person/email")

        assert "name" in Person.__datomic_fields__
        assert "email" in Person.__datomic_fields__

    def test_model_with_default_factory(self):
        """Test model with default factory."""
        class Person(DatomicModel):
            tags: list[str] = Field(":person/tags", default_factory=list)

        p1 = Person()
        p2 = Person()
        assert p1.tags == []
        assert p2.tags == []
        # Different instances
        p1.tags.append("test")
        assert p2.tags == []


class TestLazyRef:
    """Tests for LazyRef class."""

    def test_lazy_ref_id_from_int(self):
        """Test LazyRef with integer id."""
        ref = LazyRef(123, None, None)
        assert ref.id == 123

    def test_lazy_ref_id_from_dict(self):
        """Test LazyRef with dict id."""
        ref = LazyRef({":db/id": 456}, None, None)
        assert ref.id == 456

    def test_lazy_ref_repr(self):
        """Test LazyRef string representation."""
        ref = LazyRef(123, None, None)
        assert "LazyRef" in repr(ref)
        assert "123" in repr(ref)

    def test_lazy_ref_resolve_no_db(self):
        """Test LazyRef resolve without database raises."""
        ref = LazyRef(123, None, None)
        with pytest.raises(ValueError, match="No database connection"):
            ref.resolve()


class TestModelRegistry:
    """Tests for ModelRegistry class."""

    def test_register_and_get(self):
        """Test registering and getting a model."""
        registry = ModelRegistry()

        class Person(DatomicModel):
            name: str = Field(":person/name")

        registry.register(Person)
        assert registry.get("Person") is Person

    def test_register_with_namespace(self):
        """Test registering model with namespace."""
        registry = ModelRegistry()

        class Person(DatomicModel):
            __namespace__ = "person"
            name: str = Field(":person/name")

        registry.register(Person)
        assert registry.get_by_namespace("person") is Person

    def test_get_unknown_returns_none(self):
        """Test getting unknown model returns None."""
        registry = ModelRegistry()
        assert registry.get("Unknown") is None

    def test_all_models(self):
        """Test getting all registered models."""
        registry = ModelRegistry()

        class Person(DatomicModel):
            name: str = Field(":person/name")

        class Article(DatomicModel):
            title: str = Field(":article/title")

        registry.register(Person)
        registry.register(Article)
        all_models = registry.all_models()
        assert Person in all_models
        assert Article in all_models

    def test_clear(self):
        """Test clearing registry."""
        registry = ModelRegistry()

        class Person(DatomicModel):
            name: str = Field(":person/name")

        registry.register(Person)
        registry.clear()
        assert registry.get("Person") is None

    def test_register_model_decorator(self):
        """Test @register_model decorator."""
        # Clear global registry first
        model_registry.clear()

        @register_model
        class Person(DatomicModel):
            name: str = Field(":person/name")

        assert model_registry.get("Person") is Person


class TestCardinality:
    """Tests for Cardinality enum."""

    def test_cardinality_values(self):
        """Test cardinality enum values."""
        assert ONE == Cardinality.ONE
        assert MANY == Cardinality.MANY
        assert ONE.value == ":db.cardinality/one"
        assert MANY.value == ":db.cardinality/many"


class TestRefStrategy:
    """Tests for RefStrategy enum."""

    def test_ref_strategy_values(self):
        """Test ref strategy enum values."""
        assert RefStrategy.ID_ONLY.value == "id"
        assert RefStrategy.EAGER.value == "eager"
        assert RefStrategy.LAZY.value == "lazy"


class TestDefaultConverter:
    """Tests for the default_converter instance."""

    def test_default_converter_is_type_converter(self):
        """Test that default_converter is a TypeConverter."""
        assert isinstance(default_converter, TypeConverter)

    def test_default_converter_has_defaults(self):
        """Test that default_converter has default converters."""
        assert default_converter.get_converter(":db.type/string") is not None
        assert default_converter.get_converter(":db.type/long") is not None


class TestFactoryProtocols:
    """Tests for factory protocol compliance."""

    def test_dict_row_matches_protocol(self):
        """Test dict_row matches RowFactory protocol."""
        from datomic_py.serialization.factories import RowFactory
        assert isinstance(dict_row, RowFactory)

    def test_dict_entity_matches_protocol(self):
        """Test dict_entity matches EntityFactory protocol."""
        from datomic_py.serialization.factories import EntityFactory
        assert isinstance(dict_entity, EntityFactory)


class TestEdgeCases:
    """Tests for edge cases."""

    def test_model_with_no_fields(self):
        """Test model with no fields."""
        class EmptyModel(DatomicModel):
            pass

        model = EmptyModel()
        assert model.to_dict() == {}

    def test_model_missing_entity_attribute(self):
        """Test model from entity with missing attribute."""
        class Person(DatomicModel):
            name: str = Field(":person/name")
            email: str = Field(":person/email")

        entity = {":person/name": "Alice"}
        person = Person.from_entity(entity)
        assert person.name == "Alice"
        assert person.email is None

    def test_empty_row(self):
        """Test factories with empty row."""
        result = dict_row((), ())
        assert result == {}

    def test_empty_entity(self):
        """Test factories with empty entity."""
        result = dict_entity({})
        assert result == {}

    def test_row_column_mismatch(self):
        """Test dict_row with mismatched row/columns."""
        with pytest.raises(ValueError):
            dict_row((1, 2), ["a"])  # strict=True in zip


# Check if Pydantic is available
HAS_PYDANTIC = importlib.util.find_spec("pydantic") is not None


@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
class TestPydanticSupport:
    """Tests for Pydantic model support."""

    def test_pydantic_row_factory(self):
        """Test PydanticRowFactory creates Pydantic model instances."""
        from pydantic import BaseModel

        from datomic_py.serialization.pydantic_support import pydantic_row

        class Person(BaseModel):
            name: str
            email: str

        factory = pydantic_row(Person)
        row = ("Alice", "alice@example.com")
        columns = ["name", "email"]
        result = factory(row, columns)

        assert isinstance(result, Person)
        assert result.name == "Alice"
        assert result.email == "alice@example.com"

    def test_pydantic_row_factory_with_mapping(self):
        """Test PydanticRowFactory with field mapping."""
        from pydantic import BaseModel

        from datomic_py.serialization.pydantic_support import pydantic_row

        class Person(BaseModel):
            person_name: str
            person_email: str

        factory = pydantic_row(Person, {"name": "person_name", "email": "person_email"})
        row = ("Alice", "alice@example.com")
        columns = ["name", "email"]
        result = factory(row, columns)

        assert result.person_name == "Alice"
        assert result.person_email == "alice@example.com"

    def test_pydantic_row_factory_sanitizes_columns(self):
        """Test PydanticRowFactory sanitizes column names."""
        from pydantic import BaseModel

        from datomic_py.serialization.pydantic_support import pydantic_row

        class Person(BaseModel):
            person_name: str
            person_email: str

        factory = pydantic_row(Person)
        row = ("Alice", "alice@example.com")
        columns = [":person/name", ":person/email"]
        result = factory(row, columns)

        assert result.person_name == "Alice"
        assert result.person_email == "alice@example.com"

    def test_pydantic_entity_factory(self):
        """Test PydanticEntityFactory creates Pydantic model instances."""
        from pydantic import BaseModel

        from datomic_py.serialization.pydantic_support import pydantic_entity

        class Person(BaseModel):
            name: str
            email: str

        factory = pydantic_entity(Person, {"name": ":person/name", "email": ":person/email"})
        entity = {":db/id": 123, ":person/name": "Alice", ":person/email": "alice@example.com"}
        result = factory(entity)

        assert isinstance(result, Person)
        assert result.name == "Alice"
        assert result.email == "alice@example.com"

    def test_pydantic_entity_factory_auto_mapping(self):
        """Test PydanticEntityFactory with auto-derived field names."""
        from pydantic import BaseModel

        from datomic_py.serialization.pydantic_support import pydantic_entity

        class Person(BaseModel):
            person_name: str
            person_email: str

        factory = pydantic_entity(Person)
        entity = {":person/name": "Alice", ":person/email": "alice@example.com"}
        result = factory(entity)

        assert result.person_name == "Alice"
        assert result.person_email == "alice@example.com"

    def test_pydantic_entity_factory_no_validate(self):
        """Test PydanticEntityFactory with validation disabled."""
        from pydantic import BaseModel

        from datomic_py.serialization.pydantic_support import pydantic_entity

        class Person(BaseModel):
            name: str
            email: str

        factory = pydantic_entity(
            Person, {"name": ":person/name", "email": ":person/email"}, validate=False
        )
        entity = {":person/name": "Alice", ":person/email": "alice@example.com"}
        result = factory(entity)

        assert result.name == "Alice"
        assert result.email == "alice@example.com"

    def test_pydantic_entity_factory_skips_db_id(self):
        """Test PydanticEntityFactory skips :db/id by default."""
        from pydantic import BaseModel

        from datomic_py.serialization.pydantic_support import pydantic_entity

        class Person(BaseModel):
            name: str

        factory = pydantic_entity(Person, {"name": ":person/name"})
        entity = {":db/id": 123, ":person/name": "Alice"}
        result = factory(entity)

        assert result.name == "Alice"
        assert not hasattr(result, "db_id")


class TestPydanticSupportImportError:
    """Tests for Pydantic support when Pydantic is not installed."""

    def test_has_pydantic_flag(self):
        """Test HAS_PYDANTIC flag reflects installation status."""
        from datomic_py.serialization.pydantic_support import HAS_PYDANTIC as module_has_pydantic

        # Should match our local check
        assert module_has_pydantic == HAS_PYDANTIC
