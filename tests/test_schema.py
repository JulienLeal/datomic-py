"""Tests for the schema module."""


from pydatomic.schema import (
    BOOLEAN,
    IDENTITY,
    MANY,
    ONE,
    STRING,
    VALUE,
    Attribute,
    Schema,
)


class TestSchemaConstants:
    """Tests for schema constants."""

    def test_cardinality_constants(self):
        """Test cardinality constants are defined correctly."""
        assert ONE == ":db.cardinality/one"
        assert MANY == ":db.cardinality/many"

    def test_unique_constants(self):
        """Test uniqueness constants are defined correctly."""
        assert IDENTITY == ":db.unique/identity"
        assert VALUE == ":db.unique/value"

    def test_type_constants(self):
        """Test type constants are defined correctly."""
        assert STRING == ":db.type/string"
        assert BOOLEAN == ":db.type/boolean"


class TestAttribute:
    """Tests for Attribute function."""

    def test_basic_attribute(self):
        """Test creating a basic attribute."""
        attr = Attribute(":person/name", STRING)

        assert ":db/ident :person/name" in attr
        assert ":db/valueType :db.type/string" in attr
        assert ":db/cardinality :db.cardinality/one" in attr
        # Optional attributes should not be present when not set
        assert ":db/unique" not in attr
        assert ":db/index" not in attr
        assert ":db/fulltext" not in attr
        assert ":db/noHistory" not in attr

    def test_attribute_with_doc(self):
        """Test creating an attribute with documentation."""
        attr = Attribute(":person/name", STRING, doc='"A person\'s name"')

        assert ':db/doc "A person\'s name"' in attr

    def test_attribute_with_cardinality_many(self):
        """Test creating an attribute with cardinality many."""
        attr = Attribute(":person/aliases", STRING, cardinality=MANY)

        assert ":db/cardinality :db.cardinality/many" in attr

    def test_attribute_with_unique_identity(self):
        """Test creating an attribute with unique identity."""
        attr = Attribute(":person/email", STRING, unique=IDENTITY)

        assert ":db/unique :db.unique/identity" in attr

    def test_attribute_with_unique_value(self):
        """Test creating an attribute with unique value."""
        attr = Attribute(":person/ssn", STRING, unique=VALUE)

        assert ":db/unique :db.unique/value" in attr

    def test_attribute_with_index(self):
        """Test creating an attribute with index."""
        attr = Attribute(":person/name", STRING, index=True)

        assert ":db/index true" in attr

    def test_attribute_with_fulltext(self):
        """Test creating an attribute with fulltext search."""
        attr = Attribute(":article/content", STRING, fulltext=True)

        assert ":db/fulltext true" in attr

    def test_attribute_with_no_history(self):
        """Test creating an attribute with no history."""
        attr = Attribute(":session/token", STRING, noHistory=True)

        assert ":db/noHistory true" in attr

    def test_attribute_with_all_options(self):
        """Test creating an attribute with all options."""
        attr = Attribute(
            ":user/email",
            STRING,
            doc='"User email address"',
            cardinality=ONE,
            unique=IDENTITY,
            index=True,
            fulltext=False,
            noHistory=False,
        )

        assert ":db/ident :user/email" in attr
        assert ":db/valueType :db.type/string" in attr
        assert ':db/doc "User email address"' in attr
        assert ":db/cardinality :db.cardinality/one" in attr
        assert ":db/unique :db.unique/identity" in attr
        assert ":db/index true" in attr
        # fulltext and noHistory are False, so they shouldn't be included
        assert ":db/fulltext" not in attr
        assert ":db/noHistory" not in attr

    def test_attribute_returns_string(self):
        """Test that Attribute returns a string."""
        attr = Attribute(":test/attr", STRING)
        assert isinstance(attr, str)
        assert attr.startswith("{")
        assert attr.endswith("}")


class TestSchema:
    """Tests for Schema function."""

    def test_empty_schema(self):
        """Test creating an empty schema."""
        schema = Schema()
        assert schema == ()

    def test_single_attribute_schema(self):
        """Test creating a schema with single attribute."""
        attr = Attribute(":person/name", STRING)
        schema = Schema(attr)

        assert len(schema) == 1
        assert schema[0] == attr

    def test_multiple_attribute_schema(self):
        """Test creating a schema with multiple attributes."""
        attr1 = Attribute(":person/name", STRING)
        attr2 = Attribute(":person/active", BOOLEAN)

        schema = Schema(attr1, attr2)

        assert len(schema) == 2
        assert schema[0] == attr1
        assert schema[1] == attr2

    def test_schema_returns_tuple(self):
        """Test that Schema returns a tuple."""
        schema = Schema(
            Attribute(":task/name", STRING),
            Attribute(":task/done", BOOLEAN),
        )

        assert isinstance(schema, tuple)

    def test_schema_is_iterable(self):
        """Test that Schema result is iterable."""
        attr1 = Attribute(":a", STRING)
        attr2 = Attribute(":b", STRING)
        schema = Schema(attr1, attr2)

        attrs = list(schema)
        assert len(attrs) == 2
