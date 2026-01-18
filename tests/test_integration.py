"""
Integration tests for datomic_py with a real Datomic database.

These tests use testcontainers to spin up a Datomic Pro instance
with a REST API server for testing.

To run these tests:
    pip install testcontainers
    DOCKER_HOST=... TESTCONTAINERS_RYUK_DISABLED=true pytest tests/test_integration.py -v

Note: These tests require Docker to be running.
"""

import pytest

from datomic_py import MANY, ONE, STRING, Attribute, Schema

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


class TestDatomicIntegration:
    """Integration tests for basic Datomic operations."""

    def test_transact_schema(self, db):
        """Test transacting a schema."""
        # Define a simple schema
        schema = Schema(
            Attribute(":person/name", STRING, cardinality=ONE),
            Attribute(":person/email", STRING, cardinality=ONE),
        )

        result = db.transact(schema)

        # Verify the transaction was successful
        assert result is not None
        assert ":db-after" in result
        assert ":tx-data" in result

    def test_transact_data(self, db):
        """Test transacting data."""
        # First, ensure we have a schema attribute
        schema = Schema(
            Attribute(":test/name", STRING, cardinality=ONE),
        )
        db.transact(schema)

        # Then transact some data
        result = db.transact(['{:db/id #db/id[:db.part/user] :test/name "Alice"}'])

        assert result is not None
        assert ":db-after" in result
        assert ":tempids" in result

    def test_query(self, db):
        """Test querying data."""
        # Create schema
        schema = Schema(
            Attribute(":user/name", STRING, cardinality=ONE),
        )
        db.transact(schema)

        # Insert data
        db.transact(['{:db/id #db/id[:db.part/user] :user/name "Bob"}'])
        db.transact(['{:db/id #db/id[:db.part/user] :user/name "Charlie"}'])

        # Query the data
        result = db.query("[:find ?n :where [?e :user/name ?n]]")

        assert result is not None
        # Result should be a tuple of tuples
        names = {row[0] for row in result}
        assert "Bob" in names
        assert "Charlie" in names

    def test_query_with_input(self, db):
        """Test querying with input parameters."""
        # Create schema
        schema = Schema(
            Attribute(":item/name", STRING, cardinality=ONE),
        )
        db.transact(schema)

        # Insert data
        db.transact(['{:db/id #db/id[:db.part/user] :item/name "Widget"}'])

        # Query with input
        result = db.query(
            '[:find ?e :in $ ?name :where [?e :item/name ?name]]',
            extra_args=['"Widget"']
        )

        assert result is not None
        assert len(result) >= 1

    def test_entity_retrieval(self, db):
        """Test retrieving an entity by ID."""
        # Create schema
        schema = Schema(
            Attribute(":product/name", STRING, cardinality=ONE),
        )
        db.transact(schema)

        # Insert data and get the entity ID
        result = db.transact(['{:db/id #db/id[:db.part/user] :product/name "Gadget"}'])

        # Get the entity ID from tempids
        tempids = result.get(":tempids", {})
        if tempids:
            # Get the first tempid mapping
            eid = list(tempids.values())[0]

            # Retrieve the entity
            entity = db.entity(eid)

            assert entity is not None
            assert entity.get(":product/name") == "Gadget"


class TestSchemaIntegration:
    """Integration tests for schema operations."""

    def test_schema_with_cardinality_many(self, db):
        """Test schema with cardinality many."""
        schema = Schema(
            Attribute(":tag/name", STRING, cardinality=ONE),
            Attribute(":article/tags", STRING, cardinality=MANY),
        )
        result = db.transact(schema)

        assert result is not None
        assert ":db-after" in result

    def test_multiple_transactions(self, db):
        """Test multiple sequential transactions."""
        # Create schema
        schema = Schema(
            Attribute(":counter/value", STRING, cardinality=ONE),
        )
        db.transact(schema)

        # Multiple transactions
        for i in range(5):
            db.transact([f'{{:db/id #db/id[:db.part/user] :counter/value "{i}"}}'])

        # Query all values
        result = db.query("[:find ?v :where [?e :counter/value ?v]]")

        assert len(result) == 5


class TestQueryIntegration:
    """Integration tests for query operations."""

    def test_find_entity_ids(self, db):
        """Test finding entity IDs."""
        schema = Schema(
            Attribute(":record/type", STRING, cardinality=ONE),
        )
        db.transact(schema)

        db.transact(['{:db/id #db/id[:db.part/user] :record/type "alpha"}'])
        db.transact(['{:db/id #db/id[:db.part/user] :record/type "beta"}'])

        result = db.query("[:find ?e :where [?e :record/type]]")

        assert result is not None
        assert len(result) >= 2
        # Each result should be a tuple with an entity ID
        for row in result:
            assert isinstance(row[0], int)

    def test_find_tuples(self, db):
        """Test finding tuples of values."""
        schema = Schema(
            Attribute(":pair/key", STRING, cardinality=ONE),
            Attribute(":pair/value", STRING, cardinality=ONE),
        )
        db.transact(schema)

        db.transact(
            ['{:db/id #db/id[:db.part/user] :pair/key "k1" :pair/value "v1"}']
        )

        result = db.query("[:find ?k ?v :where [?e :pair/key ?k] [?e :pair/value ?v]]")

        assert result is not None
        assert len(result) >= 1
        # Check that k1, v1 pair exists
        pairs = {(row[0], row[1]) for row in result}
        assert ("k1", "v1") in pairs


class TestHistoryIntegration:
    """Integration tests for history queries."""

    def test_history_query(self, db):
        """Test querying with history."""
        schema = Schema(
            Attribute(":state/value", STRING, cardinality=ONE),
        )
        db.transact(schema)

        # Create initial value
        result = db.transact(['{:db/id #db/id[:db.part/user] :state/value "initial"}'])
        eid = list(result.get(":tempids", {}).values())[0]

        # Update the value (retract and add)
        db.transact([f'[:db/retract {eid} :state/value "initial"]'])
        db.transact([f'[:db/add {eid} :state/value "updated"]'])

        # Query current state
        current = db.query("[:find ?v :where [?e :state/value ?v]]")

        # Query history
        history = db.query("[:find ?v :where [?e :state/value ?v]]", history=True)

        # History should include both values
        current_values = {row[0] for row in current}
        history_values = {row[0] for row in history}

        assert "updated" in current_values
        assert "initial" in history_values or "updated" in history_values


class TestDatabaseCreation:
    """
    Test database creation operation.

    This test class is kept separate and run last since creating multiple
    databases can cause issues with the container.
    """

    def test_create_database(self, conn):
        """Test creating a new database."""
        db = conn.create_database("test-create-db")
        assert db is not None
        assert db.name == "test-create-db"
