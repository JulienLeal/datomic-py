"""
Async integration tests for datomic_py with a real Datomic database.

These tests use testcontainers to spin up a Datomic Pro instance
with a REST API server for testing.

To run these tests:
    pip install testcontainers
    DOCKER_HOST=... TESTCONTAINERS_RYUK_DISABLED=true pytest tests/test_async_integration.py -v

Note: These tests require Docker to be running.
"""

import pytest

from datomic_py import MANY, ONE, STRING, AsyncDatomic, Attribute, Schema

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def async_conn(datomic_container):
    """Provide an async Datomic connection."""
    return AsyncDatomic(datomic_container.get_rest_url(), datomic_container.get_storage_alias())


@pytest.fixture(scope="module")
def async_db(async_conn, event_loop):
    """
    Provide an async database for testing.

    Uses a single database for all tests to avoid resource issues.
    """

    async def create_db():
        return await async_conn.create_database("async-test-db")

    return event_loop.run_until_complete(create_db())


@pytest.fixture(scope="module")
def event_loop():
    """Create an event loop for the module scope."""
    import asyncio

    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


class TestAsyncDatomicIntegration:
    """Async integration tests for basic Datomic operations."""

    @pytest.mark.asyncio
    async def test_transact_schema(self, async_db):
        """Test transacting a schema."""
        # Define a simple schema
        schema = Schema(
            Attribute(":async_person/name", STRING, cardinality=ONE),
            Attribute(":async_person/email", STRING, cardinality=ONE),
        )

        result = await async_db.transact(schema)

        # Verify the transaction was successful
        assert result is not None
        assert ":db-after" in result
        assert ":tx-data" in result

    @pytest.mark.asyncio
    async def test_transact_data(self, async_db):
        """Test transacting data."""
        # First, ensure we have a schema attribute
        schema = Schema(
            Attribute(":async_test/name", STRING, cardinality=ONE),
        )
        await async_db.transact(schema)

        # Then transact some data
        result = await async_db.transact(
            ['{:db/id #db/id[:db.part/user] :async_test/name "AsyncAlice"}']
        )

        assert result is not None
        assert ":db-after" in result
        assert ":tempids" in result

    @pytest.mark.asyncio
    async def test_query(self, async_db):
        """Test querying data."""
        # Create schema
        schema = Schema(
            Attribute(":async_user/name", STRING, cardinality=ONE),
        )
        await async_db.transact(schema)

        # Insert data
        await async_db.transact(['{:db/id #db/id[:db.part/user] :async_user/name "AsyncBob"}'])
        await async_db.transact(['{:db/id #db/id[:db.part/user] :async_user/name "AsyncCharlie"}'])

        # Query the data
        result = await async_db.query("[:find ?n :where [?e :async_user/name ?n]]")

        assert result is not None
        # Result should be a tuple of tuples
        names = {row[0] for row in result}
        assert "AsyncBob" in names
        assert "AsyncCharlie" in names

    @pytest.mark.asyncio
    async def test_query_with_input(self, async_db):
        """Test querying with input parameters."""
        # Create schema
        schema = Schema(
            Attribute(":async_item/name", STRING, cardinality=ONE),
        )
        await async_db.transact(schema)

        # Insert data
        await async_db.transact(['{:db/id #db/id[:db.part/user] :async_item/name "AsyncWidget"}'])

        # Query with input
        result = await async_db.query(
            '[:find ?e :in $ ?name :where [?e :async_item/name ?name]]',
            extra_args=['"AsyncWidget"']
        )

        assert result is not None
        assert len(result) >= 1

    @pytest.mark.asyncio
    async def test_entity_retrieval(self, async_db):
        """Test retrieving an entity by ID."""
        # Create schema
        schema = Schema(
            Attribute(":async_product/name", STRING, cardinality=ONE),
        )
        await async_db.transact(schema)

        # Insert data and get the entity ID
        result = await async_db.transact(
            ['{:db/id #db/id[:db.part/user] :async_product/name "AsyncGadget"}']
        )

        # Get the entity ID from tempids
        tempids = result.get(":tempids", {})
        if tempids:
            # Get the first tempid mapping
            eid = list(tempids.values())[0]

            # Retrieve the entity
            entity = await async_db.entity(eid)

            assert entity is not None
            assert entity.get(":async_product/name") == "AsyncGadget"


class TestAsyncSchemaIntegration:
    """Async integration tests for schema operations."""

    @pytest.mark.asyncio
    async def test_schema_with_cardinality_many(self, async_db):
        """Test schema with cardinality many."""
        schema = Schema(
            Attribute(":async_tag/name", STRING, cardinality=ONE),
            Attribute(":async_article/tags", STRING, cardinality=MANY),
        )
        result = await async_db.transact(schema)

        assert result is not None
        assert ":db-after" in result

    @pytest.mark.asyncio
    async def test_multiple_transactions(self, async_db):
        """Test multiple sequential transactions."""
        # Create schema
        schema = Schema(
            Attribute(":async_counter/value", STRING, cardinality=ONE),
        )
        await async_db.transact(schema)

        # Multiple transactions
        for i in range(5):
            await async_db.transact(
                [f'{{:db/id #db/id[:db.part/user] :async_counter/value "async_{i}"}}']
            )

        # Query all values
        result = await async_db.query("[:find ?v :where [?e :async_counter/value ?v]]")

        assert len(result) == 5


class TestAsyncQueryIntegration:
    """Async integration tests for query operations."""

    @pytest.mark.asyncio
    async def test_find_entity_ids(self, async_db):
        """Test finding entity IDs."""
        schema = Schema(
            Attribute(":async_record/type", STRING, cardinality=ONE),
        )
        await async_db.transact(schema)

        await async_db.transact(['{:db/id #db/id[:db.part/user] :async_record/type "async_alpha"}'])
        await async_db.transact(['{:db/id #db/id[:db.part/user] :async_record/type "async_beta"}'])

        result = await async_db.query("[:find ?e :where [?e :async_record/type]]")

        assert result is not None
        assert len(result) >= 2
        # Each result should be a tuple with an entity ID
        for row in result:
            assert isinstance(row[0], int)

    @pytest.mark.asyncio
    async def test_find_tuples(self, async_db):
        """Test finding tuples of values."""
        schema = Schema(
            Attribute(":async_pair/key", STRING, cardinality=ONE),
            Attribute(":async_pair/value", STRING, cardinality=ONE),
        )
        await async_db.transact(schema)

        await async_db.transact([
            '{:db/id #db/id[:db.part/user] :async_pair/key "async_k1" '
            ':async_pair/value "async_v1"}'
        ])

        result = await async_db.query(
            "[:find ?k ?v :where [?e :async_pair/key ?k] [?e :async_pair/value ?v]]"
        )

        assert result is not None
        assert len(result) >= 1
        # Check that async_k1, async_v1 pair exists
        pairs = {(row[0], row[1]) for row in result}
        assert ("async_k1", "async_v1") in pairs
