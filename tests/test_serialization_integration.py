"""
Integration tests for the serialization module with a real Datomic database.

These tests verify that serialization features work correctly with actual
Datomic query and entity results.

To run these tests:
    pip install testcontainers
    DOCKER_HOST=... TESTCONTAINERS_RYUK_DISABLED=true pytest tests/test_serialization_integration.py -v

Note: These tests require Docker to be running.
"""

from dataclasses import dataclass

import pytest

from datomic_py import MANY, ONE, STRING, Attribute, Schema
from datomic_py.serialization import (
    DatomicModel,
    Field,
    clean_dict_entity,
    dataclass_entity,
    dataclass_row,
    dict_row,
    namedtuple_row,
    register_model,
)
from datomic_py.serialization.models import MANY as MANY_CARD
from datomic_py.serialization.models import RefStrategy

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


class TestRowFactoriesIntegration:
    """Integration tests for row factories with real queries."""

    @pytest.fixture(autouse=True)
    def setup_schema(self, db):
        """Set up schema for row factory tests."""
        schema = Schema(
            Attribute(":employee/name", STRING, cardinality=ONE),
            Attribute(":employee/email", STRING, cardinality=ONE),
            Attribute(":employee/department", STRING, cardinality=ONE),
        )
        db.transact(schema)

        # Insert test data
        db.transact(['{:db/id #db/id[:db.part/user] :employee/name "Alice" :employee/email "alice@example.com" :employee/department "Engineering"}'])
        db.transact(['{:db/id #db/id[:db.part/user] :employee/name "Bob" :employee/email "bob@example.com" :employee/department "Sales"}'])

    def test_dict_row_with_query(self, db):
        """Test dict_row factory with actual query."""
        results = db.query(
            "[:find ?name ?email :where [?e :employee/name ?name] [?e :employee/email ?email]]",
            row_factory=dict_row,
        )

        assert len(results) >= 2
        assert all(isinstance(row, dict) for row in results)

        # Check that dict contains expected keys
        names = {row["name"] for row in results}
        assert "Alice" in names
        assert "Bob" in names

    def test_namedtuple_row_with_query(self, db):
        """Test namedtuple_row factory with actual query."""
        factory = namedtuple_row("Employee")
        results = db.query(
            "[:find ?name ?dept :where [?e :employee/name ?name] [?e :employee/department ?dept]]",
            row_factory=factory,
        )

        assert len(results) >= 2
        # Check namedtuple attributes
        for row in results:
            assert hasattr(row, "name")
            assert hasattr(row, "dept")

        names = {row.name for row in results}
        assert "Alice" in names
        assert "Bob" in names

    def test_dataclass_row_with_query(self, db):
        """Test dataclass_row factory with actual query."""

        @dataclass
        class Employee:
            name: str
            email: str

        factory = dataclass_row(Employee)
        results = db.query(
            "[:find ?name ?email :where [?e :employee/name ?name] [?e :employee/email ?email]]",
            row_factory=factory,
        )

        assert len(results) >= 2
        assert all(isinstance(row, Employee) for row in results)

        # Check dataclass fields
        names = {row.name for row in results}
        assert "Alice" in names

    def test_columns_extracted_from_query(self, db):
        """Test that column names are extracted from :find clause."""
        results = db.query(
            "[:find ?name ?email :where [?e :employee/name ?name] [?e :employee/email ?email]]",
            row_factory=dict_row,
        )

        assert len(results) >= 1
        # Keys should be extracted from ?name and ?email
        first_row = results[0]
        assert "name" in first_row
        assert "email" in first_row


class TestEntityFactoriesIntegration:
    """Integration tests for entity factories with real entities."""

    @pytest.fixture(autouse=True)
    def setup_schema(self, db):
        """Set up schema for entity factory tests."""
        schema = Schema(
            Attribute(":customer/name", STRING, cardinality=ONE),
            Attribute(":customer/email", STRING, cardinality=ONE),
            Attribute(":customer/tier", STRING, cardinality=ONE),
        )
        db.transact(schema)

    def test_clean_dict_entity_with_real_entity(self, db):
        """Test clean_dict_entity with actual entity."""
        result = db.transact(['{:db/id #db/id[:db.part/user] :customer/name "Charlie" :customer/email "charlie@example.com" :customer/tier "gold"}'])
        eid = list(result.get(":tempids", {}).values())[0]

        factory = clean_dict_entity()
        entity = db.entity(eid, entity_factory=factory)

        assert isinstance(entity, dict)
        # Keys should have namespace stripped
        assert "name" in entity
        assert "email" in entity
        assert entity["name"] == "Charlie"

    def test_clean_dict_entity_with_namespace(self, db):
        """Test clean_dict_entity with namespace included."""
        result = db.transact(['{:db/id #db/id[:db.part/user] :customer/name "Diana" :customer/email "diana@example.com"}'])
        eid = list(result.get(":tempids", {}).values())[0]

        factory = clean_dict_entity(include_namespace=True)
        entity = db.entity(eid, entity_factory=factory)

        # Keys should include namespace
        assert "customer_name" in entity
        assert entity["customer_name"] == "Diana"

    def test_dataclass_entity_with_real_entity(self, db):
        """Test dataclass_entity with actual entity."""

        @dataclass
        class Customer:
            name: str
            email: str

        result = db.transact(['{:db/id #db/id[:db.part/user] :customer/name "Eve" :customer/email "eve@example.com"}'])
        eid = list(result.get(":tempids", {}).values())[0]

        factory = dataclass_entity(Customer, {"name": ":customer/name", "email": ":customer/email"})
        entity = db.entity(eid, entity_factory=factory)

        assert isinstance(entity, Customer)
        assert entity.name == "Eve"
        assert entity.email == "eve@example.com"


class TestDatomicModelIntegration:
    """Integration tests for DatomicModel with real database operations."""

    @pytest.fixture(autouse=True)
    def setup_schema(self, db):
        """Set up schema for model tests."""
        schema = Schema(
            Attribute(":project/name", STRING, cardinality=ONE),
            Attribute(":project/status", STRING, cardinality=ONE),
            Attribute(":project/tags", STRING, cardinality=MANY),
        )
        db.transact(schema)

    def test_model_from_entity(self, db):
        """Test creating model from entity."""

        class Project(DatomicModel):
            name: str = Field(":project/name")
            status: str = Field(":project/status")

        result = db.transact(['{:db/id #db/id[:db.part/user] :project/name "Website Redesign" :project/status "active"}'])
        eid = list(result.get(":tempids", {}).values())[0]

        entity = db.entity(eid)
        project = Project.from_entity(entity)

        assert project.name == "Website Redesign"
        assert project.status == "active"
        assert project.db_id == eid

    def test_model_from_row(self, db):
        """Test creating model from query row."""

        class Project(DatomicModel):
            name: str = Field(":project/name")
            status: str = Field(":project/status")

        db.transact(['{:db/id #db/id[:db.part/user] :project/name "Mobile App" :project/status "planning"}'])

        results = db.query(
            "[:find ?name ?status :where [?e :project/name ?name] [?e :project/status ?status]]"
        )

        for row in results:
            columns = (":project/name", ":project/status")
            project = Project.from_row(row, columns)
            if project.name == "Mobile App":
                assert project.status == "planning"
                break

    def test_model_with_cardinality_many(self, db):
        """Test model with cardinality many attribute."""

        class Project(DatomicModel):
            name: str = Field(":project/name")
            tags: list[str] = Field(":project/tags", cardinality=MANY_CARD)

        result = db.transact(['{:db/id #db/id[:db.part/user] :project/name "Data Pipeline" :project/tags ["python" "etl" "data"]}'])
        eid = list(result.get(":tempids", {}).values())[0]

        entity = db.entity(eid)
        project = Project.from_entity(entity)

        assert project.name == "Data Pipeline"
        assert isinstance(project.tags, list)
        assert len(project.tags) >= 1

    def test_model_to_dict_roundtrip(self, db):
        """Test model can be converted to dict and back."""

        class Project(DatomicModel):
            name: str = Field(":project/name")
            status: str = Field(":project/status")

        result = db.transact(['{:db/id #db/id[:db.part/user] :project/name "API Gateway" :project/status "completed"}'])
        eid = list(result.get(":tempids", {}).values())[0]

        # Load from database
        entity = db.entity(eid)
        project = Project.from_entity(entity)

        # Convert to dict
        project_dict = project.to_dict()

        assert project_dict[":project/name"] == "API Gateway"
        assert project_dict[":project/status"] == "completed"
        assert project_dict[":db/id"] == eid


class TestRegisteredModelIntegration:
    """Integration tests for registered models."""

    @pytest.fixture(autouse=True)
    def setup_schema_and_clear_registry(self, db):
        """Set up schema and clear model registry."""
        from datomic_py.serialization.registry import model_registry

        model_registry.clear()

        schema = Schema(
            Attribute(":task/title", STRING, cardinality=ONE),
            Attribute(":task/priority", STRING, cardinality=ONE),
        )
        db.transact(schema)

    def test_registered_model_lookup(self, db):
        """Test that registered models can be looked up."""
        from datomic_py.serialization.registry import model_registry

        @register_model
        class Task(DatomicModel):
            __namespace__ = "task"
            title: str = Field(":task/title")
            priority: str = Field(":task/priority")

        # Should be registered
        assert model_registry.get("Task") is Task
        assert model_registry.get_by_namespace("task") is Task

        # Use the model
        result = db.transact(['{:db/id #db/id[:db.part/user] :task/title "Fix bug" :task/priority "high"}'])
        eid = list(result.get(":tempids", {}).values())[0]

        entity = db.entity(eid)
        task = Task.from_entity(entity)

        assert task.title == "Fix bug"
        assert task.priority == "high"


class TestQueryWithMultipleFactories:
    """Test using different factories with the same query."""

    @pytest.fixture(autouse=True)
    def setup_schema(self, db):
        """Set up schema for multi-factory tests."""
        schema = Schema(
            Attribute(":book/title", STRING, cardinality=ONE),
            Attribute(":book/author", STRING, cardinality=ONE),
        )
        db.transact(schema)

        db.transact(['{:db/id #db/id[:db.part/user] :book/title "The Pragmatic Programmer" :book/author "Dave Thomas"}'])
        db.transact(['{:db/id #db/id[:db.part/user] :book/title "Clean Code" :book/author "Robert Martin"}'])

    def test_same_query_different_factories(self, db):
        """Test same query with different row factories."""
        query = "[:find ?title ?author :where [?e :book/title ?title] [?e :book/author ?author]]"

        # As tuples (default)
        tuple_results = db.query(query)
        assert all(isinstance(row, tuple) for row in tuple_results)

        # As dicts
        dict_results = db.query(query, row_factory=dict_row)
        assert all(isinstance(row, dict) for row in dict_results)

        # As namedtuples
        nt_results = db.query(query, row_factory=namedtuple_row("Book"))
        assert all(hasattr(row, "title") and hasattr(row, "author") for row in nt_results)

        # As dataclass
        @dataclass
        class Book:
            title: str
            author: str

        dc_results = db.query(query, row_factory=dataclass_row(Book))
        assert all(isinstance(row, Book) for row in dc_results)

        # All should have same data count
        assert len(tuple_results) == len(dict_results) == len(nt_results) == len(dc_results)
