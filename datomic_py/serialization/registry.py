"""Model registry for Datomic models."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datomic_py.serialization.models import DatomicModel


class ModelRegistry:
    """
    Registry for DatomicModel classes.

    Enables:
    - Forward reference resolution
    - Namespace-based model lookup
    - Schema verification against database

    Example:
        registry = ModelRegistry()
        registry.register(Person)
        model_cls = registry.get("Person")

    """

    __slots__ = ("_by_name", "_by_namespace")

    def __init__(self) -> None:
        self._by_name: dict[str, type[DatomicModel]] = {}
        self._by_namespace: dict[str, type[DatomicModel]] = {}

    def register(self, model: type[DatomicModel]) -> None:
        """
        Register a model class.

        Args:
            model: The DatomicModel subclass to register

        """
        self._by_name[model.__name__] = model
        if hasattr(model, "__namespace__") and model.__namespace__:
            self._by_namespace[model.__namespace__] = model

    def get(self, name: str) -> type[DatomicModel] | None:
        """
        Get a model by class name.

        Args:
            name: The class name (e.g., "Person")

        Returns:
            The model class or None

        """
        return self._by_name.get(name)

    def get_by_namespace(self, namespace: str) -> type[DatomicModel] | None:
        """
        Get a model by Datomic namespace.

        Args:
            namespace: The namespace (e.g., "person")

        Returns:
            The model class or None

        """
        return self._by_namespace.get(namespace)

    def all_models(self) -> list[type[DatomicModel]]:
        """Get all registered models."""
        return list(self._by_name.values())

    def clear(self) -> None:
        """Clear all registered models."""
        self._by_name.clear()
        self._by_namespace.clear()

    def verify_against_db(
        self,
        db: Any,  # Database
        model: type[DatomicModel],
    ) -> list[str]:
        """
        Verify model fields against database schema.

        Queries the database for schema attributes and checks that
        model fields match the expected types and cardinalities.

        Args:
            db: Database connection
            model: Model class to verify

        Returns:
            List of warning/error messages (empty if all OK)

        """
        warnings: list[str] = []

        # Query for schema attributes
        for field_name, descriptor in model.__datomic_fields__.items():
            attr = descriptor.attr

            # Query schema for this attribute
            result = db.query(
                """[:find ?type ?card
                    :in $ ?attr
                    :where
                    [?a :db/ident ?attr]
                    [?a :db/valueType ?t]
                    [?t :db/ident ?type]
                    [?a :db/cardinality ?c]
                    [?c :db/ident ?card]]""",
                extra_args=[attr],
            )

            if not result:
                warnings.append(f"Attribute {attr} not found in schema")
                continue

            db_type, db_card = result[0]

            # Check cardinality
            expected_card = descriptor.cardinality.value
            if db_card != expected_card:
                warnings.append(
                    f"Field {field_name}: cardinality mismatch "
                    f"(model: {expected_card}, db: {db_card})"
                )

            # Check if ref field matches ref type
            if descriptor.ref and db_type != ":db.type/ref":
                warnings.append(
                    f"Field {field_name}: marked as ref but db type is {db_type}"
                )
            elif not descriptor.ref and db_type == ":db.type/ref":
                warnings.append(
                    f"Field {field_name}: db type is ref but field not marked as ref"
                )

        return warnings


# Global model registry instance
model_registry = ModelRegistry()


def register_model(model: type[DatomicModel]) -> type[DatomicModel]:
    """
    Register a model with the global registry.

    Use as a decorator on DatomicModel subclasses.

    Usage:
        @register_model
        class Person(DatomicModel):
            ...

    Returns:
        The model class unchanged

    """
    model_registry.register(model)
    return model
