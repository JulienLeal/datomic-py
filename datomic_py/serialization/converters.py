"""Type conversion pipeline for Datomic values."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

# Type alias for converter functions
Converter = Callable[[Any], Any]


# Helper converter functions - defined first so they can be used in TypeConverter
def _identity(value: Any) -> Any:
    """Identity converter - returns value unchanged."""
    return value


def _to_bool(value: Any) -> bool:
    """Convert to boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value)


def _to_int(value: Any) -> int:
    """Convert to integer."""
    if isinstance(value, int):
        return value
    return int(value)


def _to_float(value: Any) -> float:
    """Convert to float."""
    if isinstance(value, float):
        return value
    return float(value)


def _to_decimal(value: Any) -> Decimal:
    """Convert to Decimal."""
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _to_datetime(value: Any) -> datetime:
    """Convert to datetime - already handled by EDN parser, passthrough."""
    if isinstance(value, datetime):
        return value
    # Fallback if raw string comes through
    s = str(value)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _to_uuid(value: Any) -> UUID:
    """Convert to UUID - already handled by EDN parser, passthrough."""
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


class TypeConverter:
    """
    Registry for type converters mapping Datomic types to Python types.

    Follows psycopg3 pattern of registering converters by type name.

    Example:
        converter = TypeConverter()
        converter.register(":db.type/instant", lambda v: datetime.fromisoformat(v))
        result = converter.convert(value, ":db.type/instant")

    """

    __slots__ = ("_converters", "_python_type_map")

    def __init__(self) -> None:
        self._converters: dict[str, Converter] = {}
        self._python_type_map: dict[type, str] = {}
        self._register_defaults()

    def _register_defaults(self) -> None:
        """Register default Datomic type converters."""
        # These are mostly no-ops since EDN parser handles them,
        # but allow customization and ensure type consistency
        self.register(":db.type/string", _identity, python_type=str)
        self.register(":db.type/boolean", _to_bool, python_type=bool)
        self.register(":db.type/long", _to_int, python_type=int)
        self.register(":db.type/bigint", _to_int, python_type=int)
        self.register(":db.type/float", _to_float, python_type=float)
        self.register(":db.type/double", _to_float, python_type=float)
        self.register(":db.type/bigdec", _to_decimal, python_type=Decimal)
        self.register(":db.type/instant", _to_datetime, python_type=datetime)
        self.register(":db.type/uuid", _to_uuid, python_type=UUID)
        self.register(":db.type/uri", str, python_type=str)
        self.register(":db.type/keyword", _identity, python_type=str)
        self.register(":db.type/ref", _to_int, python_type=int)  # Default: return entity ID
        self.register(":db.type/bytes", _identity, python_type=bytes)

    def register(
        self,
        datomic_type: str,
        converter: Converter | type,
        *,
        python_type: type | None = None,
    ) -> None:
        """
        Register a converter for a Datomic type.

        Args:
            datomic_type: The Datomic type string (e.g., ":db.type/string")
            converter: A callable that converts the value, or a type for passthrough
            python_type: Optional Python type for reverse mapping

        """
        if isinstance(converter, type):
            self._converters[datomic_type] = converter
            if python_type is None:
                python_type = converter
        else:
            self._converters[datomic_type] = converter

        if python_type is not None:
            self._python_type_map[python_type] = datomic_type

    def convert(self, value: Any, datomic_type: str) -> Any:
        """Convert a value according to its Datomic type."""
        if value is None:
            return None
        converter = self._converters.get(datomic_type)
        if converter is None:
            return value
        return converter(value)

    def get_converter(self, datomic_type: str) -> Converter | None:
        """Get the converter for a Datomic type."""
        return self._converters.get(datomic_type)

    def get_datomic_type(self, python_type: type) -> str | None:
        """Get the Datomic type for a Python type."""
        return self._python_type_map.get(python_type)


# Default converter instance
default_converter = TypeConverter()


class CompiledConverter:
    """
    Pre-compiled converter for a specific schema/model.

    Optimizes repeated conversions by pre-resolving converter functions.

    Example:
        compiled = CompiledConverter(
            {"name": ":db.type/string", "age": ":db.type/long"}
        )
        row = compiled.convert_row({"name": "Alice", "age": "30"})

    """

    __slots__ = ("_field_converters",)

    def __init__(
        self,
        field_types: dict[str, str],  # attr_name -> datomic_type
        converter: TypeConverter | None = None,
    ) -> None:
        converter = converter or default_converter
        self._field_converters: dict[str, Converter] = {}

        for attr_name, datomic_type in field_types.items():
            conv = converter.get_converter(datomic_type)
            if conv is not None:
                self._field_converters[attr_name] = conv

    def convert_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """Convert all values in a row dict."""
        result = dict(row)
        for attr_name, conv in self._field_converters.items():
            if attr_name in result and result[attr_name] is not None:
                result[attr_name] = conv(result[attr_name])
        return result

    def convert_value(self, attr_name: str, value: Any) -> Any:
        """Convert a single value by attribute name."""
        if value is None:
            return None
        conv = self._field_converters.get(attr_name)
        if conv is not None:
            return conv(value)
        return value
