"""EDN tag handlers and registry."""

from typing import Any, Callable
from uuid import UUID

from pydatomic.edn.datetime_utils import parse_datetime
from pydatomic.edn.types import SKIP


# Type for tag handler functions
TagHandler = Callable[[Any, int | None], Any]


def _handle_inst(value: Any, pos: int | None = None) -> Any:
    """Handle #inst tag for datetime values."""
    if isinstance(value, str):
        return parse_datetime(value, pos)
    return value


def _handle_uuid(value: Any, pos: int | None = None) -> Any:
    """Handle #uuid tag for UUID values."""
    if isinstance(value, str):
        return UUID(value)
    return value


def _handle_db_fn(value: Any, pos: int | None = None) -> Any:
    """Handle #db/fn tag - returns value as-is."""
    return value


def _handle_discard(value: Any, pos: int | None = None) -> Any:
    """Handle #_ discard tag - skips the value."""
    return SKIP


class TagRegistry:
    """Registry for EDN tag handlers.

    This allows extensibility by registering custom tag handlers.
    """

    def __init__(self):
        self._handlers: dict[str, TagHandler] = {}
        self._register_defaults()

    def _register_defaults(self) -> None:
        """Register the default EDN tag handlers."""
        self.register("inst", _handle_inst)
        self.register("uuid", _handle_uuid)
        self.register("db/fn", _handle_db_fn)
        self.register("_", _handle_discard)

    def register(self, tag: str, handler: TagHandler) -> None:
        """Register a tag handler.

        Args:
            tag: The tag name (without #).
            handler: A callable that takes (value, position) and returns the processed value.
        """
        self._handlers[tag] = handler

    def unregister(self, tag: str) -> None:
        """Unregister a tag handler.

        Args:
            tag: The tag name to unregister.
        """
        self._handlers.pop(tag, None)

    def get_handler(self, tag: str) -> TagHandler | None:
        """Get the handler for a tag.

        Args:
            tag: The tag name.

        Returns:
            The handler function or None if not registered.
        """
        return self._handlers.get(tag)

    def is_known(self, tag: str) -> bool:
        """Check if a tag is registered.

        Args:
            tag: The tag name.

        Returns:
            True if the tag has a registered handler.
        """
        return tag in self._handlers

    @property
    def known_tags(self) -> set[str]:
        """Get the set of all registered tag names."""
        return set(self._handlers.keys())


# Default tag registry instance
default_registry = TagRegistry()
