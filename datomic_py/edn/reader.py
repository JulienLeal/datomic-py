"""EDN reader/parser implementation."""

from typing import Callable

from datomic_py.exceptions import EDNParseError
from datomic_py.edn.types import NAMED_CHARS, SKIP, EDNValue
from datomic_py.edn.tags import TagRegistry, default_registry


class EdnReader:
    """EDN reader/parser class.

    Parses EDN (Extensible Data Notation) strings into Python objects.

    Attributes:
        s: The EDN string being parsed.
        pos: Current position in the string.
        length: Length of the input string.
        max_depth: Maximum nesting depth allowed.
    """

    def __init__(
        self,
        s: str,
        max_depth: int = 100,
        tag_registry: TagRegistry | None = None,
    ):
        """Initialize the EDN reader.

        Args:
            s: The EDN string to parse.
            max_depth: Maximum nesting depth allowed (default 100).
            tag_registry: Optional custom tag registry. Uses default if not provided.
        """
        self.s = s
        self.pos = 0
        self.length = len(s)
        self.max_depth = max_depth
        self._current_depth = 0
        self._tag_registry = tag_registry or default_registry

        # Dispatch table for character-based readers
        self._readers: dict[str, Callable[[], EDNValue]] = {
            '"': self._read_string,
            "[": self._read_vector,
            "(": self._read_list,
            "{": self._read_map,
            "#": self._read_dispatch,
            "\\": self._read_char,
            ":": self._read_keyword,
        }

    def peek(self) -> str | None:
        """Look at the current character without consuming it."""
        if self.pos >= self.length:
            return None
        return self.s[self.pos]

    def read(self) -> str | None:
        """Read and consume the current character."""
        if self.pos >= self.length:
            return None
        c = self.s[self.pos]
        self.pos += 1
        return c

    def skip_whitespace_and_comments(self) -> None:
        """Skip whitespace, commas, and comments."""
        while self.pos < self.length:
            c = self.s[self.pos]
            if c in " \t\n\r,":
                self.pos += 1
            elif c == ";":
                # Comment - skip to end of line
                while self.pos < self.length and self.s[self.pos] != "\n":
                    self.pos += 1
                if self.pos < self.length:
                    self.pos += 1  # Skip newline
            else:
                break

    def _read_string(self) -> str:
        """Read a string literal."""
        start_pos = self.pos - 1  # Position of opening quote
        chars = []
        while True:
            c = self.read()
            if c is None:
                raise EDNParseError(f"Unterminated string at position {start_pos}")
            if c == '"':
                break
            if c == "\\":
                escape = self.read()
                if escape == "n":
                    chars.append("\n")
                elif escape == "t":
                    chars.append("\t")
                elif escape == "r":
                    chars.append("\r")
                elif escape == '"':
                    chars.append('"')
                elif escape == "\\":
                    chars.append("\\")
                else:
                    chars.append(escape)
            else:
                chars.append(c)
        return "".join(chars)

    def read_symbol_or_keyword(self, first_char: str) -> str:
        """Read a symbol or keyword."""
        chars = [first_char]
        while self.pos < self.length:
            c = self.s[self.pos]
            if c in " \t\n\r,()[]{}\"\\;":
                break
            chars.append(c)
            self.pos += 1
        return "".join(chars)

    def read_number(self, first_char: str) -> int | float:
        """Read a number (integer or float)."""
        start_pos = self.pos - 1
        chars = [first_char]
        has_decimal = first_char == "."
        while self.pos < self.length:
            c = self.s[self.pos]
            if c == ".":
                if has_decimal:
                    raise EDNParseError(
                        f"Invalid number: multiple decimal points at position {start_pos}"
                    )
                has_decimal = True
                chars.append(c)
                self.pos += 1
            elif c.isdigit() or c in "-+eE":
                chars.append(c)
                self.pos += 1
            else:
                break
        num_str = "".join(chars)
        if "." in num_str or "e" in num_str.lower():
            return float(num_str)
        return int(num_str)

    def _read_char(self) -> str:
        """Read a character literal."""
        start_pos = self.pos - 1
        if self.pos >= self.length:
            raise EDNParseError(
                f"Unexpected end of input reading character at position {start_pos}"
            )

        # Check for named characters using the dictionary
        remaining = self.s[self.pos:]
        for name, char_value in NAMED_CHARS.items():
            if remaining.startswith(name):
                # Ensure it's not a prefix of a longer symbol
                end_pos = self.pos + len(name)
                if end_pos >= self.length or self.s[end_pos] in " \t\n\r,()[]{}\"\\;":
                    self.pos += len(name)
                    return char_value

        # Single character
        c = self.read()
        return c

    def _read_collection(self, end_char: str) -> list:
        """Read a collection until end_char."""
        start_pos = self.pos - 1
        self._current_depth += 1
        if self._current_depth > self.max_depth:
            raise EDNParseError(
                f"Maximum nesting depth ({self.max_depth}) exceeded at position {start_pos}"
            )
        try:
            items = []
            while True:
                self.skip_whitespace_and_comments()
                c = self.peek()
                if c is None:
                    raise EDNParseError(
                        f"Unterminated collection, expected {end_char} at position {start_pos}"
                    )
                if c == end_char:
                    self.read()  # consume end char
                    break
                value = self.read_value()
                # Skip values marked with unknown tags
                if value is not SKIP:
                    items.append(value)
            return items
        finally:
            self._current_depth -= 1

    def _read_vector(self) -> tuple:
        """Read a vector."""
        return tuple(self._read_collection("]"))

    def _read_list(self) -> tuple:
        """Read a list."""
        return tuple(self._read_collection(")"))

    def _read_map(self) -> dict:
        """Read a map."""
        start_pos = self.pos - 1
        self._current_depth += 1
        if self._current_depth > self.max_depth:
            raise EDNParseError(
                f"Maximum nesting depth ({self.max_depth}) exceeded at position {start_pos}"
            )
        try:
            result = {}
            while True:
                self.skip_whitespace_and_comments()
                c = self.peek()
                if c is None:
                    raise EDNParseError(f"Unterminated map at position {start_pos}")
                if c == "}":
                    self.read()
                    break
                key = self.read_value()
                self.skip_whitespace_and_comments()
                value = self.read_value()
                # Skip entries with unknown tags
                if key is not SKIP and value is not SKIP:
                    result[key] = value
            return result
        finally:
            self._current_depth -= 1

    def _read_dispatch(self) -> EDNValue:
        """Read a dispatch form (#-prefixed)."""
        dispatch_pos = self.pos - 1
        next_c = self.peek()
        if next_c == "{":
            # Set
            self.read()
            items = self._read_collection("}")
            try:
                return frozenset(items)
            except TypeError:
                # If items are unhashable, return as tuple instead
                return tuple(items)
        elif next_c == "_":
            # Discard
            self.read()
            self.skip_whitespace_and_comments()
            self.read_value()  # Read and discard
            return self.read_value()  # Return next value
        else:
            # Tag
            tag = self.read_symbol_or_keyword("")
            return self._read_tagged(tag, dispatch_pos)

    def _read_tagged(self, tag: str, tag_pos: int) -> EDNValue:
        """Read a tagged value."""
        self.skip_whitespace_and_comments()
        value = self.read_value()

        handler = self._tag_registry.get_handler(tag)
        if handler is not None:
            return handler(value, tag_pos)

        # Unknown tag - skip this value entirely
        return SKIP

    def _read_keyword(self) -> str:
        """Read a keyword."""
        return self.read_symbol_or_keyword(":")

    def read_value(self) -> EDNValue | None:
        """Read a single EDN value.

        Returns:
            The parsed EDN value, or None for empty input.
            Note: EDN nil also returns None. Use loads() which distinguishes
            between empty input and nil.
        """
        self.skip_whitespace_and_comments()

        c = self.peek()
        if c is None:
            return None

        # Check dispatch table first
        if c in self._readers:
            self.read()  # consume the character
            return self._readers[c]()

        # Number
        if c.isdigit() or (
            c in "-+"
            and self.pos + 1 < self.length
            and (self.s[self.pos + 1].isdigit() or self.s[self.pos + 1] == ".")
        ):
            return self.read_number(self.read())

        # Decimal starting with .
        if c == ".":
            return self.read_number(self.read())

        # Keywords: true, false, nil
        if c in "tfn":
            word = self.read_symbol_or_keyword(self.read())
            if word == "true":
                return True
            if word == "false":
                return False
            if word == "nil":
                return None
            return word

        # Symbol - but @ is not a valid symbol start in EDN
        if c not in "()[]{}\"\\;,@":
            return self.read_symbol_or_keyword(self.read())

        # Unknown character
        raise EDNParseError(f"Unexpected character: {c} at position {self.pos}")
