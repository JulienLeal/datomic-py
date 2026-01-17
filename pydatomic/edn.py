"""EDN (Extensible Data Notation) parser for Python."""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydatomic.exceptions import EDNParseError


# Sentinel for values that should be skipped (unknown tags)
class _Skip:
    pass


SKIP = _Skip()

# Known tag handlers
KNOWN_TAGS = {"inst", "uuid", "db/fn", "_"}


def parse_datetime(value: str) -> datetime:
    """Parse an ISO 8601 datetime string."""
    formats = [
        "%Y-%m-%dT%H:%M:%S.%f-00:00",
        "%Y-%m-%dT%H:%M:%S.%f+00:00",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S-00:00",
        "%Y-%m-%dT%H:%M:%S+00:00",
        "%Y-%m-%dT%H:%M:%SZ",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise EDNParseError(f"Invalid datetime format: {value}")


class EdnReader:
    """EDN reader/parser class."""

    def __init__(self, s: str, max_depth: int = 100):
        self.s = s
        self.pos = 0
        self.length = len(s)
        self.max_depth = max_depth
        self._current_depth = 0

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

    def read_string(self) -> str:
        """Read a string literal (opening quote already consumed)."""
        chars = []
        while True:
            c = self.read()
            if c is None:
                raise EDNParseError("Unterminated string")
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
        chars = [first_char]
        has_decimal = first_char == "."
        while self.pos < self.length:
            c = self.s[self.pos]
            if c == ".":
                if has_decimal:
                    raise EDNParseError("Invalid number: multiple decimal points")
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

    def read_character(self) -> str:
        """Read a character literal (backslash already consumed)."""
        if self.pos >= self.length:
            raise EDNParseError("Unexpected end of input reading character")

        # Check for named characters
        remaining = self.s[self.pos:]
        if remaining.startswith("newline"):
            self.pos += 7
            return "\n"
        if remaining.startswith("space"):
            self.pos += 5
            return " "
        if remaining.startswith("tab"):
            self.pos += 3
            return "\t"
        if remaining.startswith("return"):
            self.pos += 6
            return "\r"

        # Single character
        c = self.read()
        return c

    def read_collection(self, end_char: str) -> list:
        """Read a collection until end_char."""
        self._current_depth += 1
        if self._current_depth > self.max_depth:
            raise EDNParseError(f"Maximum nesting depth ({self.max_depth}) exceeded")
        try:
            items = []
            while True:
                self.skip_whitespace_and_comments()
                c = self.peek()
                if c is None:
                    raise EDNParseError(f"Unterminated collection, expected {end_char}")
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

    def read_map(self) -> dict:
        """Read a map."""
        self._current_depth += 1
        if self._current_depth > self.max_depth:
            raise EDNParseError(f"Maximum nesting depth ({self.max_depth}) exceeded")
        try:
            result = {}
            while True:
                self.skip_whitespace_and_comments()
                c = self.peek()
                if c is None:
                    raise EDNParseError("Unterminated map")
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

    def read_tagged(self, tag: str) -> Any:
        """Read a tagged value."""
        self.skip_whitespace_and_comments()
        value = self.read_value()

        if tag == "inst":
            # Parse datetime
            if isinstance(value, str):
                return parse_datetime(value)
        elif tag == "uuid":
            if isinstance(value, str):
                return UUID(value)
        elif tag == "db/fn":
            return value

        # Unknown tag - skip this value entirely
        if tag not in KNOWN_TAGS:
            return SKIP

        # Return value for any remaining known tags
        return value

    def read_value(self) -> Any:
        """Read a single EDN value."""
        self.skip_whitespace_and_comments()

        c = self.peek()
        if c is None:
            return None

        # String
        if c == '"':
            self.read()
            return self.read_string()

        # Vector
        if c == "[":
            self.read()
            return tuple(self.read_collection("]"))

        # List
        if c == "(":
            self.read()
            return tuple(self.read_collection(")"))

        # Map
        if c == "{":
            self.read()
            return self.read_map()

        # Tagged value or set
        if c == "#":
            self.read()
            next_c = self.peek()
            if next_c == "{":
                # Set
                self.read()
                items = self.read_collection("}")
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
                return self.read_tagged(tag)

        # Character
        if c == "\\":
            self.read()
            return self.read_character()

        # Keyword
        if c == ":":
            return self.read_symbol_or_keyword(self.read())

        # Number
        if c.isdigit() or (c in "-+" and self.pos + 1 < self.length and
                          (self.s[self.pos + 1].isdigit() or self.s[self.pos + 1] == ".")):
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
        raise EDNParseError(f"Unexpected character: {c}")


def loads(s: str | bytes, max_depth: int = 100) -> Any:
    """Load an EDN string and return the parsed Python object."""
    if isinstance(s, bytes):
        try:
            s = s.decode("utf-8")
        except UnicodeDecodeError as e:
            raise EDNParseError(f"Invalid UTF-8 encoding: {e}") from e

    reader = EdnReader(s, max_depth=max_depth)
    return reader.read_value()


if __name__ == "__main__":
    tests = [
        '"helloworld"',
        "23",
        "23.11",
        "true",
        "false",
        "nil",
        ":hello",
        "[:hello]",
        "[1 2]",
        '#{true "hello" 12}',
        '#inst "2012-09-10T23:51:55.840-00:00"',
        "(\\a \\b \\c \\d)",
        "{:a 1 :b 2 :c 3 :d 4}",
    ]
    for test in tests:
        print(f"{test} -> {loads(test)}")
