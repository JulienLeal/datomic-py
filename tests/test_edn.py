"""Tests for the EDN parser."""

from datetime import datetime
from uuid import UUID

import pytest

from pydatomic import edn
from pydatomic.exceptions import EDNParseError


class TestEdnParse:
    """Tests for EDN parsing."""

    @pytest.mark.parametrize(
        "edn_str,expected",
        [
            ('"helloworld"', "helloworld"),
            ("23", 23),
            ("23.11", 23.11),
            ("true", True),
            ("false", False),
            ("nil", None),
            (":hello", ":hello"),
            (r'"string\"ing"', 'string"ing'),
            ('"string\\n"', "string\n"),
            ("[:hello]", (":hello",)),
            ("-10.4", -10.4),
            ('"你"', "你"),
            ("\\€", "€"),
            ("[1 2]", (1, 2)),
            ('#{true "hello" 12}', {True, "hello", 12}),
            (
                '#inst "2012-09-10T23:51:55.840-00:00"',
                datetime(2012, 9, 10, 23, 51, 55, 840000),
            ),
            ("(\\a \\b \\c \\d)", ("a", "b", "c", "d")),
            ("{:a 1 :b 2 :c 3 :d 4}", {":a": 1, ":b": 2, ":c": 3, ":d": 4}),
            ("[1     2 3,4]", (1, 2, 3, 4)),
            (
                "{:a [1 2 3] :b #{23.1 43.1 33.1}}",
                {":a": (1, 2, 3), ":b": frozenset([23.1, 43.1, 33.1])},
            ),
            ("{:a 1 :b [32 32 43] :c 4}", {":a": 1, ":b": (32, 32, 43), ":c": 4}),
            ("\\你", "你"),
            (
                '#db/fn{:lang "clojure" :code "(map l)"}',
                {":lang": "clojure", ":code": "(map l)"},
            ),
            ("#_ {[#{}] #{[]}} [23[34][32][4]]", (23, (34,), (32,), (4,))),
            (
                "(:graham/stratton true  \n , "
                '"A string with \\n \\"s" true #uuid "f81d4fae7dec11d0a76500a0c91e6bf6")',
                (
                    ":graham/stratton",
                    True,
                    'A string with \n "s',
                    True,
                    UUID("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"),
                ),
            ),
            (
                "[\\space \\€ [true []] ;true\n"
                '[true #inst "2012-09-10T23:39:43.309-00:00" true ""]]',
                (
                    " ",
                    "€",
                    (True, ()),
                    (True, datetime(2012, 9, 10, 23, 39, 43, 309000), True, ""),
                ),
            ),
            (
                " {true false nil    [true, ()] 6 {#{nil false} {nil \\newline} }}",
                {None: (True, ()), True: False, 6: {frozenset([False, None]): {None: "\n"}}},
            ),
            (
                '[#{6.22e-18, -3.1415, 1} true #graham #{"pie" "chips"} "work"]',
                (frozenset([6.22e-18, -3.1415, 1]), True, "work"),
            ),
            ("(\\a .5)", ("a", 0.5)),
            (
                "(List #{[123 456 {}] {a 1 b 2 c ({}, [])}})",
                ("List", ((123, 456, {}), {"a": 1, "c": ({}, ()), "b": 2})),
            ),
        ],
    )
    def test_parse_edn(self, edn_str: str, expected):
        """Test parsing various EDN data types."""
        result = edn.loads(edn_str)
        # For sets, compare as sets since order doesn't matter
        if isinstance(expected, set):
            assert isinstance(result, (set, frozenset))
            assert set(result) == expected
        elif isinstance(expected, tuple) and len(expected) > 0:
            # For tuples containing sets, need special comparison
            if isinstance(result, tuple) and len(result) == len(expected):
                for r, e in zip(result, expected):
                    if isinstance(e, set):
                        assert isinstance(r, (set, frozenset))
                        assert set(r) == e
                    elif isinstance(e, frozenset):
                        assert isinstance(r, frozenset)
                        assert r == e
                    elif isinstance(e, dict):
                        assert r == e
                    else:
                        assert r == e
            else:
                assert result == expected
        else:
            assert result == expected

    @pytest.mark.parametrize(
        "malformed",
        [
            "[1 2 3",
            "@EE",
            "[@nil tee]",
        ],
    )
    def test_malformed_data(self, malformed: str):
        """Verify exception is raised on malformed data."""
        with pytest.raises((ValueError, EDNParseError, StopIteration)):
            edn.loads(malformed)


class TestEdnBasic:
    """Basic EDN parsing tests."""

    def test_parse_integer(self):
        """Test parsing integers."""
        assert edn.loads("42") == 42
        assert edn.loads("-123") == -123
        assert edn.loads("+456") == 456

    def test_parse_float(self):
        """Test parsing floats."""
        assert edn.loads("3.14") == 3.14
        assert edn.loads("-2.5") == -2.5
        assert edn.loads("1e10") == 1e10
        assert edn.loads("1.5e-3") == 1.5e-3

    def test_parse_string(self):
        """Test parsing strings."""
        assert edn.loads('"hello"') == "hello"
        assert edn.loads('"hello world"') == "hello world"
        assert edn.loads('"line1\\nline2"') == "line1\nline2"

    def test_parse_keyword(self):
        """Test parsing keywords."""
        assert edn.loads(":keyword") == ":keyword"
        assert edn.loads(":namespaced/keyword") == ":namespaced/keyword"

    def test_parse_boolean(self):
        """Test parsing booleans."""
        assert edn.loads("true") is True
        assert edn.loads("false") is False

    def test_parse_nil(self):
        """Test parsing nil."""
        assert edn.loads("nil") is None

    def test_parse_vector(self):
        """Test parsing vectors."""
        assert edn.loads("[1 2 3]") == (1, 2, 3)
        assert edn.loads("[]") == ()

    def test_parse_list(self):
        """Test parsing lists."""
        assert edn.loads("(1 2 3)") == (1, 2, 3)
        assert edn.loads("()") == ()

    def test_parse_map(self):
        """Test parsing maps."""
        assert edn.loads("{:a 1}") == {":a": 1}
        assert edn.loads("{}") == {}

    def test_parse_set(self):
        """Test parsing sets."""
        result = edn.loads("#{1 2 3}")
        assert isinstance(result, (set, frozenset))
        assert set(result) == {1, 2, 3}

    def test_parse_nested(self):
        """Test parsing nested structures."""
        result = edn.loads("{:data [1 2 {:nested true}]}")
        assert result == {":data": (1, 2, {":nested": True})}

    def test_parse_datetime(self):
        """Test parsing datetime."""
        result = edn.loads('#inst "2023-01-15T10:30:00.000-00:00"')
        assert result == datetime(2023, 1, 15, 10, 30, 0, 0)

    def test_parse_uuid(self):
        """Test parsing UUID."""
        result = edn.loads('#uuid "550e8400-e29b-41d4-a716-446655440000"')
        assert result == UUID("550e8400-e29b-41d4-a716-446655440000")

    def test_parse_bytes(self):
        """Test parsing from bytes."""
        result = edn.loads(b'"hello"')
        assert result == "hello"


class TestEdnCharacters:
    """Tests for EDN character parsing."""

    def test_simple_char(self):
        """Test simple character parsing."""
        assert edn.loads("\\a") == "a"
        assert edn.loads("\\z") == "z"

    def test_special_chars(self):
        """Test special character parsing."""
        assert edn.loads("\\newline") == "\n"
        assert edn.loads("\\space") == " "
        assert edn.loads("\\tab") == "\t"

    def test_unicode_char(self):
        """Test unicode character parsing."""
        assert edn.loads("\\€") == "€"
        assert edn.loads("\\你") == "你"


class TestEdnComments:
    """Tests for EDN comment handling."""

    def test_comment_in_vector(self):
        """Test comments are ignored."""
        result = edn.loads("[1 ;comment\n2]")
        assert result == (1, 2)

    def test_discard(self):
        """Test discard reader macro."""
        result = edn.loads("#_ ignored [1 2]")
        assert result == (1, 2)


class TestEdnDepthLimit:
    """Tests for EDN recursion depth limit."""

    def test_deep_nesting_within_limit(self):
        """Test that nesting within the limit works."""
        # Create nested vectors 50 deep (within default limit of 100)
        nested = "[" * 50 + "1" + "]" * 50
        result = edn.loads(nested)
        # Unpack to get the value
        for _ in range(50):
            assert isinstance(result, tuple)
            assert len(result) == 1
            result = result[0]
        assert result == 1

    def test_deep_nesting_exceeds_limit(self):
        """Test that nesting exceeding the limit raises EDNParseError."""
        # Create nested vectors 150 deep (exceeds default limit of 100)
        nested = "[" * 150 + "1" + "]" * 150
        with pytest.raises(EDNParseError, match="Maximum nesting depth"):
            edn.loads(nested)

    def test_custom_depth_limit(self):
        """Test that custom depth limit is respected."""
        nested = "[" * 20 + "1" + "]" * 20
        # Should work with default limit
        edn.loads(nested)
        # Should fail with limit of 10
        with pytest.raises(EDNParseError, match="Maximum nesting depth"):
            edn.loads(nested, max_depth=10)

    def test_map_nesting_depth(self):
        """Test that map nesting also counts toward depth limit."""
        nested = "{:a " * 15 + "1" + "}" * 15
        with pytest.raises(EDNParseError, match="Maximum nesting depth"):
            edn.loads(nested, max_depth=10)


class TestEdnErrorHandling:
    """Tests for EDN error handling."""

    def test_invalid_utf8_bytes(self):
        """Test that invalid UTF-8 bytes raise EDNParseError."""
        invalid_bytes = b"\xff\xfe"
        with pytest.raises(EDNParseError, match="Invalid UTF-8 encoding"):
            edn.loads(invalid_bytes)

    def test_unterminated_map(self):
        """Test that unterminated map raises EDNParseError."""
        with pytest.raises(EDNParseError, match="Unterminated map"):
            edn.loads("{:a 1")

    def test_unterminated_string(self):
        """Test that unterminated string raises EDNParseError."""
        with pytest.raises(EDNParseError, match="Unterminated string"):
            edn.loads('"hello')

    def test_unterminated_collection(self):
        """Test that unterminated collection raises EDNParseError."""
        with pytest.raises(EDNParseError, match="Unterminated collection"):
            edn.loads("[1 2 3")

    def test_multiple_decimal_points(self):
        """Test that multiple decimal points raises EDNParseError."""
        with pytest.raises(EDNParseError, match="multiple decimal points"):
            edn.loads("1.2.3")


class TestEdnDatetimeFormats:
    """Tests for EDN datetime format parsing."""

    def test_datetime_with_minus_offset(self):
        """Test datetime with -00:00 offset."""
        result = edn.loads('#inst "2023-01-15T10:30:00.123-00:00"')
        assert result == datetime(2023, 1, 15, 10, 30, 0, 123000)

    def test_datetime_with_plus_offset(self):
        """Test datetime with +00:00 offset."""
        result = edn.loads('#inst "2023-01-15T10:30:00.123+00:00"')
        assert result == datetime(2023, 1, 15, 10, 30, 0, 123000)

    def test_datetime_with_z_suffix(self):
        """Test datetime with Z suffix."""
        result = edn.loads('#inst "2023-01-15T10:30:00.123Z"')
        assert result == datetime(2023, 1, 15, 10, 30, 0, 123000)

    def test_datetime_without_microseconds(self):
        """Test datetime without microseconds."""
        result = edn.loads('#inst "2023-01-15T10:30:00-00:00"')
        assert result == datetime(2023, 1, 15, 10, 30, 0)

    def test_datetime_without_microseconds_z(self):
        """Test datetime without microseconds with Z suffix."""
        result = edn.loads('#inst "2023-01-15T10:30:00Z"')
        assert result == datetime(2023, 1, 15, 10, 30, 0)

    def test_invalid_datetime_format(self):
        """Test that invalid datetime format raises EDNParseError."""
        with pytest.raises(EDNParseError, match="Invalid datetime format"):
            edn.loads('#inst "not-a-date"')
