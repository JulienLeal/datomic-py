"""Tests for the EDN parser."""

from datetime import datetime, timezone
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
                datetime(2012, 9, 10, 23, 51, 55, 840000, tzinfo=timezone.utc),
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
                    (True, datetime(2012, 9, 10, 23, 39, 43, 309000, tzinfo=timezone.utc), True, ""),
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
        assert result == datetime(2023, 1, 15, 10, 30, 0, 0, tzinfo=timezone.utc)

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
        assert result == datetime(2023, 1, 15, 10, 30, 0, 123000, tzinfo=timezone.utc)

    def test_datetime_with_plus_offset(self):
        """Test datetime with +00:00 offset."""
        result = edn.loads('#inst "2023-01-15T10:30:00.123+00:00"')
        assert result == datetime(2023, 1, 15, 10, 30, 0, 123000, tzinfo=timezone.utc)

    def test_datetime_with_z_suffix(self):
        """Test datetime with Z suffix."""
        result = edn.loads('#inst "2023-01-15T10:30:00.123Z"')
        assert result == datetime(2023, 1, 15, 10, 30, 0, 123000, tzinfo=timezone.utc)

    def test_datetime_without_microseconds(self):
        """Test datetime without microseconds."""
        result = edn.loads('#inst "2023-01-15T10:30:00-00:00"')
        assert result == datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

    def test_datetime_without_microseconds_z(self):
        """Test datetime without microseconds with Z suffix."""
        result = edn.loads('#inst "2023-01-15T10:30:00Z"')
        assert result == datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

    def test_invalid_datetime_format(self):
        """Test that invalid datetime format raises EDNParseError."""
        with pytest.raises(EDNParseError, match="Invalid datetime format"):
            edn.loads('#inst "not-a-date"')


class TestEdnDumps:
    """Tests for EDN serialization (dumps function)."""

    def test_dumps_nil(self):
        """Test serializing None to nil."""
        assert edn.dumps(None) == "nil"

    def test_dumps_bool(self):
        """Test serializing booleans."""
        assert edn.dumps(True) == "true"
        assert edn.dumps(False) == "false"

    def test_dumps_int(self):
        """Test serializing integers."""
        assert edn.dumps(42) == "42"
        assert edn.dumps(-123) == "-123"
        assert edn.dumps(0) == "0"

    def test_dumps_float(self):
        """Test serializing floats."""
        assert edn.dumps(3.14) == "3.14"
        assert edn.dumps(-2.5) == "-2.5"

    def test_dumps_string(self):
        """Test serializing strings."""
        assert edn.dumps("hello") == '"hello"'
        assert edn.dumps("hello world") == '"hello world"'
        assert edn.dumps('with "quotes"') == '"with \\"quotes\\""'
        assert edn.dumps("with\nnewline") == '"with\\nnewline"'

    def test_dumps_keyword(self):
        """Test serializing keywords (strings starting with :)."""
        assert edn.dumps(":keyword") == ":keyword"
        assert edn.dumps(":namespaced/key") == ":namespaced/key"

    def test_dumps_list(self):
        """Test serializing lists/tuples as vectors."""
        assert edn.dumps([1, 2, 3]) == "[1 2 3]"
        assert edn.dumps((1, 2, 3)) == "[1 2 3]"
        assert edn.dumps([]) == "[]"

    def test_dumps_set(self):
        """Test serializing sets."""
        result = edn.dumps({1})
        assert result == "#{1}"

    def test_dumps_map(self):
        """Test serializing dicts as maps."""
        result = edn.dumps({":a": 1})
        assert result == "{:a 1}"
        assert edn.dumps({}) == "{}"

    def test_dumps_uuid(self):
        """Test serializing UUIDs."""
        uuid = UUID("550e8400-e29b-41d4-a716-446655440000")
        assert edn.dumps(uuid) == '#uuid "550e8400-e29b-41d4-a716-446655440000"'

    def test_dumps_datetime(self):
        """Test serializing datetimes."""
        dt = datetime(2023, 1, 15, 10, 30, 0)
        result = edn.dumps(dt)
        assert result.startswith('#inst "2023-01-15T10:30:00')

    def test_dumps_nested(self):
        """Test serializing nested structures."""
        data = {":data": [1, 2, {":nested": True}]}
        result = edn.dumps(data)
        assert ":data" in result
        assert ":nested" in result
        assert "true" in result

    def test_dumps_roundtrip(self):
        """Test that dumps -> loads gives back equivalent data."""
        original = {":a": 1, ":b": [1, 2, 3], ":c": True}
        serialized = edn.dumps(original)
        parsed = edn.loads(serialized)
        assert parsed == {":a": 1, ":b": (1, 2, 3), ":c": True}

    def test_dumps_unsupported_type(self):
        """Test that unsupported types raise EDNParseError."""
        with pytest.raises(EDNParseError, match="Cannot serialize"):
            edn.dumps(object())


class TestEdnTagRegistry:
    """Tests for the EDN tag handler registry."""

    def test_default_registry_has_known_tags(self):
        """Test that default registry has expected tags."""
        from pydatomic.edn import default_registry

        assert default_registry.is_known("inst")
        assert default_registry.is_known("uuid")
        assert default_registry.is_known("db/fn")
        assert default_registry.is_known("_")

    def test_register_custom_tag(self):
        """Test registering a custom tag handler."""
        from pydatomic.edn import TagRegistry

        registry = TagRegistry()

        def custom_handler(value, pos=None):
            return f"custom:{value}"

        registry.register("custom", custom_handler)
        assert registry.is_known("custom")
        assert registry.get_handler("custom") is not None

    def test_unregister_tag(self):
        """Test unregistering a tag handler."""
        from pydatomic.edn import TagRegistry

        registry = TagRegistry()
        registry.register("temp", lambda v, p: v)
        assert registry.is_known("temp")
        registry.unregister("temp")
        assert not registry.is_known("temp")

    def test_custom_registry_with_loads(self):
        """Test using a custom registry with loads."""
        from pydatomic.edn import TagRegistry, loads

        registry = TagRegistry()

        def double_handler(value, pos=None):
            return value * 2 if isinstance(value, int) else value

        registry.register("double", double_handler)

        result = loads('#double 21', tag_registry=registry)
        assert result == 42

    def test_unknown_tag_skipped(self):
        """Test that unknown tags are skipped."""
        result = edn.loads('[1 #unknown "skipped" 2]')
        assert result == (1, 2)


class TestEdnErrorPositions:
    """Tests for error position information in messages."""

    def test_unterminated_string_has_position(self):
        """Test that unterminated string error includes position."""
        with pytest.raises(EDNParseError) as exc_info:
            edn.loads('  "hello')
        assert "position" in str(exc_info.value)

    def test_unterminated_map_has_position(self):
        """Test that unterminated map error includes position."""
        with pytest.raises(EDNParseError) as exc_info:
            edn.loads("  {:a 1")
        assert "position" in str(exc_info.value)

    def test_unterminated_collection_has_position(self):
        """Test that unterminated collection error includes position."""
        with pytest.raises(EDNParseError) as exc_info:
            edn.loads("  [1 2 3")
        assert "position" in str(exc_info.value)

    def test_multiple_decimal_points_has_position(self):
        """Test that multiple decimal points error includes position."""
        with pytest.raises(EDNParseError) as exc_info:
            edn.loads("  1.2.3")
        assert "position" in str(exc_info.value)

    def test_unexpected_character_has_position(self):
        """Test that unexpected character error includes position."""
        with pytest.raises(EDNParseError) as exc_info:
            edn.loads("  @invalid")
        assert "position" in str(exc_info.value)


class TestEdnNamedChars:
    """Tests for named character handling via NAMED_CHARS dict."""

    def test_return_char(self):
        """Test parsing return character."""
        assert edn.loads("\\return") == "\r"

    def test_named_chars_not_prefix_match(self):
        """Test that named chars don't match as prefixes of longer symbols."""
        # \spaceship should be 's' not ' ' followed by 'paceship'
        result = edn.loads("\\spaceship")
        # Since 'spaceship' doesn't match any delimiter, it reads 'spaceship' as char
        # Actually the character reader reads one char at a time for non-named chars
        # But named char check happens first - 'space' is a prefix of 'spaceship'
        # Let me verify the actual behavior
        assert result == "s"

    def test_all_named_chars(self):
        """Test all named characters from NAMED_CHARS dict."""
        assert edn.loads("\\newline") == "\n"
        assert edn.loads("\\space") == " "
        assert edn.loads("\\tab") == "\t"
        assert edn.loads("\\return") == "\r"


class TestEdnModuleExports:
    """Tests for module exports."""

    def test_edn_module_exports(self):
        """Test that edn module exports expected names."""
        assert hasattr(edn, "loads")
        assert hasattr(edn, "dumps")
        assert hasattr(edn, "EdnReader")
        assert hasattr(edn, "EDNParseError")
        assert hasattr(edn, "TagRegistry")
        assert hasattr(edn, "parse_datetime")

    def test_edn_subpackage_import(self):
        """Test importing from edn subpackage directly."""
        from pydatomic.edn import loads, dumps, EdnReader, TagRegistry

        assert callable(loads)
        assert callable(dumps)
        assert EdnReader is not None
        assert TagRegistry is not None


class TestEdnDatetimeExtended:
    """Extended tests for datetime parsing improvements."""

    def test_datetime_with_positive_offset(self):
        """Test datetime with positive timezone offset."""
        from datetime import timedelta
        result = edn.loads('#inst "2023-06-15T14:30:00+05:30"')
        expected_tz = timezone(timedelta(hours=5, minutes=30))
        assert result == datetime(2023, 6, 15, 14, 30, 0, tzinfo=expected_tz)

    def test_datetime_with_negative_offset(self):
        """Test datetime with negative timezone offset."""
        from datetime import timedelta
        result = edn.loads('#inst "2023-06-15T14:30:00-05:00"')
        expected_tz = timezone(timedelta(hours=-5))
        assert result == datetime(2023, 6, 15, 14, 30, 0, tzinfo=expected_tz)

    def test_datetime_milliseconds_precision(self):
        """Test datetime with milliseconds precision."""
        result = edn.loads('#inst "2023-01-15T10:30:00.123Z"')
        assert result.microsecond == 123000

    def test_datetime_microseconds_precision(self):
        """Test datetime with microseconds precision."""
        result = edn.loads('#inst "2023-01-15T10:30:00.123456Z"')
        assert result.microsecond == 123456
