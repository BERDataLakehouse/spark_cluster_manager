"""Tests for the argument checker module."""

import pytest

from src.service.arg_checkers import contains_control_characters, not_falsy


class TestNotFalsy:
    """Tests for the not_falsy function."""

    def test_truthy_string(self):
        assert not_falsy("hello", "name") == "hello"

    def test_truthy_number(self):
        assert not_falsy(42, "count") == 42

    def test_truthy_list(self):
        assert not_falsy([1, 2], "items") == [1, 2]

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="name is required"):
            not_falsy("", "name")

    def test_none_raises(self):
        with pytest.raises(ValueError, match="value is required"):
            not_falsy(None, "value")

    def test_zero_raises(self):
        with pytest.raises(ValueError, match="count is required"):
            not_falsy(0, "count")

    def test_empty_list_raises(self):
        with pytest.raises(ValueError, match="items is required"):
            not_falsy([], "items")


class TestContainsControlCharacters:
    """Tests for the contains_control_characters function."""

    def test_no_control_chars(self):
        assert contains_control_characters("hello world") == -1

    def test_null_byte(self):
        assert contains_control_characters("hello\x00world") == 5

    def test_tab_character(self):
        assert contains_control_characters("hello\tworld") == 5

    def test_newline(self):
        assert contains_control_characters("hello\nworld") == 5

    def test_allowed_chars_tab(self):
        assert contains_control_characters("hello\tworld", allowed_chars=["\t"]) == -1

    def test_allowed_chars_newline(self):
        assert contains_control_characters("line1\nline2", allowed_chars=["\n"]) == -1

    def test_control_char_at_start(self):
        assert contains_control_characters("\x01abc") == 0

    def test_empty_string(self):
        assert contains_control_characters("") == -1

    def test_multiple_control_chars_returns_first(self):
        assert contains_control_characters("\x01\x02\x03") == 0
