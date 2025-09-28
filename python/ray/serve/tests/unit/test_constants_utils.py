import os
from unittest.mock import patch

import pytest

from ray.serve._private.constants_utils import (
    _validate_name,
    get_env_bool,
    get_env_float,
    get_env_float_non_negative,
    get_env_float_positive,
    get_env_int,
    get_env_int_non_negative,
    get_env_int_positive,
    get_env_str,
    parse_latency_buckets,
    str_to_list,
)


class TestStrToList:
    def test_str_to_list_basic(self):
        assert str_to_list("a,b,c") == ["a", "b", "c"]

    def test_str_to_list_with_whitespace(self):
        assert str_to_list(" a , b , c ") == ["a", "b", "c"]

    def test_str_to_list_empty_string(self):
        assert str_to_list("") == []

    def test_str_to_list_with_empty_entries(self):
        assert str_to_list("a,,b,c,") == ["a", "b", "c"]

    def test_str_to_list_only_whitespace(self):
        assert str_to_list("   ") == []

    def test_str_to_list_single_entry(self):
        assert str_to_list("single") == ["single"]

    def test_str_to_list_only_commas(self):
        assert str_to_list(",,,,") == []

    def test_str_to_list_whitespace_entries(self):
        assert str_to_list("a, ,b") == ["a", "b"]


class TestParseLatencyBuckets:
    def test_parse_latency_buckets(self):
        # Test valid inputs with different formats
        assert parse_latency_buckets("1,2,3", []) == [1.0, 2.0, 3.0]
        assert parse_latency_buckets("1,2,3,4 ", []) == [1.0, 2.0, 3.0, 4.0]
        assert parse_latency_buckets("  1,2,3,4,5", []) == [1.0, 2.0, 3.0, 4.0, 5.0]
        assert parse_latency_buckets(" 1, 2,3  ,4,5 ,6 ", []) == [
            1.0,
            2.0,
            3.0,
            4.0,
            5.0,
            6.0,
        ]

        # Test decimal numbers
        assert parse_latency_buckets("0.5,1.5,2.5", []) == [0.5, 1.5, 2.5]

    def test_parse_latency_buckets_invalid(self):
        # Test negative numbers
        with pytest.raises(ValueError, match=".*must be positive.*"):
            parse_latency_buckets("-1,1,2,3,4", [])

        # Test non-ascending order
        with pytest.raises(ValueError, match=".*be in strictly ascending order*"):
            parse_latency_buckets("4,3,2,1", [])

        # Test duplicate values
        with pytest.raises(ValueError, match=".*be in strictly ascending order.*"):
            parse_latency_buckets("1,2,2,3,4", [])

        # Test invalid number format
        with pytest.raises(ValueError, match=".*Invalid.*format.*"):
            parse_latency_buckets("1,2,3,4,a", [])

        # Test empty list
        with pytest.raises(ValueError, match=".*could not convert.*"):
            parse_latency_buckets(",,,", [])

        # Test invalid separators
        with pytest.raises(ValueError, match=".*could not convert.*"):
            parse_latency_buckets("1;2;3;4", [])


@pytest.fixture
def mock_environ():
    with patch.dict(os.environ, {}, clear=True) as mock_env:
        yield mock_env


class TestEnvValueFunctions:
    def test_get_env_int(self, mock_environ):
        assert get_env_int("RAY_SERVE_TEST_VAR", 0) == 0

        mock_environ["RAY_SERVE_TEST_VAR"] = "42"
        assert get_env_int("RAY_SERVE_TEST_VAR", 0) == 42

        mock_environ["RAY_SERVE_TEST_VAR"] = "-1"
        assert get_env_int("RAY_SERVE_TEST_VAR", 0) == -1

        mock_environ["RAY_SERVE_TEST_VAR"] = "0.1"
        with pytest.raises(ValueError, match=".*`0.1` cannot be converted to `int`!*"):
            get_env_int_positive("RAY_SERVE_TEST_VAR", 5)

        mock_environ["RAY_SERVE_TEST_VAR"] = "abc"
        with pytest.raises(ValueError, match=".*`abc` cannot be converted to `int`!*"):
            get_env_int_positive("RAY_SERVE_TEST_VAR", 5)

        with pytest.raises(ValueError, match=".*require prefix `RAY_SERVE_`*"):
            get_env_int_positive("NO_PREFIX", 5)

    def test_get_env_int_positive(self, mock_environ):
        assert get_env_int_positive("RAY_SERVE_TEST_VAR", 1) == 1

        mock_environ["RAY_SERVE_TEST_VAR"] = "42"
        assert get_env_int_positive("RAY_SERVE_TEST_VAR", 1) == 42

        mock_environ["RAY_SERVE_TEST_VAR"] = "-1"
        with pytest.raises(ValueError, match=".*Expected positive `int`.*"):
            get_env_int_positive("RAY_SERVE_TEST_VAR", 5)

    def test_get_env_int_non_negative(self, mock_environ):
        assert get_env_int_non_negative("RAY_SERVE_TEST_VAR", 0) == 0
        assert get_env_int_non_negative("RAY_SERVE_TEST_VAR", 1) == 1

        mock_environ["RAY_SERVE_TEST_VAR"] = "42"
        assert get_env_int_non_negative("RAY_SERVE_TEST_VAR", 0) == 42

        mock_environ["RAY_SERVE_TEST_VAR"] = "-1"
        with pytest.raises(ValueError, match=".*Expected non negative `int`.*"):
            get_env_int_non_negative("RAY_SERVE_TEST_VAR", 5)

        with pytest.raises(ValueError, match=".*Expected non negative `int`.*"):
            get_env_int_non_negative("RAY_SERVE_TEST_VAR_FROM_DEFAULT", -1)

    def test_get_env_float(self, mock_environ):
        assert get_env_float("RAY_SERVE_TEST_VAR", 0.0) == 0.0

        mock_environ["RAY_SERVE_TEST_VAR"] = "3.14"
        assert get_env_float("RAY_SERVE_TEST_VAR", 0.0) == 3.14

        mock_environ["RAY_SERVE_TEST_VAR"] = "-2.5"
        assert get_env_float("RAY_SERVE_TEST_VAR", 0.0) == -2.5

        mock_environ["RAY_SERVE_TEST_VAR"] = "abc"
        with pytest.raises(
            ValueError, match=".*`abc` cannot be converted to `float`!*"
        ):
            get_env_float("RAY_SERVE_TEST_VAR", 0.0)

    def test_get_env_float_positive(self, mock_environ):
        assert get_env_float_positive("RAY_SERVE_TEST_VAR", 1.5) == 1.5
        assert get_env_float_positive("RAY_SERVE_TEST_VAR", None) is None

        mock_environ["RAY_SERVE_TEST_VAR"] = "42.5"
        assert get_env_float_positive("RAY_SERVE_TEST_VAR", 1.0) == 42.5

        mock_environ["RAY_SERVE_TEST_VAR"] = "-1.2"
        with pytest.raises(ValueError, match=".*Expected positive `float`.*"):
            get_env_float_positive("RAY_SERVE_TEST_VAR", 5.0)

        with pytest.raises(ValueError, match=".*Expected positive `float`.*"):
            get_env_float_positive("RAY_SERVE_TEST_VAR_FROM_DEFAULT", 0.0)

        with pytest.raises(ValueError, match=".*Expected positive `float`.*"):
            get_env_float_positive("RAY_SERVE_TEST_VAR_FROM_DEFAULT", -1)

    def test_get_env_float_non_negative(self, mock_environ):
        assert get_env_float_non_negative("RAY_SERVE_TEST_VAR", 0.0) == 0.0
        assert get_env_float_non_negative("RAY_SERVE_TEST_VAR", 1.5) == 1.5

        mock_environ["RAY_SERVE_TEST_VAR"] = "42.5"
        assert get_env_float_non_negative("RAY_SERVE_TEST_VAR", 0.0) == 42.5

        mock_environ["RAY_SERVE_TEST_VAR"] = "-1.2"
        with pytest.raises(ValueError, match=".*Expected non negative `float`.*"):
            get_env_float_non_negative("RAY_SERVE_TEST_VAR", 5.0)

    def test_get_env_str(self, mock_environ):
        mock_environ["RAY_SERVE_TEST_STR"] = "hello"
        assert get_env_str("RAY_SERVE_TEST_STR", "default") == "hello"

        assert get_env_str("RAY_SERVE_NONEXISTENT_VAR", "default_str") == "default_str"

        assert get_env_str("RAY_SERVE_NONEXISTENT_VAR", None) is None

    def test_get_env_bool(self, mock_environ):
        mock_environ["RAY_SERVE_TEST_BOOL_TRUE"] = "1"
        assert get_env_bool("RAY_SERVE_TEST_BOOL_TRUE", "0") is True

        # Test with any other value (False)
        mock_environ["RAY_SERVE_TEST_BOOL_FALSE"] = "true"
        assert get_env_bool("RAY_SERVE_TEST_BOOL_FALSE", "0") is False
        mock_environ["RAY_SERVE_TEST_BOOL_FALSE2"] = "yes"
        assert get_env_bool("RAY_SERVE_TEST_BOOL_FALSE2", "0") is False

        # Test with default when environment variable not set
        assert get_env_bool("RAY_SERVE_NONEXISTENT_VAR", "1") is True
        assert get_env_bool("RAY_SERVE_NONEXISTENT_VAR", "0") is False


class TestValidation:
    @pytest.mark.parametrize(
        "name",
        [
            "RAY_SERVE_FOO",
            "RAY_SERVE__DOUBLE_UNDERSCORE",
            "RAY_SERVE_123",
            "RAY_SERVE_VAR_NAME",
        ],
    )
    def test_validate_name_accepts_valid_prefix(self, name):
        # Should not raise
        assert _validate_name(name) is None

    @pytest.mark.parametrize(
        "name",
        [
            "",
            "RAY_SERVE",  # missing trailing underscore and name
            "SERVE_VAR",
            "ray_SERVE_BAR",
            "RAY_service_VAR",
        ],
    )
    def test_validate_name_rejects_invalid_prefix(self, name):
        with pytest.raises(ValueError, match=".*require prefix `RAY_SERVE_`*"):
            _validate_name(name)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
