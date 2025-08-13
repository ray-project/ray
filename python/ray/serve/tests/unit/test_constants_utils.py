import os
from unittest.mock import patch

import pytest
from testfixtures import mock

from ray.serve._private.constants_utils import (
    get_env_bool,
    get_env_float,
    get_env_float_non_negative,
    get_env_float_positive,
    get_env_float_warning,
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
        assert get_env_int("TEST_VAR", 0) == 0

        mock_environ["TEST_VAR"] = "42"
        assert get_env_int("TEST_VAR", 0) == 42

        mock_environ["TEST_VAR"] = "-1"
        assert get_env_int("TEST_VAR", 0) == -1

        mock_environ["TEST_VAR"] = "0.1"
        with pytest.raises(ValueError, match=".*`0.1` cannot be converted to `int`!*"):
            get_env_int_positive("TEST_VAR", 5)

        mock_environ["TEST_VAR"] = "abc"
        with pytest.raises(ValueError, match=".*`abc` cannot be converted to `int`!*"):
            get_env_int_positive("TEST_VAR", 5)

    def test_get_env_int_positive(self, mock_environ):
        assert get_env_int_positive("TEST_VAR", 1) == 1

        mock_environ["TEST_VAR"] = "42"
        assert get_env_int_positive("TEST_VAR", 1) == 42

        mock_environ["TEST_VAR"] = "-1"
        with pytest.raises(ValueError, match=".*Expected positive `int`.*"):
            get_env_int_positive("TEST_VAR", 5)

    def test_get_env_int_non_negative(self, mock_environ):
        assert get_env_int_non_negative("TEST_VAR", 0) == 0
        assert get_env_int_non_negative("TEST_VAR", 1) == 1

        mock_environ["TEST_VAR"] = "42"
        assert get_env_int_non_negative("TEST_VAR", 0) == 42

        mock_environ["TEST_VAR"] = "-1"
        with pytest.raises(ValueError, match=".*Expected non negative `int`.*"):
            get_env_int_non_negative("TEST_VAR", 5)

        with pytest.raises(ValueError, match=".*Expected non negative `int`.*"):
            get_env_int_non_negative("TEST_VAR_FROM_DEFAULT", -1)

    def test_get_env_float(self, mock_environ):
        assert get_env_float("TEST_VAR", 0.0) == 0.0

        mock_environ["TEST_VAR"] = "3.14"
        assert get_env_float("TEST_VAR", 0.0) == 3.14

        mock_environ["TEST_VAR"] = "-2.5"
        assert get_env_float("TEST_VAR", 0.0) == -2.5

        mock_environ["TEST_VAR"] = "abc"
        with pytest.raises(
            ValueError, match=".*`abc` cannot be converted to `float`!*"
        ):
            get_env_float("TEST_VAR", 0.0)

    def test_get_env_float_positive(self, mock_environ):
        assert get_env_float_positive("TEST_VAR", 1.5) == 1.5
        assert get_env_float_positive("TEST_VAR", None) is None

        mock_environ["TEST_VAR"] = "42.5"
        assert get_env_float_positive("TEST_VAR", 1.0) == 42.5

        mock_environ["TEST_VAR"] = "-1.2"
        with pytest.raises(ValueError, match=".*Expected positive `float`.*"):
            get_env_float_positive("TEST_VAR", 5.0)

        with pytest.raises(ValueError, match=".*Expected positive `float`.*"):
            get_env_float_positive("TEST_VAR_FROM_DEFAULT", 0.0)

        with pytest.raises(ValueError, match=".*Expected positive `float`.*"):
            get_env_float_positive("TEST_VAR_FROM_DEFAULT", -1)

    def test_get_env_float_non_negative(self, mock_environ):
        assert get_env_float_non_negative("TEST_VAR", 0.0) == 0.0
        assert get_env_float_non_negative("TEST_VAR", 1.5) == 1.5

        mock_environ["TEST_VAR"] = "42.5"
        assert get_env_float_non_negative("TEST_VAR", 0.0) == 42.5

        mock_environ["TEST_VAR"] = "-1.2"
        with pytest.raises(ValueError, match=".*Expected non negative `float`.*"):
            get_env_float_non_negative("TEST_VAR", 5.0)

    def test_get_env_str(self, mock_environ):
        mock_environ["TEST_STR"] = "hello"
        assert get_env_str("TEST_STR", "default") == "hello"

        assert get_env_str("NONEXISTENT_VAR", "default_str") == "default_str"

        assert get_env_str("NONEXISTENT_VAR", None) is None

    def test_get_env_bool(self, mock_environ):
        mock_environ["TEST_BOOL_TRUE"] = "1"
        assert get_env_bool("TEST_BOOL_TRUE", "0") is True

        # Test with any other value (False)
        mock_environ["TEST_BOOL_FALSE"] = "true"
        assert get_env_bool("TEST_BOOL_FALSE", "0") is False
        mock_environ["TEST_BOOL_FALSE2"] = "yes"
        assert get_env_bool("TEST_BOOL_FALSE2", "0") is False

        # Test with default when environment variable not set
        assert get_env_bool("NONEXISTENT_VAR", "1") is True
        assert get_env_bool("NONEXISTENT_VAR", "0") is False


class TestDeprecationFunctions:
    def test_current_behavior(self, mock_environ):
        mock_environ["OLD_VAR_NEG"] = "-1"
        assert get_env_float("OLD_VAR_NEG", 10.0) or 10.0 == -1.0
        assert (get_env_float("OLD_VAR_NEG", 0.0) or None) == -1.0

        mock_environ["OLD_VAR_ZERO"] = "0"
        assert get_env_float("OLD_VAR_ZERO", 10.0) or 10.0 == 10.0

        assert get_env_float("NOT_SET", 10.0) or 10.0 == 10.0

        assert (get_env_float("NOT_SET", 0.0) or None) is None

    @mock.patch("ray.__version__", "2.49.0")  # Version before 2.50.0
    def test_with_positive_value_before_250(self, mock_environ):
        env_name = "TEST_POSITIVE_FLOAT"
        mock_environ[env_name] = "5.5"

        result = get_env_float_warning(env_name, 10.0)

        assert result == 5.5

    @mock.patch("ray.__version__", "2.49.0")  # Version before 2.50.0
    def test_with_non_positive_value_before_250(self, mock_environ):
        env_name = "TEST_NON_POSITIVE_FLOAT"
        mock_environ[env_name] = "-2.5"

        with pytest.warns(FutureWarning) as record:
            result = get_env_float_warning(env_name, 10.0)

        assert result == -2.5
        assert len(record) == 1
        assert "will require a positive value" in str(record[0].message)

    @mock.patch("ray.__version__", "2.49.0")  # Version before 2.50.0
    def test_with_zero_value_before_250(self, mock_environ):
        env_name = "TEST_ZERO_FLOAT"
        mock_environ[env_name] = "0.0"

        with pytest.warns(FutureWarning) as record:
            result = get_env_float_warning(env_name, 10.0)

        assert result == 10.0
        assert len(record) == 1
        assert "will require a positive value" in str(record[0].message)

    @mock.patch("ray.__version__", "2.49.0")  # Version before 2.50.0
    def test_with_no_env_value_before_250(self):
        env_name = "TEST_MISSING_FLOAT"
        # Don't set environment variable

        result = get_env_float_warning(env_name, 1.0)

        assert result == 1.0

    @mock.patch("ray.__version__", "2.50.0")  # Version at 2.50.0
    def test_remain_the_same_behavior_at_2_50(self, mock_environ):
        env_name = "TEST_FLOAT"
        mock_environ[env_name] = "2.0"

        assert get_env_float_warning(env_name, 1.0) == 2.0

        mock_environ["TEST_VAR"] = "-1.2"
        assert get_env_float_warning("TEST_VAR", 5.0) == -1.2

        mock_environ["TEST_VAR"] = "0.0"
        assert get_env_float_warning("TEST_VAR", 5.0) == 5.0

    @mock.patch("ray.__version__", "2.51.0")  # Version after 2.50.0
    def test_remain_the_same_behavior_after_2_50(self, mock_environ):
        env_name = "TEST_FLOAT"
        mock_environ[env_name] = "2.0"

        assert get_env_float_warning(env_name, 1.0) == 2.0

        mock_environ["TEST_VAR"] = "-1.2"
        assert get_env_float_warning("TEST_VAR", 5.0) == -1.2

        mock_environ["TEST_VAR"] = "0.0"
        assert get_env_float_warning("TEST_VAR", 5.0) == 5.0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
