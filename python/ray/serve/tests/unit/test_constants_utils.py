import pytest

from ray.serve._private.constants_utils import (
    get_env_bool,
    get_env_float,
    get_env_int,
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


class TestEnvUtils:
    def test_get_env_int(self, monkeypatch):
        # Test with environment variable set to valid int
        monkeypatch.setenv("TEST_INT", "42")
        assert get_env_int("TEST_INT", 0) == 42

        # Test with environment variable not set
        assert get_env_int("NONEXISTENT_VAR", 99) == 99

        # Test with environment variable set to invalid int
        monkeypatch.setenv("TEST_INVALID_INT", "not_an_int")
        with pytest.raises(ValueError):
            get_env_int("TEST_INVALID_INT", 0)

    def test_get_env_float(self, monkeypatch):
        # Test with environment variable set to valid float
        monkeypatch.setenv("TEST_FLOAT", "3.14")
        assert get_env_float("TEST_FLOAT", 0.0) == 3.14

        # Test with environment variable not set
        assert get_env_float("NONEXISTENT_VAR", 2.71) == 2.71

        # Test with environment variable set to invalid float
        monkeypatch.setenv("TEST_INVALID_FLOAT", "not_a_float")
        with pytest.raises(ValueError):
            get_env_float("TEST_INVALID_FLOAT", 0.0)

    def test_get_env_str(self, monkeypatch):
        # Test with environment variable set
        monkeypatch.setenv("TEST_STR", "hello")
        assert get_env_str("TEST_STR", "default") == "hello"

        # Test with environment variable not set
        assert get_env_str("NONEXISTENT_VAR", "default_str") == "default_str"

        # Test with None default
        assert get_env_str("NONEXISTENT_VAR", None) is None

    def test_get_env_bool(self, monkeypatch):
        # Test with value "1" (True)
        monkeypatch.setenv("TEST_BOOL_TRUE", "1")
        assert get_env_bool("TEST_BOOL_TRUE", "0") is True

        # Test with any other value (False)
        monkeypatch.setenv("TEST_BOOL_FALSE", "true")
        assert get_env_bool("TEST_BOOL_FALSE", "0") is False

        monkeypatch.setenv("TEST_BOOL_FALSE2", "yes")
        assert get_env_bool("TEST_BOOL_FALSE2", "0") is False

        # Test with default when environment variable not set
        assert get_env_bool("NONEXISTENT_VAR", "1") is True
        assert get_env_bool("NONEXISTENT_VAR", "0") is False


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
