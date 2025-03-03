import pytest
from ray.serve._private.constants import (
    DEFAULT_LATENCY_BUCKET_MS,
    parse_latency_buckets,
)


def test_parse_latency_buckets():
    # Test empty string returns default buckets
    assert (
        parse_latency_buckets("", DEFAULT_LATENCY_BUCKET_MS)
        == DEFAULT_LATENCY_BUCKET_MS
    )

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


def test_parse_latency_buckets_invalid():
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
