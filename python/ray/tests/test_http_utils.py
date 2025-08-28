import pytest
import sys

import ray._private.http_utils as http_utils


def test_validate_http_only_endpoint_valid_endpoint_no_error():
    endpoint = "http://example.com:8000/path"
    http_utils.validate_http_only_endpoint(endpoint)


def test_validate_http_only_endpoint_invalid_endpoint_no_scheme_raises_error():
    endpoint = "example.com:8000/path"
    with pytest.raises(
        ValueError,
        match="Invalid HTTP endpoint: example.com:8000/path. The endpoint must have a scheme.",
    ):
        http_utils.validate_http_only_endpoint(endpoint)


def test_validate_http_only_endpoint_invalid_endpoint_not_http_scheme_raises_error():
    endpoint = "https://example.com:8000/path"
    with pytest.raises(
        ValueError,
        match="Invalid HTTP endpoint: https://example.com:8000/path. The endpoint must have a scheme of 'http'.",
    ):
        http_utils.validate_http_only_endpoint(endpoint)


def test_validate_http_only_endpoint_invalid_endpoint_no_hostname_raises_error():
    endpoint = "http://:8000/path"
    with pytest.raises(
        ValueError,
        match="Invalid HTTP endpoint: http://:8000/path. The endpoint must have a hostname.",
    ):
        http_utils.validate_http_only_endpoint(endpoint)


def test_validate_http_only_endpoint_invalid_endpoint_invalid_url_raises_error():
    endpoint = "invalid-url"
    with pytest.raises(ValueError):
        http_utils.validate_http_only_endpoint(endpoint)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
