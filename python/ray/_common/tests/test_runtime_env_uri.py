"""Tests for ray._common.runtime_env_uri (parse_uri re-export)."""

import sys

import pytest

from ray._common.runtime_env_uri import Protocol, parse_uri


def test_parse_uri_https():
    protocol, path = parse_uri("https://test.com/file.zip")
    assert protocol == Protocol.HTTPS
    assert "file" in path or "zip" in path


def test_parse_uri_whl_preserves_filename():
    protocol, path = parse_uri("https://test.com/pkg-1.0.0.whl")
    assert protocol == Protocol.HTTPS
    assert path == "pkg-1.0.0.whl"


def test_parse_uri_invalid_protocol_raises():
    with pytest.raises(ValueError, match="Invalid protocol"):
        parse_uri("invalid://something")


def test_parse_uri_path_raises():
    with pytest.raises(ValueError, match="Expected URI but received path"):
        parse_uri("/local/path")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
