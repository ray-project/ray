"""Tests for ray._common.logging_constants."""

import sys

import pytest

from ray._common.logging_constants import LOGRECORD_STANDARD_ATTRS


def test_logrecord_standard_attrs_is_frozenset():
    assert isinstance(LOGRECORD_STANDARD_ATTRS, frozenset)


def test_logrecord_standard_attrs_contains_standard_names():
    expected = {"message", "levelname", "name", "pathname", "lineno", "exc_info"}
    assert expected.issubset(LOGRECORD_STANDARD_ATTRS)


def test_logrecord_standard_attrs_has_expected_size():
    assert len(LOGRECORD_STANDARD_ATTRS) >= 20


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
