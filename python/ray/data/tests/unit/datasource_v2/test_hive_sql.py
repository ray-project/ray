"""Hive table identifier quoting."""

import sys

import pytest

from ray.data._internal.datasource_v2.formats.hive.hive_sql import quote_identifier


def test_hive_identifier_quoting():
    assert quote_identifier("select 类别`") == "`select 类别```"
    for name in ("", "bad\0name"):
        with pytest.raises(ValueError, match="identifier"):
            quote_identifier(name)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
