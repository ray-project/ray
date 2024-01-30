# coding: utf-8
import os
import sys

import pytest  # noqa


# TODO: add tests.
def test_ok():
    assert True


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
