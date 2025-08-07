import sys

import pytest

from ci.raydepsets.workspace import (
    BuildArgSet,
    _substitute_build_args,
)


def test_substitute_build_args():
    build_arg_set = BuildArgSet(
        name="test",
        build_args={"PYTHON_VERSION": "3.10"},
    )
    assert _substitute_build_args("${PYTHON_VERSION}", build_arg_set) == "3.10"
    assert _substitute_build_args("$PYTHON_VERSION", build_arg_set) == "3.10"
    assert _substitute_build_args("$$FOO", build_arg_set) == "$FOO"
    assert _substitute_build_args({
        "name": "req_${PYTHON_VERSION}",
    }, build_arg_set) == {
        "name": "req_3.10",
    }
    assert _substitute_build_args([
        "req_${PYTHON_VERSION}",
    ], build_arg_set) == [
        "req_3.10",
    ]
    assert _substitute_build_args(None, build_arg_set) is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
