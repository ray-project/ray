from ray.tests.conftest import *  # noqa
from ray import workflow
import pytest


@workflow.step
def identity(x):
    return x


@workflow.step
def projection(x, _):
    return x


@pytest.mark.skip(reason="Variable mutable is not supported right now.")
def test_variable_mutable(workflow_start_regular):
    x = []
    a = identity.step(x)
    x.append(1)
    b = identity.step(x)
    assert projection.step(a, b).run() == []


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
