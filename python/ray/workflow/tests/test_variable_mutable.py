from ray.tests.conftest import *  # noqa

import ray
from ray import workflow
import pytest


@pytest.mark.skip(reason="Variable mutable is not supported right now.")
def test_variable_mutable(workflow_start_regular):
    @ray.remote
    def identity(x):
        return x

    @ray.remote
    def projection(x, _):
        return x

    x = []
    a = identity.bind(x)
    x.append(1)
    b = identity.bind(x)
    assert workflow.create(projection.bind(a, b)).run() == []


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
