import pytest
from ray.tests.conftest import *  # noqa
from ray.experimental import workflow


@workflow.step
def identity(x):
    return x


@workflow.step
def projection(x, _):
    return x


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "namespace": "workflow"
    }], indirect=True)
def test_variable_mutable(ray_start_regular):
    x = []
    a = identity.step(x)
    x.append(1)
    b = identity.step(x)
    assert projection.step(a, b).run() == []
