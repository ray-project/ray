import ray
import pytest
from ray.tests.conftest import *  # noqa
from ray.experimental import workflow


@workflow.step
def identity(x):
    return x


@workflow.step
def projection(x, _):
    return x


@workflow.step
def variable_mutable():
    x = []
    a = identity.step(x)
    x.append(1)
    b = identity.step(x)
    return projection.step(a, b)


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "namespace": "workflow"
    }], indirect=True)
def test_variable_mutable(ray_start_regular):
    outputs = workflow.run(variable_mutable.step())
    assert ray.get(outputs) == []
