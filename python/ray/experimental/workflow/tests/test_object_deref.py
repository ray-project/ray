from typing import List, Dict

from ray.tests.conftest import *  # noqa

import pytest

import numpy as np

import ray
from ray import ObjectRef
from ray.experimental import workflow


@ray.remote
def nested_ref():
    return ray.put(42)


@workflow.step
def nested_workflow(n: int):
    if n <= 0:
        return "nested"
    else:
        return nested_workflow.step(n - 1)


@workflow.step
def deref_check(u: int, v: "ObjectRef[int]",
                w: "List[ObjectRef[ObjectRef[int]]]", x: str, y: List[str],
                z: List[Dict[str, str]]):
    try:
        return (u == 42 and ray.get(v) == 42 and ray.get(ray.get(w[0])) == 42
                and x == "nested" and y[0] == "nested"
                and z[0]["output"] == "nested")
    except Exception:
        return False


@workflow.step
def deref_shared(x, y):
    # x and y should share the same variable.
    x.append(2)
    return y == [1, 2]


@workflow.step
def empty_list():
    return [1]


@ray.remote
def receive_workflow(workflow):
    pass


@ray.remote
def return_workflow():
    return empty_list.step()


@workflow.step
def return_data() -> ray.ObjectRef:
    obj = ray.put(np.ones(4096))
    return obj


@workflow.step
def receive_data(data: np.ndarray):
    return data


# TODO(suquark): Support ObjectRef checkpointing.
def test_objectref_inputs_exception(workflow_start_regular_shared):
    with pytest.raises(ValueError):
        assert receive_data.step(ray.put([42])).run()


@pytest.mark.skip(reason="no support for ObjectRef checkpointing yet")
def test_objectref_inputs(workflow_start_regular_shared):
    assert deref_check.step(
        ray.put(42), nested_ref.remote(), [nested_ref.remote()],
        nested_workflow.step(10), [nested_workflow.step(9)], [{
            "output": nested_workflow.step(7)
        }]).run()


def test_object_deref(workflow_start_regular_shared):
    x = empty_list.step()
    assert deref_shared.step(x, x).run()

    # test we are forbidden from directly passing workflow to Ray.
    x = empty_list.step()
    with pytest.raises(ValueError):
        ray.put(x)
    with pytest.raises(ValueError):
        ray.get(receive_workflow.remote(x))
    with pytest.raises(ValueError):
        ray.get(return_workflow.remote())

    # test return object ref
    obj = return_data.step()
    arr: np.ndarray = receive_data.step(obj).run()
    assert np.array_equal(arr, np.ones(4096))


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
