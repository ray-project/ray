from typing import List, Dict

from ray.tests.conftest import *  # noqa

import pytest

import numpy as np

import ray
from ray import ObjectRef
from ray import workflow


def test_objectref_inputs(workflow_start_regular_shared):
    @ray.remote
    def nested_workflow(n: int):
        if n <= 0:
            return "nested"
        else:
            return workflow.continuation(nested_workflow.bind(n - 1))

    @ray.remote
    def deref_check(u: int, x: str, y: List[str], z: List[Dict[str, str]]):
        try:
            return (
                u == 42
                and x == "nested"
                and isinstance(y[0], ray.ObjectRef)
                and ray.get(y) == ["nested"]
                and isinstance(z[0]["output"], ray.ObjectRef)
                and ray.get(z[0]["output"]) == "nested"
            ), f"{u}, {x}, {y}, {z}"
        except Exception as e:
            return False, str(e)

    output, s = workflow.create(
        deref_check.bind(
            ray.put(42),
            nested_workflow.bind(10),
            [nested_workflow.bind(9)],
            [{"output": nested_workflow.bind(7)}],
        )
    ).run()
    assert output is True, s


def test_objectref_outputs(workflow_start_regular_shared):
    @ray.remote
    def nested_ref():
        return ray.put(42)

    @ray.remote
    def nested_ref_workflow():
        return nested_ref.remote()

    @ray.remote
    def return_objectrefs() -> List[ObjectRef]:
        return [ray.put(x) for x in range(5)]

    single = workflow.create(nested_ref_workflow.bind()).run()
    assert ray.get(ray.get(single)) == 42

    multi = workflow.create(return_objectrefs.bind()).run()
    assert ray.get(multi) == list(range(5))


def test_object_input_dedup(workflow_start_regular_shared):
    @workflow.step
    def empty_list():
        return [1]

    @workflow.step
    def deref_shared(x, y):
        # x and y should share the same variable.
        x.append(2)
        return y == [1, 2]

    x = empty_list.step()
    assert deref_shared.step(x, x).run()


def test_object_deref(workflow_start_regular_shared):
    @ray.remote
    def empty_list():
        return [1]

    @ray.remote
    def receive_workflow(workflow):
        pass

    @ray.remote
    def return_workflow():
        return workflow.create(empty_list.bind())

    @ray.remote
    def return_data() -> ray.ObjectRef:
        return ray.put(np.ones(4096))

    @ray.remote
    def receive_data(data: "ray.ObjectRef[np.ndarray]"):
        return ray.get(data)

    # test we are forbidden from directly passing workflow to Ray.
    x = workflow.create(empty_list.bind())
    with pytest.raises(ValueError):
        ray.put(x)
    with pytest.raises(ValueError):
        ray.get(receive_workflow.remote(x))
    with pytest.raises(ValueError):
        ray.get(return_workflow.remote())

    # test return object ref
    obj = return_data.bind()
    arr: np.ndarray = workflow.create(receive_data.bind(obj)).run()
    assert np.array_equal(arr, np.ones(4096))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
