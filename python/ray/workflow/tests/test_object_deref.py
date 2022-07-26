from typing import List, Dict

from ray.tests.conftest import *  # noqa

import pytest

import numpy as np

import ray
from ray import ObjectRef
from ray import workflow


def test_objectref_inputs(workflow_start_regular_shared):
    from ray.workflow.tests.utils import skip_client_mode_test

    # TODO(suquark): Fix workflow with ObjectRefs as inputs under client mode.
    skip_client_mode_test()

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

    output, s = workflow.run(
        deref_check.bind(
            ray.put(42),
            nested_workflow.bind(10),
            [nested_workflow.bind(9)],
            [{"output": nested_workflow.bind(7)}],
        )
    )
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

    single = workflow.run(nested_ref_workflow.bind())
    assert ray.get(ray.get(single)) == 42

    multi = workflow.run(return_objectrefs.bind())
    assert ray.get(multi) == list(range(5))


# TODO(suquark): resume this test after Ray DAG bug fixing
@pytest.mark.skip(reason="There is a bug in Ray DAG that makes it serializable.")
def test_object_deref(workflow_start_regular_shared):
    @ray.remote
    def empty_list():
        return [1]

    @ray.remote
    def receive_workflow(workflow):
        pass

    @ray.remote
    def return_workflow():
        return empty_list.bind()

    @ray.remote
    def return_data() -> ray.ObjectRef:
        return ray.put(np.ones(4096))

    @ray.remote
    def receive_data(data: "ray.ObjectRef[np.ndarray]"):
        return ray.get(data)

    # test we are forbidden from directly passing workflow to Ray.
    x = empty_list.bind()
    with pytest.raises(ValueError):
        ray.put(x)
    with pytest.raises(ValueError):
        ray.get(receive_workflow.remote(x))
    with pytest.raises(ValueError):
        ray.get(return_workflow.remote())

    # test return object ref
    obj = return_data.bind()
    arr: np.ndarray = workflow.run(receive_data.bind(obj))
    assert np.array_equal(arr, np.ones(4096))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
