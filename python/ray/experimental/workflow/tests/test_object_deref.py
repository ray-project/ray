from typing import List, Dict

import pytest

import numpy as np
import ray
from ray.experimental import workflow

# alias because the original type is too long
RRef = ray.ObjectRef


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
def deref_check(u: int, v: "RRef[int]", w: "List[RRef[RRef[int]]]", x: str,
                y: List[str], z: List[Dict[str, str]]):
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
def test_objectref_inputs_exception():
    ray.init()

    with pytest.raises(ValueError):
        output = workflow.run(receive_data.step(ray.put([42])))
        assert ray.get(output)
    ray.shutdown()


@pytest.mark.skip(reason="no support for ObjectRef checkpointing yet")
def test_objectref_inputs():
    ray.init()

    output = workflow.run(
        deref_check.step(
            ray.put(42), nested_ref.remote(), [nested_ref.remote()],
            nested_workflow.step(10), [nested_workflow.step(9)], [{
                "output": nested_workflow.step(7)
            }]))
    assert ray.get(output)
    ray.shutdown()


def test_object_deref():
    ray.init()

    x = empty_list.step()
    output = workflow.run(deref_shared.step(x, x))
    assert ray.get(output)

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
    arr: np.ndarray = ray.get(workflow.run(receive_data.step(obj)))
    assert np.array_equal(arr, np.ones(4096))

    ray.shutdown()
