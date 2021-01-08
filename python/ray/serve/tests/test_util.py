import asyncio
import os
import json
from copy import deepcopy

import numpy as np
import pytest

import ray
from ray.serve.utils import (ServeEncoder, chain_future, unpack_future,
                             try_schedule_resources_on_nodes,
                             get_conda_env_dir, import_class)


def test_bytes_encoder():
    data_before = {"inp": {"nest": b"bytes"}}
    data_after = {"inp": {"nest": "bytes"}}
    assert json.loads(json.dumps(data_before, cls=ServeEncoder)) == data_after


def test_numpy_encoding():
    data = [1, 2]
    floats = np.array(data).astype(np.float32)
    ints = floats.astype(np.int32)
    uints = floats.astype(np.uint32)

    assert json.loads(json.dumps(floats, cls=ServeEncoder)) == data
    assert json.loads(json.dumps(ints, cls=ServeEncoder)) == data
    assert json.loads(json.dumps(uints, cls=ServeEncoder)) == data


@pytest.mark.asyncio
async def test_future_chaining():
    def make():
        return asyncio.get_event_loop().create_future()

    # Test 1 -> 1 chaining
    fut1, fut2 = make(), make()
    chain_future(fut1, fut2)
    fut1.set_result(1)
    assert await fut2 == 1

    # Test 1 -> 1 chaining with exception
    fut1, fut2 = make(), make()
    chain_future(fut1, fut2)
    fut1.set_exception(ValueError(""))
    with pytest.raises(ValueError):
        await fut2

    # Test many -> many chaining
    src_futs = [make() for _ in range(4)]
    dst_futs = [make() for _ in range(4)]
    chain_future(src_futs, dst_futs)
    [fut.set_result(i) for i, fut in enumerate(src_futs)]
    for i, fut in enumerate(dst_futs):
        assert await fut == i

    # Test 1 -> many unwrapping
    batched_future = make()
    single_futures = unpack_future(batched_future, 4)
    batched_future.set_result(list(range(4)))
    for i, fut in enumerate(single_futures):
        assert await fut == i

    # Test 1 -> many unwrapping with exception
    batched_future = make()
    single_futures = unpack_future(batched_future, 4)
    batched_future.set_exception(ValueError(""))
    for future in single_futures:
        with pytest.raises(ValueError):
            await future


def test_mock_scheduler():
    ray_nodes = {
        "AAA": {
            "CPU": 2.0,
            "GPU": 2.0
        },
        "BBB": {
            "CPU": 4.0,
        }
    }

    assert try_schedule_resources_on_nodes(
        [
            {
                "CPU": 2,
                "GPU": 2
            },  # node 1
            {
                "CPU": 4
            }  # node 2
        ],
        deepcopy(ray_nodes)) == [True, True]

    assert try_schedule_resources_on_nodes([
        {
            "CPU": 100
        },
        {
            "GPU": 1
        },
    ], deepcopy(ray_nodes)) == [False, True]

    assert try_schedule_resources_on_nodes(
        [
            {
                "CPU": 6
            },  # Equals to the sum of cpus but shouldn't be scheduable.
        ],
        deepcopy(ray_nodes)) == [False]


def test_get_conda_env_dir(tmp_path):
    d = tmp_path / "tf1"
    d.mkdir()
    os.environ["CONDA_PREFIX"] = str(d)
    with pytest.raises(ValueError):
        # env does not exist
        env_dir = get_conda_env_dir("tf2")
    tf2_dir = tmp_path / "tf2"
    tf2_dir.mkdir()
    env_dir = get_conda_env_dir("tf2")
    assert (env_dir == str(tmp_path / "tf2"))
    os.environ["CONDA_PREFIX"] = ""


def test_import_class():
    assert import_class("ray.serve.Client") == ray.serve.api.Client
    assert import_class("ray.serve.api.Client") == ray.serve.api.Client

    policy_cls = import_class("ray.serve.controller.TrafficPolicy")
    assert policy_cls == ray.serve.controller.TrafficPolicy

    policy = policy_cls({"endpoint1": 0.5, "endpoint2": 0.5})
    with pytest.raises(ValueError):
        policy.set_traffic_dict({"endpoint1": 0.5, "endpoint2": 0.6})
    policy.set_traffic_dict({"endpoint1": 0.4, "endpoint2": 0.6})

    print(repr(policy))


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
