# coding: utf-8
import logging
import os
import sys

import pytest

import ray

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("use_tls", [True], indirect=True)
def test_put_get_with_tls(shutdown_only, use_tls):
    ray.init(num_cpus=0)

    for i in range(100):
        value_before = i * 10**6
        object_ref = ray.put(value_before)
        value_after = ray.get(object_ref)
        assert value_before == value_after

    for i in range(100):
        value_before = i * 10**6 * 1.0
        object_ref = ray.put(value_before)
        value_after = ray.get(object_ref)
        assert value_before == value_after

    for i in range(100):
        value_before = "h" * i
        object_ref = ray.put(value_before)
        value_after = ray.get(object_ref)
        assert value_before == value_after

    for i in range(100):
        value_before = [1] * i
        object_ref = ray.put(value_before)
        value_after = ray.get(object_ref)
        assert value_before == value_after


@pytest.mark.parametrize("use_tls", [True], indirect=True)
def test_submit_with_tls(shutdown_only, use_tls):
    ray.init(num_cpus=2, num_gpus=1, resources={"Custom": 1})

    @ray.remote
    def f(n):
        return list(range(n))

    id1, id2, id3 = f._remote(args=[3], num_returns=3)
    assert ray.get([id1, id2, id3]) == [0, 1, 2]

    @ray.remote
    class Actor:
        def __init__(self, x, y=0):
            self.x = x
            self.y = y

        def method(self, a, b=0):
            return self.x, self.y, a, b

    a = Actor._remote(
        args=[0], kwargs={"y": 1}, num_gpus=1, resources={"Custom": 1})

    id1, id2, id3, id4 = a.method._remote(
        args=["test"], kwargs={"b": 2}, num_returns=4)
    assert ray.get([id1, id2, id3, id4]) == [0, 1, "test", 2]


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason=("Cryptography doesn't install in Mac build pipeline"))
@pytest.mark.parametrize("use_tls", [True], indirect=True)
def test_client_connect_to_tls_server(use_tls, init_and_serve):
    from ray.util.client import ray as ray_client
    os.environ["RAY_USE_TLS"] = "0"
    with pytest.raises(ConnectionError):
        ray_client.connect("localhost:50051")

    os.environ["RAY_USE_TLS"] = "1"
    ray_client.connect("localhost:50051")
