# coding: utf-8
import logging
import os
import sys

import pytest

import ray
import ray.cluster_utils
from ray.dag import InputNode, MultiOutputNode, compiled_dag_node
from ray.tests.conftest import *  # noqa


logger = logging.getLogger(__name__)

if sys.platform != "linux":
    pytest.skip("Skipping, requires Linux.", allow_module_level=True)


def enable_torch_channel():
    # Patch these flags to force enable this for test.
    compiled_dag_node.USE_TORCH_CHANNEL = True
    compiled_dag_node.USE_TORCH_BROADCAST = True


@ray.remote
class Actor:
    def __init__(self, init_value):
        print("__init__ PID", os.getpid())

    def noop(self, _):
        return b"value"


def test_basic(ray_start_regular):
    # Run in a separate process since proper teardown isn't implemented yet.
    @ray.remote
    def f():
        enable_torch_channel()

        a = Actor.remote(0)
        with InputNode() as i:
            dag = a.noop.bind(i)

        compiled_dag = dag.experimental_compile()

        for i in range(3):
            output_channel = compiled_dag.execute(b"input")
            result = output_channel.begin_read()
            assert result == b"value"
            output_channel.end_read()

        compiled_dag.teardown()

    ray.get(f.remote())


def test_broadcast(ray_start_regular):
    # Run in a separate process since proper teardown isn't implemented yet.
    @ray.remote
    def f():
        enable_torch_channel()

        actors = [Actor.remote(0) for _ in range(4)]
        with InputNode() as i:
            out = [a.noop.bind(i) for a in actors]
            dag = MultiOutputNode(out)

        compiled_dag = dag.experimental_compile()

        for i in range(3):
            output_channels = compiled_dag.execute(1)
            results = [chan.begin_read() for chan in output_channels]
            assert results == [b"value"] * 4
            for chan in output_channels:
                chan.end_read()

        compiled_dag.teardown()

    ray.get(f.remote())


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
