# coding: utf-8
import logging
import os
import sys

import pytest

import ray
import ray.cluster_utils
from ray.dag import InputNode, MultiOutputNode, compiled_dag_node
from ray.experimental.torch_channel import batch_wait
from ray.tests.conftest import *  # noqa


logger = logging.getLogger(__name__)

if sys.platform != "linux":
    pytest.skip("Skipping, requires Linux.", allow_module_level=True)


def enable_torch_channel():
    # Patch these flags to force enable this for test.
    # compiled_dag_node.USE_TORCH_CHANNEL = True
    # compiled_dag_node.USE_TORCH_BROADCAST = True
    pass


@ray.remote
class Actor:
    def __init__(self, init_value):
        print("__init__ PID", os.getpid())

    def noop(self, _):
        time.sleep(0.001)
        return b"value"


def test_basic(ray_start_regular):
    # Run in a separate process since proper teardown isn't implemented yet.
    @ray.remote
    def f():
        enable_torch_channel()

        a = Actor.remote(0)
        with InputNode() as i:
            dag = a.noop.bind(i)

        compiled_dag = dag.experimental_compile(buffer_size_bytes=1000)

        it = 10000
        s = time.time()
        for i in range(it):
            output_channel = compiled_dag.execute(b"input")
            result = output_channel.begin_read()
            assert result == b"value"
            output_channel.end_read()
        elapsed_s = (time.time() - s)

        compiled_dag.teardown()

        return  it / elapsed_s

    throughput = ray.get(f.remote())
    print("throughput: ", throughput, "it/s")


def test_broadcast(ray_start_regular):
    # Run in a separate process since proper teardown isn't implemented yet.
    @ray.remote
    def f():
        enable_torch_channel()
        elapsed = []

        actors = [Actor.remote(0) for _ in range(8)]
        with InputNode() as i:
            out = [a.noop.bind(i) for a in actors]
            dag = MultiOutputNode(out)

        compiled_dag = dag.experimental_compile(buffer_size_bytes=1000)

        it = 10000
        s = time.time()
        for i in range(it):
            g = time.time()
            output_channels = compiled_dag.execute(b"input")
            # TODO(sang): We should batch wait to improve throughput.
            # results = batch_wait(output_channels)
            results = [chan.begin_read() for chan in output_channels]
            # assert results == [b"value"] * 4
            for chan in output_channels:
                chan.end_read()
            elapsed.append((time.time() - g) * 1000 * 1000)
        elapsed_s = (time.time() - s)

        compiled_dag.teardown()
        return  it / elapsed_s, elapsed

    throughput, elapsed = ray.get(f.remote())
    print("throughput: ", throughput, "it/s")
    print("p50: ", sorted(elapsed)[len(elapsed) // 2])


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
