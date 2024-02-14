# coding: utf-8
import logging
import os
import sys

import pytest

import ray
import ray.cluster_utils
from ray.dag import InputNode, MultiOutputNode, compiled_dag_node
from ray.util import collective as col
from ray.tests.conftest import *  # noqa


logger = logging.getLogger(__name__)

if sys.platform != "linux":
    pytest.skip("Skipping, requires Linux.", allow_module_level=True)


MAX_BUFFER_SIZE = int(100 * 1e6)
USE_TORCH_CHANNEL = bool(int(os.environ.get("USE_TORCH_CHANNEL", "0")))
USE_COLLECTIVE_CHANNEL = bool(int(os.environ.get("USE_COLLECTIVE_CHANNEL", "0")))
if USE_COLLECTIVE_CHANNEL:
    print("USE_COLLECTIVE_CHANNEL")
    from ray.experimental.collective_channel import RayCollectiveChannel as Channel
elif USE_TORCH_CHANNEL:
    print("USE_TORCH_CHANNEL")
    from ray.experimental.torch_channel import TorchChannel as Channel
# else:
#     raise ValueError("Should set an env var USE_TORCH_CHANNEL=1 or USE_COLLECTIVE_CHANNEL=1")


@ray.remote
class Actor:
    def __init__(self, init_value):
        print("__init__ PID", os.getpid())

    def noop(self, _):
        # time.sleep(0.001)
        return b"value"


@pytest.mark.skipif(sys.platform != "linux", reason="Requires Linux.")
def test_put_local_get(ray_start_regular):

    @ray.remote
    class Actor:
        def __init__(self):
            self.chan = Channel(
                MAX_BUFFER_SIZE,
                [0],
                0,
            )

        def recv(self):
            return_val = self.chan.begin_read()
            self.chan.end_read()
            return return_val

    actor = Actor.remote()
    chan = Channel(
        MAX_BUFFER_SIZE,
        [1],
        0,
    )
    col.create_and_init_collective_group([actor], include_driver=True)

    for i in range(1000):
        val = i.to_bytes(8, "little")
        recv_fut = actor.recv.remote()
        chan.write(val)
        assert ray.get(recv_fut) == val


def test_basic(ray_start_regular):
    # Run in a separate process since proper teardown isn't implemented yet.
    @ray.remote
    def f():
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
        elapsed = []
        NUM_ACTORS = 2

        actors = [Actor.remote(0) for _ in range(NUM_ACTORS)]
        with InputNode() as i:
            out = [a.noop.bind(i) for a in actors]
            dag = MultiOutputNode(out)

        compiled_dag = dag.experimental_compile(buffer_size_bytes=1000)

        it = 1
        s = time.time()
        for i in range(it):
            g = time.time()
            output_channels = compiled_dag.execute(b"input")
            # TODO(sang): We should batch wait to improve throughput.
            # results = batch_wait(output_channels)
            results = [chan.begin_read() for chan in output_channels]
            assert results == [b"value"] * NUM_ACTORS
            for chan in output_channels:
                chan.end_read()
            elapsed.append((time.time() - g) * 1000 * 1000)
        elapsed_s = (time.time() - s)

        print("done?")
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
