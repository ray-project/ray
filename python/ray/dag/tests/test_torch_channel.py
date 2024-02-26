# coding: utf-8
import logging
import os
import sys
import random

import pytest

import ray
import ray.cluster_utils
from ray.dag import InputNode, MultiOutputNode, compiled_dag_node
from ray.util import collective as col
from ray.tests.conftest import *  # noqa
from ray.experimental.collective_channel import batch_read


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
    def __init__(self, init_value, fail_after=None, sys_exit=False):
        print("__init__ PID", os.getpid())
        self.i = init_value
        self.fail_after = fail_after
        self.sys_exit = sys_exit

    def inc(self, x):
        self.i += x
        if self.fail_after and self.i > self.fail_after:
            # Randomize the failures to better cover multi actor scenarios.
            if random.random() > 0.5:
                if self.sys_exit:
                    os._exit(1)
                else:
                    raise ValueError("injected fault")
        return self.i

    def append_to(self, lst):
        lst.append(self.i)
        return lst

    def inc_two(self, x, y):
        self.i += x
        self.i += y
        return self.i

    def sleep(self, x):
        time.sleep(x)
        return x

    def raise_error(self, _):
        raise ValueError("injected fault")

    def noop(self, _):
        # time.sleep(0.015)
        return b"value"

    def set_i(self, i):
        self.i = i
        return self.i


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

    col.teardown_collective_group([actor], include_driver=True)


def test_basic(ray_start_regular):
    # Run in a separate process since proper teardown isn't implemented yet.
    a = Actor.remote(0)
    with InputNode() as i:
        dag = a.noop.bind(i)

    s = time.time()
    compiled_dag = dag.experimental_compile()
    print("Compiles took ", (time.time() - s) * 1000 * 1000, "us")
    it = 1000
    s = time.time()
    elapsed = []
    for i in range(it):
        g = time.time()
        output_channel = compiled_dag.execute(b"input")
        result = output_channel.begin_read()
        assert result == b"value"
        output_channel.end_read()
        elapsed.append((time.time() - g) * 1000 * 1000)
    elapsed_s = time.time() - s

    throughput = it / elapsed_s
    print("throughput: ", throughput, "it/s")
    print("p50: ", sorted(elapsed)[len(elapsed) // 2])
    compiled_dag.teardown()


def test_basic_multi_node(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    cluster.add_node(num_cpus=1)

    # Run in a separate process since proper teardown isn't implemented yet.
    a = Actor.options(num_cpus=1).remote(0)
    with InputNode() as i:
        dag = a.noop.bind(i)

    s = time.time()
    compiled_dag = dag.experimental_compile()
    print("Compiles took ", (time.time() - s) * 1000 * 1000, "us")

    it = 100
    s = time.time()
    for i in range(it):
        output_channel = compiled_dag.execute(b"input")
        result = output_channel.begin_read()
        assert result == b"value"
        output_channel.end_read()
    elapsed_s = time.time() - s

    throughput = it / elapsed_s
    print("throughput: ", throughput, "it/s")
    compiled_dag.teardown()


def test_broadcast(ray_start_regular):
    # Run in a separate process since proper teardown isn't implemented yet.
    elapsed = []
    NUM_ACTORS = 8

    actors = [Actor.remote(0) for _ in range(NUM_ACTORS)]
    with InputNode() as i:
        out = [a.noop.bind(i) for a in actors]
        dag = MultiOutputNode(out)

    compiled_dag = dag.experimental_compile()

    it = 100
    s = time.time()
    for i in range(it):
        g = time.time()
        output_channels = compiled_dag.execute(b"input")
        # TODO(sang): We should batch wait to improve throughput.
        # results = batch_wait(output_channels)
        # results = [chan.begin_read() for chan in output_channels]
        results = batch_read(output_channels)
        assert results == [b"value"] * NUM_ACTORS
        for chan in output_channels:
            chan.end_read()
        elapsed.append((time.time() - g) * 1000 * 1000)
    elapsed_s = time.time() - s
    throughput = it / elapsed_s
    print("throughput: ", throughput, "it/s")
    print("p50: ", sorted(elapsed)[len(elapsed) // 2])
    compiled_dag.teardown()


# TODO(sang): Chained exception is not working.
def test_dag_chain(shutdown_only):
    ray.init(num_cpus=2)

    a = Actor.remote(0)
    b = Actor.remote(0)
    with InputNode() as inp:
        dag = b.noop.bind(a.noop.bind(inp))
        # dag = a.noop.bind(inp)

    compiled_dag = dag.experimental_compile()
    IT = 10
    s = time.time()
    for _ in range(IT):
        output_channel = compiled_dag.execute(1)
        assert output_channel.begin_read() == b"value"
        output_channel.end_read()
    print("throughtput: ", IT / (time.time() - s), "it/s")

    compiled_dag.teardown()


def test_dag_exception(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.inc.bind(inp)

    compiled_dag = dag.experimental_compile()

    IT = 10
    for _ in range(IT):
        with pytest.raises(TypeError):
            output_channel = compiled_dag.execute("hello")
            output_channel.begin_read()
            output_channel.end_read()

    # Normal execution should still work.
    chan = compiled_dag.execute(1)
    assert chan.begin_read() == 1
    chan.end_read()
    compiled_dag.teardown()

    # TODO(sang): Chained exception doesn't work for both
    # shared memory and gloo.

    # We should be able to repetitively compile DAGs
    # to the same actor.
    IT = 3
    for i in range(IT):
        compiled_dag = dag.experimental_compile()
        output_channel = compiled_dag.execute(1)
        assert output_channel.begin_read() == i + 2
        output_channel.end_read()
        compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
