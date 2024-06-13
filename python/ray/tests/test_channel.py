# coding: utf-8
import logging
import os
import sys
import time

import numpy as np
import pytest

import ray
import ray.cluster_utils
import ray.experimental.channel as ray_channel
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.dag.compiled_dag_node import CompiledDAG

logger = logging.getLogger(__name__)


def create_driver_actor():
    return CompiledDAG.DAGDriverProxyActor.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(), soft=False
        )
    ).remote()


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "darwin",
    reason="Requires Linux or Mac.",
)
def test_put_local_get(ray_start_regular):
    chan = ray_channel.Channel(None, [create_driver_actor()], 1000)

    num_writes = 1000
    for i in range(num_writes):
        val = i.to_bytes(8, "little")
        chan.write(val)
        assert chan.begin_read() == val
        chan.end_read()


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "darwin",
    reason="Requires Linux or Mac.",
)
@pytest.mark.parametrize("remote", [True, False])
def test_driver_as_reader(ray_start_cluster, remote):
    cluster = ray_start_cluster
    if remote:
        # This node is for the driver. num_cpus is 1 because the
        # CompiledDAG.DAGDriverProxyActor needs a place to run.
        cluster.add_node(num_cpus=1)
        ray.init(address=cluster.address)
        # This node is for the writer actor.
        cluster.add_node(num_cpus=1)
    else:
        # This node is for both the driver (including the DriverHelperActor) and the
        # writer actor.
        cluster.add_node(num_cpus=2)
        ray.init(address=cluster.address)

    @ray.remote(num_cpus=1)
    class Actor:
        def setup(self, driver_actor):
            self._channel = ray_channel.Channel(
                ray.get_runtime_context().current_actor,
                [driver_actor],
                1000,
            )

        def get_channel(self):
            return self._channel

        def write(self):
            self._channel.write(b"x")

    a = Actor.remote()
    ray.get(a.setup.remote(create_driver_actor()))
    chan = ray.get(a.get_channel.remote())

    ray.get(a.write.remote())
    assert chan.begin_read() == b"x"
    chan.end_read()


@pytest.mark.parametrize("remote", [True, False])
def test_driver_as_reader_with_resize(ray_start_cluster, remote):
    cluster = ray_start_cluster
    if remote:
        # This node is for the driver. num_cpus is 1 because the
        # CompiledDAG.DAGDriverProxyActor needs a place to run.
        cluster.add_node(num_cpus=1)
        ray.init(address=cluster.address)
        # This node is for the writer actor.
        cluster.add_node(num_cpus=1)
    else:
        # This node is for both the driver (including the DriverHelperActor) and the
        # writer actor.
        cluster.add_node(num_cpus=2)
        ray.init(address=cluster.address)

    @ray.remote(num_cpus=1)
    class Actor:
        def setup(self, driver_actor):
            self._channel = ray_channel.Channel(
                ray.get_runtime_context().current_actor,
                [driver_actor],
                1000,
            )

        def get_channel(self):
            return self._channel

        def write(self):
            self._channel.write(b"x")

        def write_large(self):
            self._channel.write(b"x" * 2000)

    a = Actor.remote()
    ray.get(a.setup.remote(create_driver_actor()))
    chan = ray.get(a.get_channel.remote())

    ray.get(a.write.remote())
    assert chan.begin_read() == b"x"
    chan.end_read()

    ray.get(a.write_large.remote())
    assert chan.begin_read() == b"x" * 2000
    chan.end_read()


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "darwin",
    reason="Requires Linux or Mac.",
)
def test_set_error_before_read(ray_start_regular):
    """
    Tests that if a channel is closed after a reader, a subsequent read does not block
    forever.
    """

    @ray.remote
    class Actor:
        def create_channel(self, writer, readers):
            self._channel = ray_channel.Channel(writer, readers, 1000)
            return self._channel

        def pass_channel(self, channel):
            self._channel = channel

        def close(self):
            self._channel.close()

        def write(self):
            self._channel.write(b"x")

        def begin_read(self):
            self._channel.begin_read()

        def end_read(self):
            self._channel.end_read()

    for _ in range(10):
        a = Actor.remote()
        b = Actor.remote()

        chan = ray.get(a.create_channel.remote(a, [b]))
        ray.get(b.pass_channel.remote(chan))

        # Indirectly registers the channel for both the writer and the reader.
        ray.get(a.write.remote())
        ray.get(b.begin_read.remote())
        ray.get(b.end_read.remote())

        # Check that the thread does not block on the second call to begin_read() below.
        # begin_read() acquires a lock, though if the lock is not released when
        # begin_read() fails (because the channel has been closed), then an additional
        # call to begin_read() *could* block.

        # We wrap both calls to begin_read() in pytest.raises() as both calls could
        # trigger an IOError exception if the channel has already been closed.
        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get([a.close.remote(), b.begin_read.remote()])
        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(b.begin_read.remote())


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "darwin",
    reason="Requires Linux or Mac.",
)
def test_errors(ray_start_regular):
    """
    Tests that an exception is thrown when there are more readers than specificed in the
    channel constructor.
    """

    @ray.remote
    class Actor:
        def make_chan(self, readers, do_write=True):
            self.chan = ray_channel.Channel(
                ray.get_runtime_context().current_actor, readers, 1000
            )
            if do_write:
                self.chan.write(b"hello")
            return self.chan

    a = Actor.remote()
    # Multiple consecutive reads from the same process are fine.
    chan = ray.get(a.make_chan.remote([create_driver_actor()], do_write=True))
    assert chan.begin_read() == b"hello"
    chan.end_read()

    @ray.remote
    class Reader:
        def __init__(self):
            pass

        def read(self, chan):
            return chan.begin_read()

    readers = [Reader.remote(), Reader.remote()]
    # Check that an exception is thrown when there are more readers than specificed in
    # the channel constructor.
    chan = ray.get(a.make_chan.remote([readers[0]], do_write=True))
    # At least 1 reader.
    with pytest.raises(ray.exceptions.RayTaskError) as exc_info:
        ray.get([reader.read.remote(chan) for reader in readers])
    assert "ray.exceptions.RaySystemError" in str(exc_info.value)


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "darwin",
    reason="Requires Linux or Mac.",
)
def test_put_different_meta(ray_start_regular):
    chan = ray_channel.Channel(None, [create_driver_actor()], 1000)

    def _test(val):
        chan.write(val)

        read_val = chan.begin_read()
        if isinstance(val, np.ndarray):
            assert np.array_equal(read_val, val)
        else:
            assert read_val == val
        chan.end_read()

    _test(b"hello")
    _test("hello")
    _test(1000)
    _test(np.random.rand(10))


def test_multiple_channels_different_nodes(ray_start_cluster):
    """
    Tests that multiple channels can be used at the same time between two nodes.
    """
    cluster = ray_start_cluster
    # This node is for the driver.
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    # This node is for the Reader actors.
    cluster.add_node(num_cpus=1)

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self):
            pass

        def read(self, channel, val):
            read_val = channel.begin_read()
            if isinstance(val, np.ndarray):
                assert np.array_equal(read_val, val)
            else:
                assert read_val == val
            channel.end_read()

    a = Actor.remote()
    chan_a = ray_channel.Channel(None, [a], 1000)
    chan_b = ray_channel.Channel(None, [a], 1000)
    channels = [chan_a, chan_b]

    val = np.random.rand(5)
    for i in range(10):
        for channel in channels:
            channel.write(val)
        for channel in channels:
            ray.get(a.read.remote(channel, val))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "darwin",
    reason="Requires Linux or Mac.",
)
def test_resize_channel_on_same_node(ray_start_regular):
    """
    Tests that the channel backing store is automatically increased when a large object
    is written to it. The writer and reader are on the same node.
    """
    chan = ray_channel.Channel(None, [create_driver_actor()], 1000)

    def _test(val):
        chan.write(val)

        read_val = chan.begin_read()
        if isinstance(val, np.ndarray):
            assert np.array_equal(read_val, val)
        else:
            assert read_val == val
        chan.end_read()

    # `np.random.rand(100)` requires more than 1000 bytes of storage. The channel is
    # allocated above with a backing store size of 1000 bytes.
    _test(np.random.rand(100))

    # Check that another write still works.
    _test(np.random.rand(5))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "darwin",
    reason="Requires Linux or Mac.",
)
def test_resize_channel_on_same_node_with_actor(ray_start_regular):
    """
    Tests that the channel backing store is automatically increased when a large object
    is written to it. The writer and reader are on the same node, and the reader is an
    actor.
    """

    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def read(self, channel, val):
            read_val = channel.begin_read()
            if isinstance(val, np.ndarray):
                assert np.array_equal(read_val, val)
            else:
                assert read_val == val
            channel.end_read()

    def _test(channel, actor, val):
        channel.write(val)
        ray.get(actor.read.remote(channel, val))

    a = Actor.remote()
    chan = ray_channel.Channel(None, [a], 1000)

    # `np.random.rand(100)` requires more than 1000 bytes of storage. The channel is
    # allocated above with a backing store size of 1000 bytes.
    _test(chan, a, np.random.rand(100))

    # Check that another write still works.
    _test(chan, a, np.random.rand(5))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "darwin",
    reason="Requires Linux or Mac.",
)
def test_resize_channel_on_different_nodes(ray_start_cluster):
    """
    Tests that the channel backing store is automatically increased when a large object
    is written to it. The writer and reader are on different nodes, and the reader is an
    actor.
    """
    cluster = ray_start_cluster
    # This node is for the driver.
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    # This node is for the Reader actors.
    cluster.add_node(num_cpus=1)

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self):
            pass

        def read(self, channel, val):
            read_val = channel.begin_read()
            if isinstance(val, np.ndarray):
                assert np.array_equal(read_val, val)
            else:
                assert read_val == val
            channel.end_read()

    def _test(channel, actor, val):
        channel.write(val)
        ray.get(actor.read.remote(channel, val))

    a = Actor.remote()
    chan = ray_channel.Channel(None, [a], 1000)

    # `np.random.rand(100)` requires more than 1000 bytes of storage. The channel is
    # allocated above with a backing store size of 1000 bytes.
    _test(chan, a, np.random.rand(100))

    # Check that another write still works.
    _test(chan, a, np.random.rand(5))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "darwin",
    reason="Requires Linux or Mac.",
)
@pytest.mark.parametrize("num_readers", [1, 4])
def test_put_remote_get(ray_start_regular, num_readers):
    """
    Tests that an actor can read objects/primitives of various types through a channel
    when the reader is spawned with @ray.remote.
    """

    @ray.remote(num_cpus=0)
    class Reader:
        def __init__(self):
            pass

        def read(self, chan, num_writes):
            for i in range(num_writes):
                val = i.to_bytes(8, "little")
                assert chan.begin_read() == val
                chan.end_read()

            for i in range(num_writes):
                val = i.to_bytes(100, "little")
                assert chan.begin_read() == val
                chan.end_read()

            for val in [
                b"hello world",
                "hello again",
                1000,
            ]:
                assert chan.begin_read() == val
                chan.end_read()

    num_writes = 1000
    readers = [Reader.remote() for _ in range(num_readers)]

    chan = ray_channel.Channel(None, readers, 1000)
    chan.ensure_registered_as_writer()

    done = [reader.read.remote(chan, num_writes) for reader in readers]
    for i in range(num_writes):
        val = i.to_bytes(8, "little")
        chan.write(val)

    # Test different data size.
    for i in range(num_writes):
        val = i.to_bytes(100, "little")
        chan.write(val)

    # Test different metadata.
    for val in [
        b"hello world",
        "hello again",
        1000,
    ]:
        chan.write(val)

    ray.get(done)


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "darwin",
    reason="Requires Linux or Mac.",
)
@pytest.mark.parametrize("remote", [True, False])
def test_remote_reader(ray_start_cluster, remote):
    """
    Tests that an actor can read objects/primitives of various types through a channel
    when the reader and writer are on the (1) same node (remote=False) along with (2)
    different nodes (remote=True).
    """
    num_readers = 10
    num_writes = 1000
    num_iterations = 3

    cluster = ray_start_cluster
    if remote:
        # This node is for the driver.
        cluster.add_node(num_cpus=0)
        ray.init(address=cluster.address)
        # This node is for the Reader actors.
        cluster.add_node(num_cpus=num_readers)
    else:
        # This node is for both the driver and the Reader actors.
        cluster.add_node(num_cpus=num_readers)
        ray.init(address=cluster.address)

    @ray.remote(num_cpus=1)
    class Reader:
        def __init__(self):
            pass

        def get_node_id(self) -> str:
            return ray.get_runtime_context().get_node_id()

        def pass_channel(self, channel):
            self._reader_chan = channel

        def read(self, num_reads):
            for i in range(num_reads):
                self._reader_chan.begin_read()
                self._reader_chan.end_read()

    readers = [Reader.remote() for _ in range(num_readers)]
    channel = ray_channel.Channel(None, readers, 1000)

    # All readers have received the channel.
    ray.get([reader.pass_channel.remote(channel) for reader in readers])

    for j in range(num_iterations):
        work = [reader.read.remote(num_writes) for reader in readers]
        start = time.perf_counter()
        for i in range(num_writes):
            channel.write(b"x")
        end = time.perf_counter()
        ray.get(work)
        print(end - start, 10_000 / (end - start))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "darwin",
    reason="Requires Linux or Mac.",
)
@pytest.mark.parametrize("remote", [True, False])
def test_remote_reader_close(ray_start_cluster, remote):
    """
    Tests that readers do not block forever on begin_read() when they close the channel.
    Specifically, the following behavior should happen:
    1. Each reader calls begin_read() on one channel.
    2. Each reader calls close() on the channel on a different thread.
    3. Each reader should unblock and return from begin_read().

    Tests (1) the readers and writer on the same node (remote=False) along with
    different nodes (remote=True).
    """
    num_readers = 10

    cluster = ray_start_cluster
    if remote:
        # This node is for the driver.
        cluster.add_node(num_cpus=0)
        ray.init(address=cluster.address)
        # This node is for the Reader actors.
        cluster.add_node(num_cpus=num_readers)
    else:
        # This node is for both the driver and the Reader actors.
        cluster.add_node(num_cpus=num_readers)
        ray.init(address=cluster.address)

    @ray.remote(num_cpus=1)
    class Reader:
        def __init__(self):
            pass

        def get_node_id(self) -> str:
            return ray.get_runtime_context().get_node_id()

        def pass_channel(self, channel):
            self._reader_chan = channel

        def read(self):
            try:
                self._reader_chan.begin_read()
            except IOError:
                pass

        def close(self):
            self._reader_chan.close()

    readers = [Reader.remote() for _ in range(num_readers)]
    channel = ray_channel.Channel(None, readers, 1000)

    # All readers have received the channel.
    ray.get([reader.pass_channel.remote(channel) for reader in readers])

    reads = [
        reader.read.options(concurrency_group="_ray_system").remote()
        for reader in readers
    ]
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(reads, timeout=1.0)

    ray.get([reader.close.remote() for reader in readers])
    ray.get(reads)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
