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

logger = logging.getLogger(__name__)


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "darwin",
    reason="Requires Linux or Mac.",
)
def test_put_local_get(ray_start_regular):
    chan = ray_channel.Channel([None], 1000)

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
def test_errors(ray_start_regular):
    @ray.remote
    class Actor:
        def make_chan(self, do_write=True):
            self.chan = ray_channel.Channel([None], 1000)
            if do_write:
                self.chan.write(b"hello")
            return self.chan

    a = Actor.remote()
    # Multiple consecutive reads from the same process are fine.
    chan = ray.get(a.make_chan.remote(do_write=True))
    assert chan.begin_read() == b"hello"
    chan.end_read()

    @ray.remote
    class Reader:
        def __init__(self):
            pass

        def read(self, chan):
            return chan.begin_read()

    # Multiple reads from n different processes, where n > num_readers, errors.
    chan = ray.get(a.make_chan.remote(do_write=True))
    readers = [Reader.remote(), Reader.remote()]
    # At least 1 reader
    with pytest.raises(ray.exceptions.RayTaskError) as exc_info:
        ray.get([reader.read.remote(chan) for reader in readers])
    assert "ray.exceptions.RaySystemError" in str(exc_info.value)


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "darwin",
    reason="Requires Linux or Mac.",
)
def test_put_different_meta(ray_start_regular):
    chan = ray_channel.Channel([None], 1000)

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

    # Cannot put a serialized value larger than the allocated buffer.
    with pytest.raises(ValueError):
        _test(np.random.rand(100))

    _test(np.random.rand(1))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "darwin",
    reason="Requires Linux or Mac.",
)
@pytest.mark.parametrize("num_readers", [1, 4])
def test_put_remote_get(ray_start_regular, num_readers):
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

    chan = ray_channel.Channel(readers, 1000)
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


@pytest.mark.parametrize("remote", [True, False])
def test_remote_reader(ray_start_cluster, remote):
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
    channel = ray_channel.Channel(readers, 1000)

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


@pytest.mark.parametrize("remote", [True, False])
def test_remote_reader_close(ray_start_cluster, remote):
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
    channel = ray_channel.Channel(readers, 1000)

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
