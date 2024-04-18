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


@pytest.mark.parametrize("remote", [True])
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

        def allocate_local_reader_channel(self, writer_channel, num_readers):
            print("HERE\n")
            self._reader_chan = ray_channel.Channel(
                ray.runtime_context.get_runtime_context().get_node_id(),
                _writer_channel=writer_channel,
                num_readers=num_readers,
            )
            return self._reader_chan

        def send_channel(self, reader_channel):
            self._reader_chan = reader_channel

        def pass_channel(self, channel):
            print("pass channel here\n")
            if ray.runtime_context.get_runtime_context().get_node_id() == channel._local_node_id:
                # Reader and writer are on the same node.
                self._reader_chan = channel
            else:
                # Reader and writer are on different nodes.
                self._reader_chan = ray_channel.Channel(
                    ray.runtime_context.get_runtime_context().get_node_id(),
                    _writer_channel=channel,
                    # Pass 1 only for this reader. If the channel is already registered on the
                    # local node, `num_readers` will be incremented by 1.
                    num_readers=1,
                )

        def read(self, num_reads):
            print("here to read\n")
            for i in range(num_reads):
                self._reader_chan.begin_read()
                self._reader_chan.end_read()

    readers = [Reader.remote() for _ in range(num_readers)]
    if remote:
        reader_node_id = ray.get(readers[0].get_node_id.remote())
        channel = ray_channel.Channel(reader_node_id, 1000, num_readers)
        for reader in readers:
            reader_channel = ray.get(reader.pass_channel.remote(channel))
    else:
        print("hmmm\n")
        channel = ray_channel.Channel(
            ray.runtime_context.get_runtime_context().get_node_id(),
            1000,
            num_readers=num_readers,
        )
        reader_channel = channel

    # ray.get([reader.send_channel.remote(reader_channel) for reader in readers])
    for j in range(num_iterations):
        print("start A\n")
        work = [reader.read.remote(num_writes) for reader in readers]
        print("start B\n")
        start = time.perf_counter()
        for i in range(num_writes):
            print("start C\n")
            channel.write(b"x")
            print("start D\n")
        end = time.perf_counter()
        ray.get(work)
        print(end - start, 10_000 / (end - start))


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
