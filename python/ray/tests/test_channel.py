# coding: utf-8
import logging
import os
import sys
import time

# import numpy as np
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

        def send_channel(self, reader_channel):
            self._reader_chan = reader_channel

        def pass_channel(self, channel):
            print("pass channel here\n")
            self._reader_chan = channel

        def read(self, num_reads):
            print("here to read\n")
            for i in range(num_reads):
                print("iteration " + str(i) + "\n")
                self._reader_chan.begin_read()
                print("begin_read finished\n")
                self._reader_chan.end_read()
                print("end_read finished\n")

        def get_worker_id(self):
            worker = ray.worker.global_worker
            return worker.worker_id.hex()

    readers = [Reader.remote() for _ in range(num_readers)]

    reader_node_id = ray.runtime_context.get_runtime_context().get_node_id()
    if remote:
        reader_node_id = ray.get(readers[0].get_node_id.remote())
    worker_ids = ray.get([reader.get_worker_id.remote() for reader in readers])

    print("blah A\n")
    print("worker ids are " + str(worker_ids) + "\n")
    channel = ray_channel.Channel(reader_node_id, 1000, worker_ids, num_readers)
    print("blah B\n")

    print("Pass channel to readers, " + str(len(readers)) + "\n")
    # All readers have received the channel.
    ray.get([reader.pass_channel.remote(channel) for reader in readers])

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
