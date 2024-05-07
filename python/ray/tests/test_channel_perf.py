# coding: utf-8
import logging
import os
import sys
import time

import pytest

import ray
import ray.cluster_utils
import ray.experimental.channel as ray_channel
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)

# Ray limits the max gRPC size to 512 MiB. We set our max size to slightly below that to
# be safe.
sizes = [1, 1024, 1024**2, 1024**2 * 500]
names = ["1 byte", "1 KiB", "1 MiB", "500 MiB"]
num_ops = [10000, 10000, 10000, 10]
num_readers = [1, 2, 4, 8]
num_cpus = 20


def _get_node_id(self):
    ret = ray.get_runtime_context().get_node_id()
    time.sleep(1)
    return ret


@ray.remote
def get_node_id(self):
    return _get_node_id(self)


def get_nodes(remote):
    while True:
        if remote:
            node_ids = ray.get(
                [get_node_id.options(num_cpus=1).remote(None) for _ in range(num_cpus)]
            )
            nodes = set()
            for node_id in node_ids:
                nodes.add(node_id)
            assert len(nodes) == 2
            a_node, b_node = nodes
            if a_node != b_node:
                break
        else:
            a_node = ray.get(get_node_id.options(num_cpus=1).remote(None))
            b_node = a_node
            if a_node == b_node:
                break

    return a_node, b_node


def channel_ping_pong(remote, obj_size, name, num_ops, num_readers):
    a_node, b_node = get_nodes(remote)

    @ray.remote(num_cpus=1)
    class A:
        def __init__(self):
            pass

        def pass_channel(self, channel):
            self._second_channel = channel

        def create_channel(self, readers, obj_size):
            self._first_channel = ray_channel.Channel(readers, obj_size)
            return self._first_channel

        def ping_pong(self, num_ops, obj_size):
            value = b"x" * obj_size

            start = time.perf_counter()
            for i in range(num_ops):
                self._first_channel.write(value)
                self._second_channel.begin_read()
                self._second_channel.end_read()
            end = time.perf_counter()
            return end - start

    @ray.remote(num_cpus=1)
    class B:
        def __init__(self):
            pass

        def pass_channel(self, channel):
            self._first_channel = channel

        def create_channel(self, readers, obj_size):
            self._second_channel = ray_channel.Channel(readers, obj_size)
            return self._second_channel

        def ping_pong(self, num_ops, obj_size, write):
            value = b"x" * obj_size if write else b""
            for i in range(num_ops):
                self._first_channel.begin_read()
                self._first_channel.end_read()
                if write:
                    self._second_channel.write(value)

    a = A.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(a_node, soft=False)
    ).remote()

    assert num_readers > 0
    b_list = [
        B.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(b_node, soft=False)
        ).remote()
        for _ in range(num_readers)
    ]

    fn = a.__ray_call__
    assert a_node == ray.get(fn.remote(_get_node_id))
    for b in b_list:
        fn = b.__ray_call__
        assert b_node == ray.get(fn.remote(_get_node_id))

    first_channel = ray.get(a.create_channel.remote(b_list, obj_size))
    second_channel = ray.get(b_list[0].create_channel.remote([a], obj_size))

    ray.get(a.pass_channel.remote(second_channel))
    for b in b_list:
        ray.get(b.pass_channel.remote(first_channel))

    num_iterations = 3
    diff_seconds = 0.0
    for i in range(num_iterations):
        a_work = a.ping_pong.remote(num_ops, obj_size)
        b_work_list = []
        for j in range(len(b_list)):
            b = b_list[j]
            write = j == 0
            b_work_list.append(b.ping_pong.remote(num_ops, obj_size, write))

        diff_seconds += ray.get(a_work)
        ray.get(b_work_list)

    diff_microseconds = diff_seconds * 1_000_000
    round_trip_latency = diff_microseconds / (num_ops * num_iterations)
    print("\nName: %s" % name)
    print("Num readers: %d" % num_readers)
    print("Time per round trip: %.2f us" % round_trip_latency)

    # Both actors have a reference to each other, so there is a reference cycle. Thus,
    # we must manually kill the actors as they cannot be killed automatically.
    ray.kill(a)
    for b in b_list:
        ray.kill(b)


def test_channel_ping_pong_remote():
    for i in range(len(sizes)):
        for num_r in num_readers:
            channel_ping_pong(True, sizes[i], names[i], num_ops[i], num_r)


def test_channel_ping_pong_local():
    for i in range(len(sizes)):
        for num_r in num_readers:
            channel_ping_pong(False, sizes[i], names[i], num_ops[i], num_r)


def vanilla_ray_ping_pong(remote, obj_size, name, num_ops, num_readers):
    a_node, b_node = get_nodes(remote)

    @ray.remote(num_cpus=1)
    class A:
        def run(self, num_ops, b_node, obj_size, num_readers):
            b_list = [
                B.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        b_node, soft=False
                    )
                ).remote()
                for _ in range(num_readers)
            ]

            value = b"x" * obj_size
            start = time.perf_counter()
            for i in range(num_ops):
                object_ref = ray.put(value)
                work = []
                for j in range(num_readers):
                    work.append(b_list[j].run.remote(object_ref, j == 0))
                for w in work:
                    ret_obj = ray.get(w)
            end = time.perf_counter()
            return end - start

    @ray.remote(num_cpus=1)
    class B:
        def run(self, obj, write):
            if write:
                return obj
            return b""

    a = A.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(a_node, soft=False)
    ).remote()

    num_iterations = 3
    diff_seconds = 0.0
    for _ in range(num_iterations):
        diff_seconds += ray.get(a.run.remote(num_ops, b_node, obj_size, num_readers))

    diff_microseconds = diff_seconds * 1_000_000
    round_trip_latency = diff_microseconds / (num_ops * num_iterations)
    print("\nName: %s" % name)
    print("Num readers: %d" % num_readers)
    print("Time per round trip: %.2f us" % round_trip_latency)


def test_vanilla_ray_ping_pong_remote():
    for i in range(len(sizes)):
        for num_r in num_readers:
            vanilla_ray_ping_pong(True, sizes[i], names[i], num_ops[i], num_r)


def test_vanilla_ray_ping_pong_local():
    for i in range(len(sizes)):
        for num_r in num_readers:
            vanilla_ray_ping_pong(False, sizes[i], names[i], num_ops[i], num_r)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
