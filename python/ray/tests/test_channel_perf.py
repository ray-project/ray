# coding: utf-8
import logging
import os
import sys
import time

import pytest

import ray
import ray.cluster_utils
import ray.experimental.channel as ray_channel

logger = logging.getLogger(__name__)


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "darwin",
    reason="Requires Linux or Mac.",
)
@pytest.mark.parametrize("remote", [True, False])
def test_ping_pong(ray_start_cluster, remote):
    cluster = ray_start_cluster
    if remote:
        # This node is for the driver.
        cluster.add_node(num_cpus=0)
        ray.init(address=cluster.address)
        # Each actor runs on its own node.
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=1)
    else:
        # This node is for both actors.
        cluster.add_node(num_cpus=2)
        ray.init(address=cluster.address)

    @ray.remote(num_cpus=1)
    class ActorTo:
        def __init__(self):
            pass

        def pass_channel_to(self, channel):
            self._chan_to = channel

        def create_channel_from(self, readers):
            self._chan_from = ray_channel.Channel(readers, 1000)
            return self._chan_from

        def ping_pong(self, num_ops):
            for i in range(num_ops):
                self._chan_to.begin_read()
                self._chan_to.end_read()
                self._chan_from.write(b"x")

    @ray.remote(num_cpus=1)
    class ActorFrom:
        def __init__(self):
            pass

        def create_channel_to(self, readers):
            self._chan_to = ray_channel.Channel(readers, 1000)
            return self._chan_to

        def pass_channel_from(self, channel):
            self._chan_from = channel

        def ping_pong(self, num_ops):
            start = time.perf_counter()
            for i in range(num_ops):
                self._chan_to.write(b"x")
                self._chan_from.begin_read()
                self._chan_from.end_read()
            end = time.perf_counter()
            return end - start

    actor_to = ActorTo.remote()
    actor_from = ActorFrom.remote()

    def get_node_id(self):
        return ray.get_runtime_context().get_node_id()

    # Check that when `remote` is True, the two actors are on different nodes.
    # Otherwise, they should be on the same node.
    fn = actor_to.__ray_call__
    node_id_to = ray.get(fn.remote(get_node_id))
    fn = actor_from.__ray_call__
    node_id_from = ray.get(fn.remote(get_node_id))
    if remote:
        assert node_id_to != node_id_from
    else:
        assert node_id_to == node_id_from

    chan_to = ray.get(actor_from.create_channel_to.remote([actor_to]))
    chan_from = ray.get(actor_to.create_channel_from.remote([actor_from]))

    ray.get(actor_to.pass_channel_to.remote(chan_to))
    ray.get(actor_from.pass_channel_from.remote(chan_from))

    num_ops = 100000
    num_iterations = 3
    diff_seconds = 0.0
    for i in range(num_iterations):
        work_to = actor_to.ping_pong.remote(num_ops)
        work_from = actor_from.ping_pong.remote(num_ops)

        diff_seconds += ray.get(work_from)
        ray.get(work_to)

    diff_microseconds = diff_seconds * 100000
    round_trip_latency = diff_microseconds / (num_ops * num_iterations)
    print("Time per round trip: %.2f us" % round_trip_latency)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
