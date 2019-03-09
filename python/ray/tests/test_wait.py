from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import pytest
import time

import ray
from ray.exceptions import RayActorError
from ray.tests.cluster_utils import Cluster


@pytest.fixture
def remote_node_cluster():
    internal_config = json.dumps({
        "initial_reconstruction_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 10
    })

    cluster = Cluster(
        initialize_head=True,
        connect=True,
        head_node_args={
            "num_cpus": 0,
            "_internal_config": internal_config
        })

    node = cluster.add_node(num_cpus=1, _internal_config=internal_config)

    yield cluster, node

    ray.shutdown()
    cluster.shutdown()


@pytest.mark.timeout(10)
def test_dead_actor_methods_ready(remote_node_cluster):
    """Tests that methods completed by dead actors are returned as ready"""
    cluster, node = remote_node_cluster

    @ray.remote
    class Actor(object):
        def __init__(self):
            pass

        def ping(self):
            time.sleep(1)

    a = Actor.remote()

    ray.get(a.ping.remote())

    ping_id = a.ping.remote()
    cluster.remove_node(node)

    ready = []
    while len(ready) == 0:
        ready, _ = ray.wait([ping_id], timeout=0.01)
        time.sleep(1)

    with pytest.raises(RayActorError):
        ray.get(ready)
