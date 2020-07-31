import pytest
try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None
import sys
import os

import ray
import ray.test_utils
import ray.cluster_utils


@pytest.mark.skipif(
    os.environ.get("RAY_GCS_ACTOR_SERVICE_ENABLED") != "true",
    reason=("This edge case is not handled when GCS actor management is off. "
            "We won't fix this because GCS actor management "
            "will be on by default anyway."))
def test_placement_group_pack(ray_start_cluster):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 2
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    placement_group_id = ray.experimental.placement_group(
        name="name", strategy="PACK", bundles=[{
            "CPU": 2
        }, {
            "CPU": 2
        }])
    actor_1 = Actor.options(
        placement_group_id=placement_group_id,
        placement_group_bundle_index=0).remote()
    actor_2 = Actor.options(
        placement_group_id=placement_group_id,
        placement_group_bundle_index=1).remote()

    print(ray.get(actor_1.value.remote()))
    print(ray.get(actor_2.value.remote()))

    # Get all actors.
    actor_infos = ray.actors()

    # Make sure all actors in counter_list are collocated in one node.
    actor_info_1 = actor_infos.get(actor_1._actor_id.hex())
    actor_info_2 = actor_infos.get(actor_2._actor_id.hex())

    assert actor_info_1 and actor_info_2

    node_of_actor_1 = actor_info_1["Address"]["NodeID"]
    node_of_actor_2 = actor_info_2["Address"]["NodeID"]
    assert node_of_actor_1 == node_of_actor_2


@pytest.mark.skipif(
    os.environ.get("RAY_GCS_ACTOR_SERVICE_ENABLED") != "true",
    reason=("This edge case is not handled when GCS actor management is off. "
            "We won't fix this because GCS actor management "
            "will be on by default anyway."))
def test_placement_group_pack_best_effort(ray_start_cluster):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    # TODO(Shanly):
    pass


@pytest.mark.skipif(
    os.environ.get("RAY_GCS_ACTOR_SERVICE_ENABLED") != "true",
    reason=("This edge case is not handled when GCS actor management is off. "
            "We won't fix this because GCS actor management "
            "will be on by default anyway."))
def test_placement_group_spread(ray_start_cluster):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 2
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    placement_group_id = ray.experimental.placement_group(
        name="name", strategy="SPREAD", bundles=[{
            "CPU": 2
        }, {
            "CPU": 2
        }])
    actor_1 = Actor.options(
        placement_group_id=placement_group_id, bundle_index=0).remote()
    actor_2 = Actor.options(
        placement_group_id=placement_group_id, bundle_index=1).remote()

    print(ray.get(actor_1.value.remote()))
    print(ray.get(actor_2.value.remote()))

    # Get all actors.
    actor_infos = ray.actors()

    # Make sure all actors in counter_list are collocated in one node.
    actor_info_1 = actor_infos.get(actor_1._actor_id.hex())
    actor_info_2 = actor_infos.get(actor_2._actor_id.hex())

    assert actor_info_1 and actor_info_2

    node_of_actor_1 = actor_info_1["Address"]["NodeID"]
    node_of_actor_2 = actor_info_2["Address"]["NodeID"]
    assert node_of_actor_1 != node_of_actor_2


@pytest.mark.skipif(
    os.environ.get("RAY_GCS_ACTOR_SERVICE_ENABLED") != "true",
    reason=("This edge case is not handled when GCS actor management is off. "
            "We won't fix this because GCS actor management "
            "will be on by default anyway."))
def test_placement_group_spread_best_effort(ray_start_cluster):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    # TODO(Shanly):
    pass


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
