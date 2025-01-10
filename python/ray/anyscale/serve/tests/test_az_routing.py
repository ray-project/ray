import pytest

import ray
from ray import serve
from ray._private.test_utils import SignalActor
from ray.anyscale._private.constants import ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL
from ray.cluster_utils import Cluster
from ray.serve.handle import DeploymentHandle
from ray.tests.conftest import *  # noqa


@pytest.fixture
def ray_cluster():
    cluster = Cluster()
    yield cluster
    serve.shutdown()
    ray.shutdown()
    cluster.shutdown()


def test_handle_prefers_same_az(ray_cluster):
    """Test locality routing.

    Handles should first prefer replicas on the same node,
    then replicas in the same AZ, then fallback to all replicas.
    """

    cluster = ray_cluster
    # head node
    cluster.add_node(
        num_cpus=1,
        resources={"head": 1},
        labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "us-west"},
    )
    ray.init(address=cluster.address)
    # worker nodes
    cluster.add_node(
        num_cpus=1,
        resources={"worker1": 1},
        labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "us-west"},
    )
    cluster.add_node(
        num_cpus=1,
        resources={"worker2": 1},
        labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "us-east"},
    )
    cluster.wait_for_nodes()

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    head_node_id = ray.get(get_node_id.options(resources={"head": 1}).remote())
    worker1_node_id = ray.get(get_node_id.options(resources={"worker1": 1}).remote())
    worker2_node_id = ray.get(get_node_id.options(resources={"worker2": 1}).remote())

    signal = SignalActor.remote()

    @serve.deployment(num_replicas=3, max_ongoing_requests=1)
    def inner(block_on_signal):
        if block_on_signal:
            ray.get(signal.wait.remote())

        return ray.get_runtime_context().get_node_id()

    # Schedule Outer deployment on head node
    @serve.deployment(
        num_replicas=1, ray_actor_options={"num_cpus": 0, "resources": {"head": 1}}
    )
    class Outer:
        def __init__(self, inner_handle: DeploymentHandle):
            self._h = inner_handle.options(_prefer_local_routing=True)

        async def __call__(self, block_on_signal: bool = False) -> str:
            return await self._h.remote(block_on_signal)

    # Outer deployment will be on head node, inner deployment will have 3 replicas
    # across all 3 nodes
    h = serve.run(Outer.bind(inner.bind()))

    # Requests should be sent to inner replica on head node
    for _ in range(10):
        assert h.remote().result() == head_node_id

    blocked_ref_head = h.remote(block_on_signal=True)
    with pytest.raises(TimeoutError):
        blocked_ref_head.result(timeout_s=1)

    # With inner replica on head node blocked, requests should be sent
    # to inner replica on worker node 1
    for _ in range(10):
        assert h.remote().result() == worker1_node_id

    blocked_ref_worker1 = h.remote(block_on_signal=True)
    with pytest.raises(TimeoutError):
        blocked_ref_worker1.result(timeout_s=1)

    # With inner replica on head node and worker node 1 blocked,
    # requests should be sent to inner replica on worker node 2
    for _ in range(10):
        assert h.remote().result() == worker2_node_id

    ray.get(signal.send.remote())
    assert blocked_ref_head.result() == head_node_id
    assert blocked_ref_worker1.result() == worker1_node_id


def test_handle_prefers_same_az_without_prefer_node(ray_cluster):
    """Test locality routing.

    With node locality routing turned off, handles should prefer replicas
    in the same AZ, then fallback to all replicas.
    """

    cluster = ray_cluster
    # head node
    cluster.add_node(
        num_cpus=1,
        resources={"head": 1},
        labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "us-west"},
    )
    ray.init(address=cluster.address)
    # worker nodes
    cluster.add_node(
        num_cpus=1,
        resources={"worker1": 1},
        labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "us-west"},
    )
    cluster.add_node(
        num_cpus=1,
        resources={"worker2": 1},
        labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "us-east"},
    )
    cluster.wait_for_nodes()

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    head_node_id = ray.get(get_node_id.options(resources={"head": 1}).remote())
    worker1_node_id = ray.get(get_node_id.options(resources={"worker1": 1}).remote())
    worker2_node_id = ray.get(get_node_id.options(resources={"worker2": 1}).remote())

    signal = SignalActor.remote()

    @serve.deployment(num_replicas=3, max_ongoing_requests=1)
    def inner(block_on_signal):
        if block_on_signal:
            ray.get(signal.wait.remote())

        return ray.get_runtime_context().get_node_id()

    # Schedule Outer deployment on head node
    @serve.deployment(
        num_replicas=1, ray_actor_options={"num_cpus": 0, "resources": {"head": 1}}
    )
    class Outer:
        def __init__(self, inner_handle: DeploymentHandle):
            self._h = inner_handle.options(_prefer_local_routing=False)

        async def __call__(self, block_on_signal: bool = False) -> str:
            return await self._h.remote(block_on_signal)

    # Outer deployment will be on head node, inner deployment will have 3 replicas
    # across all 3 nodes
    h = serve.run(Outer.bind(inner.bind()))

    # Requests should be sent to nodes in the same AZ (and since node locality is off,
    # requests should be spread across both nodes)
    nodes_requested = {h.remote().result() for _ in range(10)}
    assert nodes_requested == {head_node_id, worker1_node_id}

    blocked_ref1 = h.remote(block_on_signal=True)
    with pytest.raises(TimeoutError):
        blocked_ref1.result(timeout_s=1)

    # With inner replica on one node blocked, requests should be sent to
    # the other node in the same AZ
    nodes_requested = {h.remote().result() for _ in range(10)}
    assert len(nodes_requested) == 1 and nodes_requested < {
        head_node_id,
        worker1_node_id,
    }

    blocked_ref2 = h.remote(block_on_signal=True)
    with pytest.raises(TimeoutError):
        blocked_ref2.result(timeout_s=1)

    # With inner replica on head node and worker node 1 blocked,
    # requests should be sent to inner replica on worker node 2
    for _ in range(10):
        assert h.remote().result() == worker2_node_id

    ray.get(signal.send.remote())
    res1 = blocked_ref1.result()
    res2 = blocked_ref2.result()
    assert res1 in {head_node_id, worker1_node_id}
    assert res2 in {head_node_id, worker1_node_id}
    assert res1 != res2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
