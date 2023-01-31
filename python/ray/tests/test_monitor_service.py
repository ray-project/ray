import binascii
import pytest

import ray
import grpc
from ray.core.generated import monitor_pb2, monitor_pb2_grpc
from ray.cluster_utils import Cluster


@pytest.fixture
def monitor_stub(ray_start_regular_shared):
    channel = grpc.insecure_channel(ray_start_regular_shared["gcs_address"])

    return monitor_pb2_grpc.MonitorGcsServiceStub(channel)


@pytest.fixture
def monitor_stub_with_cluster():
    cluster = Cluster()
    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    channel = grpc.insecure_channel(cluster.gcs_address)
    stub = monitor_pb2_grpc.MonitorGcsServiceStub(channel)

    cluster.connect()

    yield stub, cluster
    ray.shutdown()
    cluster.shutdown()


def test_ray_version(monitor_stub):
    request = monitor_pb2.GetRayVersionRequest()
    response = monitor_stub.GetRayVersion(request)
    assert response.version == ray.__version__


def count_live_nodes():
    return sum(1 for node in ray.nodes() if node["Alive"])


def test_drain_and_kill_node(monitor_stub_with_cluster):
    monitor_stub, cluster = monitor_stub_with_cluster

    head_node = ray.nodes()[0]["NodeID"]

    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()

    assert count_live_nodes() == 2

    node_ids = {node["NodeID"] for node in ray.nodes()}
    worker_nodes = node_ids - {head_node}
    assert len(worker_nodes) == 1

    worker_node_id = next(iter(worker_nodes))

    request = monitor_pb2.DrainAndKillNodeRequest(
        node_ids=[binascii.unhexlify(worker_node_id)]
    )
    response = monitor_stub.DrainAndKillNode(request)

    assert response.drained_nodes == request.node_ids
    assert count_live_nodes() == 1

    response = monitor_stub.DrainAndKillNode(request)
    assert response.drained_nodes == request.node_ids
    assert count_live_nodes() == 1
