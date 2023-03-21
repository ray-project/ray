import binascii
import pytest

import ray
import grpc
from ray.core.generated import monitor_pb2, monitor_pb2_grpc
from ray.cluster_utils import Cluster
from ray._private.test_utils import wait_for_condition


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


def test_drain_and_kill_node(monitor_stub_with_cluster):
    monitor_stub, cluster = monitor_stub_with_cluster

    head_node = ray.nodes()[0]["NodeID"]

    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()

    wait_for_condition(lambda: count_live_nodes() == 2)

    node_ids = {node["NodeID"] for node in ray.nodes()}
    worker_nodes = node_ids - {head_node}
    assert len(worker_nodes) == 1

    worker_node_id = next(iter(worker_nodes))

    request = monitor_pb2.DrainAndKillNodeRequest(
        node_ids=[binascii.unhexlify(worker_node_id)]
    )
    response = monitor_stub.DrainAndKillNode(request)

    assert response.drained_nodes == request.node_ids
    wait_for_condition(lambda: count_live_nodes() == 1)

    response = monitor_stub.DrainAndKillNode(request)
    assert response.drained_nodes == request.node_ids
    wait_for_condition(lambda: count_live_nodes() == 1)


def test_ray_version(monitor_stub):
    request = monitor_pb2.GetRayVersionRequest()
    response = monitor_stub.GetRayVersion(request)
    assert response.version == ray.__version__


def test_scheduling_status_actors(monitor_stub):
    @ray.remote(num_cpus=0, num_gpus=1)
    class Foo:
        pass

    @ray.remote(num_cpus=1)
    class Bar:
        def ready(self):
            pass

    gpu_actors = [Foo.remote() for _ in range(2)]
    cpu_actors = [Bar.remote() for _ in range(3)]

    refs = [actor.ready.remote() for actor in cpu_actors]
    print("Waiting for an actor to be ready...", refs)
    # Wait for one actor to get placed (and therefore take the cpu).
    ray.wait([actor.ready.remote() for actor in cpu_actors])
    print("done")

    def condition():
        request = monitor_pb2.GetSchedulingStatusRequest()
        response = monitor_stub.GetSchedulingStatus(request)

        assert len(response.resource_requests) == 2

        shapes = [{"CPU": 1}, {"GPU": 1}]
        for request in response.resource_requests:
            if (
                request.resource_request_type
                != monitor_pb2.ResourceRequest.TASK_RESERVATION
            ):
                return False
            if request.count != 2:
                return False
            if len(request.bundles) != 1:
                return False
            if request.bundles[0].resources not in shapes:
                return False

        return True

    wait_for_condition(condition)

    del gpu_actors


def test_scheduling_status_pgs(monitor_stub):
    pg = ray.util.placement_group(
        [{"CPU": 0.1, "GPU": 1}, {"custom": 10}], strategy="STRICT_PACK"
    )

    def condition():
        request = monitor_pb2.GetSchedulingStatusRequest()
        response = monitor_stub.GetSchedulingStatus(request)

        assert len(response.resource_requests) == 1

        shapes = [{"CPU": 0.1, "GPU": 1}, {"custom": 10}]
        for bundle in response.resource_requests[0].bundles:
            if bundle.resources not in shapes:
                return False

        return True

    wait_for_condition(condition)
    del pg


def count_live_nodes():
    s = sum(1 for node in ray.nodes() if node["Alive"])
    print(ray.nodes())
    return s
