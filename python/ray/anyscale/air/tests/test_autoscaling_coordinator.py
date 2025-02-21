import unittest
from unittest.mock import patch

import pytest

import ray
from ray.anyscale.air._internal.autoscaling_coordinator import (
    AutoscalingCoordinator,
    get_or_create_autoscaling_coordinator,
)
from ray.cluster_utils import Cluster
from ray.tests.conftest import wait_for_condition

MOCKED_TIME = 0


def mock_time():
    global MOCKED_TIME

    return MOCKED_TIME


class TestAutoscalingCoordinator(unittest.TestCase):

    CLUSTER_NODES = [
        {
            "Resources": {"CPU": 10, "GPU": 5, "object_store_memory": 1000},
            "Alive": True,
        },
    ]

    @patch("time.time", mock_time)
    @patch("ray.autoscaler.sdk.request_resources")
    @patch("ray.nodes", return_value=CLUSTER_NODES)
    def test_basic(self, mock_nodes, mock_request_resources):
        global MOCKED_TIME

        MOCKED_TIME = 0
        req1 = [{"CPU": 3, "GPU": 1, "object_store_memory": 100}]
        req1_timeout = 2
        as_coordinator = AutoscalingCoordinator()
        mock_nodes.assert_called_once()
        as_coordinator.request_resources(
            requester_id="requester1",
            resources=req1,
            expire_after_s=req1_timeout,
        )
        mock_request_resources.assert_called_once_with(bundles=req1)
        res1 = as_coordinator.get_allocated_resources("requester1")
        assert res1 == req1

        # Send the same request again. `mock_request_resources` won't be called
        # since the request is not updated.
        as_coordinator.request_resources(
            requester_id="requester1",
            resources=req1,
            expire_after_s=req1_timeout,
        )
        assert mock_request_resources.call_count == 1

        # Send a request from requester2, with request_remaining=True.
        # requester2 should get the requested + the remaining resources.
        req2 = [{"CPU": 2, "GPU": 1, "object_store_memory": 100}]
        req2_timeout = 20
        as_coordinator.request_resources(
            requester_id="requester2",
            resources=req2,
            expire_after_s=req2_timeout,
            request_remaining=True,
        )
        mock_request_resources.assert_called_with(bundles=req1 + req2)
        res2 = as_coordinator.get_allocated_resources("requester2")
        assert res2 == req2 + [{"CPU": 5, "GPU": 3, "object_store_memory": 800}]

        # Test updating req1
        req1_updated = [{"CPU": 4, "GPU": 2, "object_store_memory": 300}]
        as_coordinator.request_resources(
            requester_id="requester1",
            resources=req1_updated,
            expire_after_s=req1_timeout,
        )
        mock_request_resources.assert_called_with(bundles=req1_updated + req2)
        res1 = as_coordinator.get_allocated_resources("requester1")
        assert res1 == req1_updated
        res2 = as_coordinator.get_allocated_resources("requester2")
        assert res2 == req2 + [{"CPU": 4, "GPU": 2, "object_store_memory": 600}]

        # After req1_timeout, req1 should be expired.
        MOCKED_TIME = req1_timeout + 0.1
        as_coordinator.tick()
        mock_request_resources.assert_called_with(bundles=req2)
        res1 = as_coordinator.get_allocated_resources("requester1")
        res2 = as_coordinator.get_allocated_resources("requester2")
        assert res1 == []
        assert res2 == req2 + [{"CPU": 8, "GPU": 4, "object_store_memory": 900}]

        # After req2_timeout, req2 should be expired.
        MOCKED_TIME = req2_timeout + 0.1
        as_coordinator.tick()
        mock_request_resources.assert_called_with(bundles=[])
        res1 = as_coordinator.get_allocated_resources("requester1")
        res2 = as_coordinator.get_allocated_resources("requester2")
        assert res1 == []
        assert res2 == []

        # Test canceling a request
        as_coordinator.cancel_request("requester2")
        res2 = as_coordinator.get_allocated_resources("requester2")
        assert res2 == []


@pytest.fixture
def cluster():
    """Initialize a Ray cluster with a 0 CPU head node and no workers."""
    cluster = Cluster()
    cluster.add_node(num_cpus=0)
    cluster.wait_for_nodes()
    cluster.connect()
    yield cluster
    ray.shutdown()
    cluster.shutdown()


@pytest.mark.parametrize("gpu_tasks_include_cpu", [True, False])
def test_autoscaling_coordinator_e2e(cluster, gpu_tasks_include_cpu):
    """Integration test for AutoscalingCoordinator.

    This test creates 2 dummy components that request resources from
    AutoscalingCoordinator, and checks allocated resources are correct.
    """
    object_store_memory = 100 * 1024**2
    num_cpu_nodes = 4
    cpu_node_spec = {"num_cpus": 8, "object_store_memory": object_store_memory}
    num_gpu_nodes = 2
    gpu_node_spec = {
        "num_cpus": 4,
        "num_gpus": 1,
        "object_store_memory": object_store_memory,
    }

    for _ in range(num_cpu_nodes):
        cluster.add_node(**cpu_node_spec)
    for _ in range(num_gpu_nodes):
        cluster.add_node(**gpu_node_spec)

    cluster.wait_for_nodes()

    @ray.remote
    def request_and_check_resources(
        requester_id, resources, expected, request_remaining
    ):
        as_coordinator = get_or_create_autoscaling_coordinator()
        ray.get(
            as_coordinator.request_resources.remote(
                requester_id=requester_id,
                resources=resources,
                expire_after_s=100,
                request_remaining=request_remaining,
            )
        )

        def check_allocated_resources():
            allocated = ray.get(
                as_coordinator.get_allocated_resources.remote(requester_id)
            )
            allocated = [
                {
                    k: int(v)
                    for k, v in r.items()
                    if k in ["CPU", "GPU", "object_store_memory"] and v != 0
                }
                for r in allocated
                if "node:__internal_head__" not in r
            ]
            allocated = [r for r in allocated if len(r) > 0]
            if allocated != expected:
                print(
                    f"{requester_id}: Allocated resources: {allocated}, "
                    f"expected: {expected}. Retrying."
                )
                return False
            else:
                return True

        wait_for_condition(
            check_allocated_resources,
            retry_interval_ms=1000,
            timeout=5,
        )
        return "ok"

    res1_resources = [
        {
            "CPU": cpu_node_spec["num_cpus"],
            "object_store_memory": object_store_memory,
        }
    ] * num_cpu_nodes
    req2_resources = [
        {
            "GPU": gpu_node_spec["num_gpus"],
        }
    ] * num_gpu_nodes
    if gpu_tasks_include_cpu:
        for r in req2_resources:
            r["CPU"] = 1
    remaining = [
        {
            "CPU": gpu_node_spec["num_cpus"] - (1 if gpu_tasks_include_cpu else 0),
            "object_store_memory": object_store_memory,
        }
    ] * num_gpu_nodes

    res1 = request_and_check_resources.remote(
        requester_id="requester1",
        resources=res1_resources,
        expected=res1_resources + remaining,
        request_remaining=True,
    )
    res2 = request_and_check_resources.remote(
        requester_id="requester2",
        resources=req2_resources,
        expected=req2_resources,
        request_remaining=False,
    )

    assert ray.get([res1, res2]) == ["ok"] * 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
