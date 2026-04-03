import sys
from collections import Counter

import pytest
from capacity_queue_request_router import CapacityQueue

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.test_utils import check_running
from ray.serve.config import DeploymentActorConfig, RequestRouterConfig


def _deploy_capacity_queue_app(
    num_replicas: int = 3,
    max_ongoing_requests: int = 5,
    acquire_timeout_s: float = 30.0,
):
    """Deploy a simple app with CapacityQueue deployment actor and CapacityQueueRouter."""

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="capacity_queue",
                actor_class=CapacityQueue,
                init_kwargs={
                    "acquire_timeout_s": acquire_timeout_s,
                    "deployment_id_name": "App",
                    "deployment_id_app": "default",
                },
                actor_options={"num_cpus": 0},
            ),
        ],
        request_router_config=RequestRouterConfig(
            request_router_class=("capacity_queue_request_router:CapacityQueueRouter"),
        ),
        num_replicas=num_replicas,
        max_ongoing_requests=max_ongoing_requests,
        ray_actor_options={"num_cpus": 0},
    )
    class App:
        def __init__(self):
            from ray.serve.context import _get_internal_replica_context

            context = _get_internal_replica_context()
            self.replica_id = context.replica_id
            self.unique_id = context.replica_id.unique_id

        async def __call__(self):
            return self.unique_id

    handle = serve.run(App.bind())
    return handle


class TestCapacityQueueRouterBasic:
    """Basic integration tests for the capacity queue router."""

    def test_single_request(self, serve_instance):
        """A single request should be routed to one of the replicas."""
        handle = _deploy_capacity_queue_app(num_replicas=2)

        # Wait for deployment to be healthy
        wait_for_condition(check_running, timeout=30)

        response = handle.remote().result(timeout_s=10)
        assert isinstance(response, str)
        assert len(response) > 0

    def test_multiple_requests_distributed(self, serve_instance):
        """Requests should be distributed across replicas."""
        num_replicas = 3
        handle = _deploy_capacity_queue_app(num_replicas=num_replicas)

        wait_for_condition(check_running, timeout=30)

        # Send enough requests that all replicas should receive at least one
        num_requests = 30
        responses = []
        for _ in range(num_requests):
            r = handle.remote().result(timeout_s=10)
            responses.append(r)

        unique_replicas = set(responses)
        # All replicas should have received at least one request
        assert len(unique_replicas) == num_replicas, (
            f"Expected {num_replicas} unique replicas, got {len(unique_replicas)}: "
            f"{unique_replicas}"
        )

    def test_concurrent_requests(self, serve_instance):
        """Concurrent requests should be distributed across replicas."""
        num_replicas = 3
        handle = _deploy_capacity_queue_app(num_replicas=num_replicas)

        wait_for_condition(check_running, timeout=30)

        # Send concurrent requests
        refs = [handle.remote() for _ in range(30)]
        responses = [ref.result(timeout_s=10) for ref in refs]

        unique_replicas = set(responses)
        assert len(unique_replicas) == num_replicas

    def test_capacity_queue_stats(self, serve_instance):
        """The capacity queue should track stats correctly."""
        handle = _deploy_capacity_queue_app(num_replicas=2)

        wait_for_condition(check_running, timeout=30)

        # Send some requests
        for _ in range(10):
            handle.remote().result(timeout_s=10)

        # Check capacity queue stats via deployment actor
        # We need to find the capacity queue actor
        from ray.serve._private.constants import (
            SERVE_DEPLOYMENT_ACTOR_PREFIX,
            SERVE_NAMESPACE,
        )

        actors = ray.util.list_named_actors(all_namespaces=True)
        queue_handle = None
        for actor_info in actors:
            if (
                actor_info["namespace"] == SERVE_NAMESPACE
                and "capacity_queue" in actor_info["name"]
                and SERVE_DEPLOYMENT_ACTOR_PREFIX in actor_info["name"]
            ):
                queue_handle = ray.get_actor(
                    actor_info["name"], namespace=SERVE_NAMESPACE
                )
                break

        assert queue_handle is not None, "CapacityQueue deployment actor not found"

        # Wait for all releases to settle (on_request_completed is async)
        def _stats_settled():
            stats = ray.get(queue_handle.get_stats.remote())
            assert stats.total_acquires >= 10
            assert stats.total_releases >= 10
            assert stats.num_replicas == 2
            assert stats.total_in_flight == 0
            return True

        wait_for_condition(_stats_settled, timeout=10)


class TestCapacityQueueRouterLoadBalancing:
    """Tests for load balancing behavior."""

    def test_least_loaded_balancing(self, serve_instance):
        """Requests should be balanced across replicas (least-loaded)."""
        num_replicas = 3
        handle = _deploy_capacity_queue_app(num_replicas=num_replicas)

        wait_for_condition(check_running, timeout=30)

        # Send sequential requests - should round-robin approximately
        num_requests = 60
        responses = []
        for _ in range(num_requests):
            r = handle.remote().result(timeout_s=10)
            responses.append(r)

        counter = Counter(responses)
        # Each replica should get roughly equal share
        expected_per_replica = num_requests / num_replicas
        for replica_id, count in counter.items():
            assert (
                count >= expected_per_replica * 0.3
            ), f"Replica {replica_id} got {count} requests, expected ~{expected_per_replica}"


class TestCapacityQueueRouterWithSingleReplica:
    """Tests with a single replica to verify basic token flow."""

    def test_single_replica_all_requests(self, serve_instance):
        """With one replica, all requests should go to the same replica."""
        handle = _deploy_capacity_queue_app(num_replicas=1)

        wait_for_condition(check_running, timeout=30)

        responses = set()
        for _ in range(10):
            r = handle.remote().result(timeout_s=10)
            responses.add(r)

        assert len(responses) == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v", "-s"]))
