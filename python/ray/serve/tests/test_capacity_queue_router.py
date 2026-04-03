import sys
import uuid
from collections import Counter

import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.constants import SERVE_DEPLOYMENT_ACTOR_PREFIX, SERVE_NAMESPACE
from ray.serve._private.experimental.capacity_queue_request_router import (
    CapacityQueue,
)
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
            request_router_class=(
                "ray.serve._private.experimental.capacity_queue_request_router:CapacityQueueRouter"
            ),
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


def _deploy_blocking_capacity_queue_app(
    signal_actor_name: str,
    num_replicas: int = 2,
    max_ongoing_requests: int = 5,
):
    """Deploy an app whose requests block until a SignalActor is triggered."""

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="capacity_queue",
                actor_class=CapacityQueue,
                init_kwargs={
                    "acquire_timeout_s": 30.0,
                    "deployment_id_name": "BlockingApp",
                    "deployment_id_app": "default",
                },
                actor_options={"num_cpus": 0},
            ),
        ],
        request_router_config=RequestRouterConfig(
            request_router_class=(
                "ray.serve._private.experimental.capacity_queue_request_router:CapacityQueueRouter"
            ),
        ),
        num_replicas=num_replicas,
        max_ongoing_requests=max_ongoing_requests,
        ray_actor_options={"num_cpus": 0},
    )
    class BlockingApp:
        def __init__(self):
            from ray.serve.context import _get_internal_replica_context

            context = _get_internal_replica_context()
            self.unique_id = context.replica_id.unique_id

        async def __call__(self):
            signal = ray.get_actor(signal_actor_name)
            await signal.wait.remote()
            return self.unique_id

    handle = serve.run(BlockingApp.bind())
    return handle


def _find_capacity_queue_handle():
    """Find the CapacityQueue deployment actor."""
    actors = ray.util.list_named_actors(all_namespaces=True)
    for actor_info in actors:
        if (
            actor_info["namespace"] == SERVE_NAMESPACE
            and "capacity_queue" in actor_info["name"]
            and SERVE_DEPLOYMENT_ACTOR_PREFIX in actor_info["name"]
        ):
            return ray.get_actor(actor_info["name"], namespace=SERVE_NAMESPACE)
    return None


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

        queue_handle = _find_capacity_queue_handle()
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


class TestCapacityQueueRouterFailures:
    def test_unreleased_token_reduces_capacity(self, serve_instance):
        """Tokens acquired but never released permanently reduce capacity.

        When a token is acquired from the queue but never released (e.g. a
        router process dies between acquire() and release()), the queue's
        in_flight count stays elevated, reducing effective capacity until the
        replica is recycled.
        """
        handle = _deploy_capacity_queue_app(num_replicas=1, max_ongoing_requests=3)
        wait_for_condition(check_running, timeout=30)

        queue = _find_capacity_queue_handle()
        wait_for_condition(
            lambda: ray.get(queue.get_stats.remote()).num_replicas == 1,
            timeout=15,
        )

        # Simulate a router acquiring a token then crashing (never releases).
        leaked = ray.get(queue.acquire.remote(timeout_s=5))
        assert leaked is not None

        stats = ray.get(queue.get_stats.remote())
        assert stats.total_in_flight == 1
        assert stats.queue_size == 2  # 3 capacity - 1 leaked

        # Remaining capacity still serves requests.
        resp = handle.remote().result(timeout_s=10)
        assert isinstance(resp, str)

        # Leaked token still occupies capacity.
        wait_for_condition(
            lambda: ray.get(queue.get_stats.remote()).total_in_flight >= 1,
            timeout=10,
        )

    def test_replica_death_releases_token_and_recovers(self, serve_instance):
        """When a replica dies mid-request, its token is released and
        the queue stops routing to it after the long-poll update."""

        @serve.deployment(
            deployment_actors=[
                DeploymentActorConfig(
                    name="capacity_queue",
                    actor_class=CapacityQueue,
                    init_kwargs={
                        "acquire_timeout_s": 30.0,
                        "deployment_id_name": "CrashApp",
                        "deployment_id_app": "default",
                    },
                    actor_options={"num_cpus": 0},
                ),
            ],
            request_router_config=RequestRouterConfig(
                request_router_class=(
                    "ray.serve._private.experimental.capacity_queue_request_router:CapacityQueueRouter"
                ),
            ),
            num_replicas=2,
            max_ongoing_requests=2,
            ray_actor_options={"num_cpus": 0},
        )
        class CrashApp:
            def __init__(self):
                from ray.serve.context import _get_internal_replica_context

                context = _get_internal_replica_context()
                self.unique_id = context.replica_id.unique_id

            async def __call__(self, crash: bool = False):
                if crash:
                    import os

                    os._exit(1)
                return self.unique_id

        handle = serve.run(CrashApp.bind())
        wait_for_condition(check_running, timeout=30)

        queue = _find_capacity_queue_handle()
        wait_for_condition(
            lambda: ray.get(queue.get_stats.remote()).num_replicas == 2,
            timeout=15,
        )

        # Crash one replica by sending a request that exits the process.
        try:
            handle.remote(crash=True).result(timeout_s=5)
        except Exception:
            pass  # Expected — the replica died.

        # The controller detects the death, removes the replica, and starts
        # a replacement. Long poll updates the queue. Eventually the queue
        # should recover to 2 replicas (the survivor + the replacement).
        wait_for_condition(
            lambda: ray.get(queue.get_stats.remote()).num_replicas == 2,
            timeout=30,
        )

        # Requests still succeed — routed to the surviving / replacement replica.
        resp = handle.remote().result(timeout_s=15)
        assert isinstance(resp, str)

    def test_capacity_queue_death_and_recovery(self, serve_instance):
        """When the CapacityQueue actor dies, the controller recreates it
        and request routing recovers."""
        handle = _deploy_capacity_queue_app(num_replicas=2)
        wait_for_condition(check_running, timeout=30)

        queue = _find_capacity_queue_handle()
        wait_for_condition(
            lambda: ray.get(queue.get_stats.remote()).num_replicas == 2,
            timeout=15,
        )

        # Verify requests work before the kill.
        resp = handle.remote().result(timeout_s=10)
        assert isinstance(resp, str)

        # Kill the capacity queue actor.
        ray.kill(queue)

        # The controller recreates the deployment actor. The new queue starts
        # fresh and gets replicas via long poll. Wait for it to appear.
        def _queue_recovered():
            new_q = _find_capacity_queue_handle()
            if new_q is None:
                return False
            stats = ray.get(new_q.get_stats.remote())
            return stats.num_replicas == 2

        wait_for_condition(_queue_recovered, timeout=30)

        # Requests should work again (router rediscovers the new queue).
        # May need a few retries while the router reconnects.
        def _request_succeeds():
            handle.remote().result(timeout_s=10)
            return True

        wait_for_condition(_request_succeeds, timeout=30)

    def test_capacity_queue_restarts_with_full_capacity(self, serve_instance):
        """After a queue restart, it bootstraps with full capacity even though
        replicas may have in-flight requests from before the crash.

        This documents a known trade-off: the restarted queue does not know
        about pre-crash in-flight counts, so it may temporarily over-provision
        capacity beyond max_ongoing_requests.
        """
        signal_name = f"block_signal_{uuid.uuid4().hex[:8]}"
        signal = SignalActor.options(name=signal_name).remote()

        handle = _deploy_blocking_capacity_queue_app(
            signal_actor_name=signal_name,
            num_replicas=1,
            max_ongoing_requests=2,
        )
        wait_for_condition(check_running, timeout=30)

        queue = _find_capacity_queue_handle()
        wait_for_condition(
            lambda: ray.get(queue.get_stats.remote()).num_replicas == 1,
            timeout=15,
        )

        # Send a blocking request — occupies 1 of 2 slots on the replica.
        ref = handle.remote()
        wait_for_condition(
            lambda: ray.get(queue.get_stats.remote()).total_in_flight == 1,
            timeout=10,
        )

        # Kill the capacity queue.
        ray.kill(queue)

        # Wait for the controller to recreate it.
        def _new_queue_ready():
            q = _find_capacity_queue_handle()
            if q is None:
                return False
            stats = ray.get(q.get_stats.remote())
            return stats.num_replicas == 1

        wait_for_condition(_new_queue_ready, timeout=30)

        # The new queue shows full capacity (2) even though the replica still
        # has 1 in-flight request from before the crash.
        new_queue = _find_capacity_queue_handle()
        stats = ray.get(new_queue.get_stats.remote())
        assert stats.total_capacity == 2
        assert stats.total_in_flight == 0  # Queue doesn't know about the old request

        # Release the signal so the blocked request finishes.
        ray.get(signal.send.remote())
        try:
            ref.result(timeout_s=10)
        except Exception:
            pass  # May fail since the queue died mid-request

        # Cleanup
        ray.kill(signal)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v", "-s"]))
