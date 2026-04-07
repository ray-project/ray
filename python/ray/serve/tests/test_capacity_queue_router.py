import sys
import uuid
from collections import Counter

import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.constants import SERVE_DEPLOYMENT_ACTOR_PREFIX, SERVE_NAMESPACE
from ray.serve._private.test_utils import check_running
from ray.serve.config import DeploymentActorConfig, RequestRouterConfig
from ray.serve.experimental.capacity_queue import (
    CapacityQueue,
)


def _deploy_capacity_queue_app(
    num_replicas: int = 3,
    max_ongoing_requests: int = 5,
    acquire_timeout_s: float = 0.5,
    token_ttl_s: float = 5,
):
    """Deploy a simple app with CapacityQueue deployment actor and CapacityQueueRouter."""

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="capacity_queue",
                actor_class=CapacityQueue,
                init_kwargs={
                    "acquire_timeout_s": acquire_timeout_s,
                    "token_ttl_s": token_ttl_s,
                    "deployment_id_name": "App",
                    "deployment_id_app": "default",
                },
                actor_options={"num_cpus": 0},
            ),
        ],
        request_router_config=RequestRouterConfig(
            request_router_class=(
                "ray.serve.experimental.capacity_queue_router:CapacityQueueRouter"
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
                    "acquire_timeout_s": 0.5,
                    "token_ttl_s": 5,
                    "deployment_id_name": "BlockingApp",
                    "deployment_id_app": "default",
                },
                actor_options={"num_cpus": 0},
            ),
        ],
        request_router_config=RequestRouterConfig(
            request_router_class=(
                "ray.serve.experimental.capacity_queue_router:CapacityQueueRouter"
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
        responses = [ref.result(timeout_s=30) for ref in refs]

        unique_replicas = set(responses)
        assert len(unique_replicas) == num_replicas

    def test_capacity_queue_stats(self, serve_instance):
        """The capacity queue should track stats correctly.

        Some early requests may fall back to power-of-two-choices before the
        router discovers the queue, so we assert >= rather than exact counts.
        """
        handle = _deploy_capacity_queue_app(num_replicas=2)

        wait_for_condition(check_running, timeout=30)

        queue_handle = _find_capacity_queue_handle()
        assert queue_handle is not None, "CapacityQueue deployment actor not found"

        # Wait for queue to have replicas before sending requests so most
        # go through the queue path (not power-of-two-choices fallback).
        wait_for_condition(
            lambda: ray.get(queue_handle.get_stats.remote()).num_replicas == 2,
            timeout=15,
        )

        # Send some requests
        for _ in range(10):
            handle.remote().result(timeout_s=10)

        # Wait for all releases to settle (on_request_completed is async)
        def _stats_settled():
            stats = ray.get(queue_handle.get_stats.remote())
            assert stats.num_replicas == 2
            assert stats.total_in_flight == 0
            # Most requests should go through the queue. Some may fall back
            # to power-of-two-choices, so use >= with a lower bound.
            assert stats.total_acquires >= 5
            assert stats.total_releases >= 5
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


class TestCapacityQueueRouterPowerOfTwoFallback:
    """Tests that the router falls back to power-of-two-choices when the
    queue is unavailable."""

    def test_requests_succeed_without_queue(self, serve_instance):
        """Requests succeed via power-of-two-choices even when the queue is
        killed immediately."""
        handle = _deploy_capacity_queue_app(num_replicas=2)
        wait_for_condition(check_running, timeout=30)

        queue = _find_capacity_queue_handle()
        wait_for_condition(
            lambda: ray.get(queue.get_stats.remote()).num_replicas == 2,
            timeout=15,
        )

        # Kill the queue so all subsequent requests must use fallback.
        ray.kill(queue)

        for _ in range(5):
            resp = handle.remote().result(timeout_s=15)
            assert isinstance(resp, str)

    def test_requests_distributed_without_queue(self, serve_instance):
        """In fallback mode, requests are still distributed across replicas."""
        num_replicas = 3
        handle = _deploy_capacity_queue_app(num_replicas=num_replicas)
        wait_for_condition(check_running, timeout=30)

        queue = _find_capacity_queue_handle()
        wait_for_condition(
            lambda: ray.get(queue.get_stats.remote()).num_replicas == num_replicas,
            timeout=15,
        )

        # Kill the queue.
        ray.kill(queue)

        responses = []
        for _ in range(30):
            r = handle.remote().result(timeout_s=15)
            responses.append(r)

        unique_replicas = set(responses)
        assert len(unique_replicas) == num_replicas


class TestCapacityQueueRouterFailures:
    def test_unreleased_token_recovered_by_ttl(self, serve_instance):
        """Leaked tokens are automatically reclaimed after the TTL expires.

        When a token is acquired but never released (e.g. a router process
        dies between acquire() and release()), the queue's in_flight count
        stays elevated. With token_ttl_s configured, a background reaper
        reclaims expired tokens and restores full capacity.
        """
        token_ttl_s = 2.0

        @serve.deployment(
            deployment_actors=[
                DeploymentActorConfig(
                    name="capacity_queue",
                    actor_class=CapacityQueue,
                    init_kwargs={
                        "acquire_timeout_s": 0.5,
                        "token_ttl_s": token_ttl_s,
                        "deployment_id_name": "TtlApp",
                        "deployment_id_app": "default",
                    },
                    actor_options={"num_cpus": 0},
                ),
            ],
            request_router_config=RequestRouterConfig(
                request_router_class=(
                    "ray.serve.experimental.capacity_queue_router:CapacityQueueRouter"
                ),
            ),
            num_replicas=1,
            max_ongoing_requests=3,
            ray_actor_options={"num_cpus": 0},
        )
        class TtlApp:
            def __init__(self):
                from ray.serve.context import _get_internal_replica_context

                context = _get_internal_replica_context()
                self.unique_id = context.replica_id.unique_id

            async def __call__(self):
                return self.unique_id

        handle = serve.run(TtlApp.bind())
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

        # After the TTL expires, the reaper reclaims the leaked token and
        # full capacity is restored.
        def _capacity_restored():
            s = ray.get(queue.get_stats.remote())
            return s.total_in_flight == 0 and s.queue_size == 3

        wait_for_condition(_capacity_restored, timeout=token_ttl_s + 5)

    def test_replica_death_releases_token_and_recovers(self, serve_instance):
        """When a replica dies mid-request, its token is released and
        the queue stops routing to it after the long-poll update."""

        @serve.deployment(
            deployment_actors=[
                DeploymentActorConfig(
                    name="capacity_queue",
                    actor_class=CapacityQueue,
                    init_kwargs={
                        "acquire_timeout_s": 0.5,
                        "token_ttl_s": 5,
                        "deployment_id_name": "CrashApp",
                        "deployment_id_app": "default",
                    },
                    actor_options={"num_cpus": 0},
                ),
            ],
            request_router_config=RequestRouterConfig(
                request_router_class=(
                    "ray.serve.experimental.capacity_queue_router:CapacityQueueRouter"
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
        # should recover to 2 replicas (the survivor + the replacement)
        # with full capacity and no leaked in-flight counts.
        def _cluster_fully_recovered():
            stats = ray.get(queue.get_stats.remote())
            assert stats.num_replicas == 2
            assert stats.total_capacity == 4  # 2 replicas * max_ongoing_requests=2
            assert stats.total_in_flight == 0
            return True

        wait_for_condition(_cluster_fully_recovered, timeout=30)

        # Requests still succeed — routed to the surviving / replacement replica.
        resp = handle.remote().result(timeout_s=15)
        assert isinstance(resp, str)

    def test_capacity_queue_death_and_recovery(self, serve_instance):
        """When the CapacityQueue actor dies, the router falls back to
        power-of-two-choices and requests continue to succeed. Once the
        controller recreates the queue, the router rediscovers it and
        resumes token-based routing.
        """
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

        # Requests should STILL succeed via power-of-two-choices fallback
        # even while the queue is dead.
        resp = handle.remote().result(timeout_s=15)
        assert isinstance(resp, str)

        # The controller recreates the deployment actor. The new queue starts
        # fresh and gets replicas via long poll. Wait for it to appear.
        def _queue_recovered():
            new_q = _find_capacity_queue_handle()
            if new_q is None:
                return False
            stats = ray.get(new_q.get_stats.remote())
            return stats.num_replicas == 2

        wait_for_condition(_queue_recovered, timeout=30)

        # After recovery, requests go through the queue again. Verify the
        # new queue is being used by checking that acquires increase.
        new_queue = _find_capacity_queue_handle()
        stats_before = ray.get(new_queue.get_stats.remote())
        for _ in range(3):
            handle.remote().result(timeout_s=10)

        def _queue_used():
            stats = ray.get(new_queue.get_stats.remote())
            return stats.total_acquires > stats_before.total_acquires

        wait_for_condition(_queue_used, timeout=10)

    def test_capacity_queue_restarts_with_full_capacity(self, serve_instance):
        """
        After a queue restart, it bootstraps with full capacity even though
        replicas may have in-flight requests from before the crash.
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

    def test_queue_converges_after_restart(self, serve_instance):
        """After the queue restarts, its per-replica token view converges to
        match actual replica capacity.

        Setup: 1 replica, max_ongoing_requests=5, 3 blocked requests.
        1. Send 3 blocking requests occupying 3/5 slots. Queue correctly
           shows in_flight=3, available_tokens=2 for the replica.
        2. Kill the queue — it restarts with in_flight=0, thinking the
           replica has 5 available tokens (stale).
        3. The router sends requests via the stale queue. Tokens for the
           3 occupied slots get rejected. Unreleased rejection tokens
           ratchet in_flight up, teaching the queue the correct state.
        4. Release the blocking requests — replica frees all 5 slots.
        5. TTL reaper clears phantom in_flight entries from rejections.
        6. Assert per-replica convergence: available_tokens == max_capacity.
        """
        token_ttl_s = 2.0
        max_ongoing = 5
        num_blocked = 3

        signal_name = f"block_signal_{uuid.uuid4().hex[:8]}"
        signal = SignalActor.options(name=signal_name).remote()

        @serve.deployment(
            deployment_actors=[
                DeploymentActorConfig(
                    name="capacity_queue",
                    actor_class=CapacityQueue,
                    init_kwargs={
                        "acquire_timeout_s": 0.5,
                        "token_ttl_s": token_ttl_s,
                        "deployment_id_name": "ConvergeApp",
                        "deployment_id_app": "default",
                    },
                    actor_options={"num_cpus": 0},
                ),
            ],
            request_router_config=RequestRouterConfig(
                request_router_class=(
                    "ray.serve.experimental.capacity_queue_router:CapacityQueueRouter"
                ),
            ),
            num_replicas=1,
            max_ongoing_requests=max_ongoing,
            ray_actor_options={"num_cpus": 0},
        )
        class ConvergeApp:
            def __init__(self):
                from ray.serve.context import _get_internal_replica_context

                context = _get_internal_replica_context()
                self.unique_id = context.replica_id.unique_id

            async def __call__(self, block: bool = False):
                if block:
                    sig = ray.get_actor(signal_name)
                    await sig.wait.remote()
                return self.unique_id

        handle = serve.run(ConvergeApp.bind())
        wait_for_condition(check_running, timeout=30)

        queue = _find_capacity_queue_handle()
        wait_for_condition(
            lambda: ray.get(queue.get_stats.remote()).num_replicas == 1,
            timeout=15,
        )

        # Step 1: Occupy 3 of 5 slots with blocking requests.
        blocking_refs = [handle.remote(block=True) for _ in range(num_blocked)]
        wait_for_condition(
            lambda: ray.get(queue.get_stats.remote()).total_in_flight == num_blocked,
            timeout=15,
        )

        # Verify pre-crash per-replica state: in_flight=3, capacity=5.
        replica_info = ray.get(queue.get_replica_in_flight.remote())
        assert len(replica_info) == 1
        for rid, (in_flight, max_cap) in replica_info.items():
            assert max_cap == max_ongoing
            assert in_flight == num_blocked
            assert max_cap - in_flight == 2  # 2 available tokens

        # Step 2: Kill the queue. It restarts with in_flight=0.
        ray.kill(queue)

        def _new_queue_ready():
            q = _find_capacity_queue_handle()
            if q is None:
                return False
            stats = ray.get(q.get_stats.remote())
            return stats.num_replicas == 1

        wait_for_condition(_new_queue_ready, timeout=30)

        new_queue = _find_capacity_queue_handle()

        # Verify stale state: queue thinks replica has 5 available tokens.
        stale_info = ray.get(new_queue.get_replica_in_flight.remote())
        for rid, (in_flight, max_cap) in stale_info.items():
            assert in_flight == 0
            assert max_cap == max_ongoing

        # Step 3 & 4: Release the blocking requests so the replica frees up.
        ray.get(signal.send.remote())
        for ref in blocking_refs:
            try:
                ref.result(timeout_s=15)
            except Exception:
                pass  # May fail — queue died while these were in flight.

        # Send requests to exercise the queue and trigger any rejection-based
        # learning for the stale window.
        for _ in range(5):
            handle.remote().result(timeout_s=15)

        # Step 5 & 6: Wait for TTL reaper, then verify per-replica convergence.
        # available_tokens (max_capacity - in_flight) must equal max_capacity
        # because the replica has 0 real in-flight after the signal release.
        def _per_replica_converged():
            info = ray.get(new_queue.get_replica_in_flight.remote())
            if len(info) != 1:
                return False
            for in_flight, max_cap in info.values():
                if max_cap - in_flight != max_ongoing:
                    return False
            return True

        wait_for_condition(_per_replica_converged, timeout=token_ttl_s + 10)

        # Final assertion: in_flight is exactly 0, all 5 tokens available.
        final_info = ray.get(new_queue.get_replica_in_flight.remote())
        for rid, (in_flight, max_cap) in final_info.items():
            assert (
                in_flight == 0
            ), f"Replica {rid}: expected converged in_flight=0, got {in_flight}"
            assert max_cap - in_flight == max_ongoing

        ray.kill(signal)

    def test_capacity_depleted_backoff_and_recovery(self, serve_instance):
        """
        When all replicas are at capacity, the router backs off and
        retries until capacity frees up.
        """
        signal_name = f"block_signal_{uuid.uuid4().hex[:8]}"
        signal = SignalActor.options(name=signal_name).remote()

        @serve.deployment(
            deployment_actors=[
                DeploymentActorConfig(
                    name="capacity_queue",
                    actor_class=CapacityQueue,
                    init_kwargs={
                        "acquire_timeout_s": 0.5,
                        "token_ttl_s": 5,
                        "deployment_id_name": "DepletedApp",
                        "deployment_id_app": "default",
                    },
                    actor_options={"num_cpus": 0},
                ),
            ],
            request_router_config=RequestRouterConfig(
                request_router_class=(
                    "ray.serve.experimental.capacity_queue_router:CapacityQueueRouter"
                ),
            ),
            num_replicas=1,
            max_ongoing_requests=2,
            ray_actor_options={"num_cpus": 0},
        )
        class DepletedApp:
            def __init__(self):
                from ray.serve.context import _get_internal_replica_context

                context = _get_internal_replica_context()
                self.unique_id = context.replica_id.unique_id

            async def __call__(self, block: bool = False):
                if block:
                    sig = ray.get_actor(signal_name)
                    await sig.wait.remote()
                return self.unique_id

        handle = serve.run(DepletedApp.bind())
        wait_for_condition(check_running, timeout=30)

        queue = _find_capacity_queue_handle()
        wait_for_condition(
            lambda: ray.get(queue.get_stats.remote()).num_replicas == 1,
            timeout=10,
        )

        # Fill both slots with blocking requests.
        blocking_refs = [handle.remote(block=True) for _ in range(2)]
        wait_for_condition(
            lambda: ray.get(queue.get_stats.remote()).total_in_flight == 2,
            timeout=10,
        )

        # Send a third request — will be blocked waiting for capacity.
        waiting_ref = handle.remote(block=False)

        # Wait for at least one CQ timeout — proves the router hit the
        # depleted path and backed off (total_timeouts is 0 before this
        # since the blocking requests were acquired via the fast path).
        wait_for_condition(
            lambda: ray.get(queue.get_stats.remote()).total_timeouts >= 1,
            timeout=30,
        )

        # Release the blockers so capacity frees up.
        ray.get(signal.send.remote())
        for ref in blocking_refs:
            ref.result(timeout_s=15)

        # The waiting request should complete once capacity is available.
        result = waiting_ref.result(timeout_s=15)
        assert isinstance(result, str)

        ray.kill(signal)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v", "-s"]))
