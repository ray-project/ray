import sys

import grpc
import httpx
import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve._private.test_utils import check_running, get_application_url
from ray.serve.config import RequestRouterConfig, gRPCOptions
from ray.serve.context import _get_internal_replica_context
from ray.serve.generated import serve_pb2, serve_pb2_grpc

ROUTER_CLASS = "ray.serve.experimental.consistent_hash_router:ConsistentHashRouter"


class TestConsistentHashRouting:
    def test_same_session_sticky_to_same_replica(self, serve_instance):
        @serve.deployment(
            request_router_config=RequestRouterConfig(
                request_router_class=ROUTER_CLASS,
                request_router_kwargs={
                    "num_virtual_nodes": 100,
                    "num_fallback_replicas": 2,
                },
                initial_backoff_s=0.01,
                backoff_multiplier=2.0,
                max_backoff_s=0.1,
            ),
            num_replicas=4,
            max_ongoing_requests=5,
            ray_actor_options={"num_cpus": 0},
        )
        class App:
            def __init__(self):
                self.unique_id = _get_internal_replica_context().replica_id.unique_id

            async def __call__(self):
                return self.unique_id

        handle = serve.run(App.bind())
        wait_for_condition(check_running, timeout=30)

        session = "session-123"
        replicas = [
            handle.options(session_id=session).remote().result(timeout_s=10)
            for _ in range(20)
        ]
        assert len(set(replicas)) == 1

    def test_different_sessions_spread_across_replicas(self, serve_instance):
        @serve.deployment(
            request_router_config=RequestRouterConfig(
                request_router_class=ROUTER_CLASS,
                request_router_kwargs={
                    "num_virtual_nodes": 100,
                    "num_fallback_replicas": 2,
                },
                initial_backoff_s=0.01,
                backoff_multiplier=2.0,
                max_backoff_s=0.1,
            ),
            num_replicas=4,
            max_ongoing_requests=5,
            ray_actor_options={"num_cpus": 0},
        )
        class App:
            def __init__(self):
                self.unique_id = _get_internal_replica_context().replica_id.unique_id

            async def __call__(self):
                return self.unique_id

        handle = serve.run(App.bind())
        wait_for_condition(check_running, timeout=30)

        replicas = [
            handle.options(session_id=f"session_{i}").remote().result(timeout_s=10)
            for i in range(40)
        ]
        # With MurmurHash3, the probability of all 40 sessions landing on
        # the same replica is negligible.
        assert len(set(replicas)) > 1

    def test_request_without_session_id_still_routes(self, serve_instance):
        @serve.deployment(
            request_router_config=RequestRouterConfig(
                request_router_class=ROUTER_CLASS,
                request_router_kwargs={
                    "num_virtual_nodes": 100,
                    "num_fallback_replicas": 2,
                },
                initial_backoff_s=0.01,
                backoff_multiplier=2.0,
                max_backoff_s=0.1,
            ),
            num_replicas=3,
            max_ongoing_requests=5,
            ray_actor_options={"num_cpus": 0},
        )
        class App:
            def __init__(self):
                self.unique_id = _get_internal_replica_context().replica_id.unique_id

            async def __call__(self):
                return self.unique_id

        handle = serve.run(App.bind())
        wait_for_condition(check_running, timeout=30)

        for _ in range(10):
            result = handle.remote().result(timeout_s=10)
            assert isinstance(result, str)
            assert len(result) > 0

    def test_mixed_sessioned_and_sessionless_traffic(self, serve_instance):
        @serve.deployment(
            request_router_config=RequestRouterConfig(
                request_router_class=ROUTER_CLASS,
                request_router_kwargs={
                    "num_virtual_nodes": 100,
                    "num_fallback_replicas": 2,
                },
                initial_backoff_s=0.01,
                backoff_multiplier=2.0,
                max_backoff_s=0.1,
            ),
            num_replicas=4,
            max_ongoing_requests=5,
            ray_actor_options={"num_cpus": 0},
        )
        class App:
            def __init__(self):
                self.unique_id = _get_internal_replica_context().replica_id.unique_id

            async def __call__(self):
                return self.unique_id

        handle = serve.run(App.bind())
        wait_for_condition(check_running, timeout=30)

        sticky_session = "sticky-session"
        sticky_landings = []
        for _ in range(10):
            sticky_landings.append(
                handle.options(session_id=sticky_session).remote().result(timeout_s=10)
            )
            # Interleave a session-less request.
            handle.remote().result(timeout_s=10)

        assert len(set(sticky_landings)) == 1


class TestOverflowToFallback:
    """
    When the primary is at max_ongoing_requests, session traffic must
    overflow to the fallback replica rather than hang.
    """

    def test_overflow_when_primary_saturated(self, serve_instance):
        signal_actor_name = "consistent-hash-signal"
        signal = SignalActor.options(name=signal_actor_name).remote()

        # max_ongoing_requests=1 so a single in-flight request fully
        # saturates the primary.
        @serve.deployment(
            request_router_config=RequestRouterConfig(
                request_router_class=ROUTER_CLASS,
                request_router_kwargs={
                    "num_virtual_nodes": 100,
                    "num_fallback_replicas": 2,
                },
                initial_backoff_s=0.01,
                backoff_multiplier=2.0,
                max_backoff_s=0.1,
            ),
            num_replicas=3,
            max_ongoing_requests=1,
            ray_actor_options={"num_cpus": 0},
        )
        class BlockingApp:
            def __init__(self):
                self.unique_id = _get_internal_replica_context().replica_id.unique_id

            async def __call__(self):
                await ray.get_actor(signal_actor_name).wait.remote()
                return self.unique_id

        handle = serve.run(BlockingApp.bind())
        wait_for_condition(check_running, timeout=30)

        session = "overflow-session"

        # Fire the first request on the session. It takes the primary and blocks on the signal.
        first_ref = handle.options(session_id=session).remote()
        wait_for_condition(
            lambda: ray.get(signal.cur_num_waiters.remote()) == 1,
            timeout=10,
        )

        # Second request on the same session -- primary is at max_ongoing_requests,
        # so the retry loop should walk to fallback_1 and that replica picks it up.
        second_ref = handle.options(session_id=session).remote()
        wait_for_condition(
            lambda: ray.get(signal.cur_num_waiters.remote()) == 2,
            timeout=10,
        )

        # Release both and read the replica ids.
        ray.get(signal.send.remote())
        first_replica = first_ref.result(timeout_s=10)
        second_replica = second_ref.result(timeout_s=10)

        assert first_replica != second_replica

    def test_sticky_returns_to_primary_after_drain(self, serve_instance):
        signal_actor_name = "consistent-hash-signal-drain"
        signal = SignalActor.options(name=signal_actor_name).remote()

        @serve.deployment(
            request_router_config=RequestRouterConfig(
                request_router_class=ROUTER_CLASS,
                request_router_kwargs={
                    "num_virtual_nodes": 100,
                    "num_fallback_replicas": 2,
                },
                initial_backoff_s=0.01,
                backoff_multiplier=2.0,
                max_backoff_s=0.1,
            ),
            num_replicas=3,
            max_ongoing_requests=1,
            ray_actor_options={"num_cpus": 0},
        )
        class BlockingApp:
            def __init__(self):
                self.unique_id = _get_internal_replica_context().replica_id.unique_id

            async def __call__(self):
                await ray.get_actor(signal_actor_name).wait.remote()
                return self.unique_id

        handle = serve.run(BlockingApp.bind())
        wait_for_condition(check_running, timeout=30)

        session = "returning-session"

        # Fire one blocking request to identify the primary.
        first_ref = handle.options(session_id=session).remote()
        wait_for_condition(
            lambda: ray.get(signal.cur_num_waiters.remote()) == 1,
            timeout=10,
        )
        ray.get(signal.send.remote())
        primary = first_ref.result(timeout_s=10)

        # Now fire several non-blocking follow-up requests on the same session.
        # They should all land on the primary because the ring is unchanged and
        # the primary is free.
        follow_ups = [
            handle.options(session_id=session).remote().result(timeout_s=10)
            for _ in range(5)
        ]
        assert all(r == primary for r in follow_ups)


class TestProtocolStickiness:
    def test_http_same_session_sticky(self, serve_instance):
        @serve.deployment(
            request_router_config=RequestRouterConfig(
                request_router_class=ROUTER_CLASS,
                request_router_kwargs={
                    "num_virtual_nodes": 100,
                    "num_fallback_replicas": 2,
                },
            ),
            num_replicas=4,
            ray_actor_options={"num_cpus": 0},
        )
        class App:
            async def __call__(self, request):
                return _get_internal_replica_context().replica_id.unique_id

        serve.run(App.bind())
        wait_for_condition(check_running, timeout=30)

        url = get_application_url()
        session_replicas = {
            httpx.get(url, headers={"x-session-id": "sess_http_42"}, timeout=10).text
            for _ in range(15)
        }
        other_replicas = {
            httpx.get(url, headers={"x-session-id": "sess_http_99"}, timeout=10).text
            for _ in range(15)
        }
        assert len(session_replicas) == 1, f"sess_http_42 drifted: {session_replicas}"
        assert len(other_replicas) == 1, f"sess_http_99 drifted: {other_replicas}"

    def test_grpc_same_session_sticky(self, ray_cluster):
        cluster = ray_cluster
        cluster.add_node(num_cpus=2)
        cluster.connect(namespace=SERVE_NAMESPACE)

        serve.start(
            grpc_options=gRPCOptions(
                port=9000,
                grpc_servicer_functions=[
                    "ray.serve.generated.serve_pb2_grpc."
                    "add_UserDefinedServiceServicer_to_server",
                ],
            ),
        )

        @serve.deployment(
            request_router_config=RequestRouterConfig(
                request_router_class=ROUTER_CLASS,
                request_router_kwargs={
                    "num_virtual_nodes": 100,
                    "num_fallback_replicas": 2,
                },
            ),
            num_replicas=4,
            ray_actor_options={"num_cpus": 0},
        )
        class GrpcApp:
            def __call__(self, user_message):
                return serve_pb2.UserDefinedResponse(
                    greeting=_get_internal_replica_context().replica_id.unique_id,
                )

        serve.run(GrpcApp.bind())
        wait_for_condition(check_running, timeout=30)

        channel = grpc.insecure_channel(get_application_url("gRPC", use_localhost=True))
        stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
        req = serve_pb2.UserDefinedMessage(name="x", num=1, foo="y")

        def run(session_id: str) -> set:
            metadata = (("session_id", session_id),)
            landings = set()
            for _ in range(15):
                resp, call = stub.__call__.with_call(request=req, metadata=metadata)
                assert call.code() == grpc.StatusCode.OK
                landings.add(resp.greeting)
            return landings

        assert len(run("sess_grpc_42")) == 1
        assert len(run("sess_grpc_99")) == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
