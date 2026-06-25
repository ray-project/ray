import asyncio
import sys

import grpc
import httpx
import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_DIRECT_INGRESS,
)
from ray.serve._private.test_utils import check_running, get_application_url
from ray.serve.config import RequestRouterConfig
from ray.serve.context import _get_internal_replica_context
from ray.serve.generated import serve_pb2, serve_pb2_grpc

ROUTER_CLASS = "ray.serve.experimental.round_robin_router:RoundRobinRouter"


def _round_robin_config() -> RequestRouterConfig:
    return RequestRouterConfig(
        request_router_class=ROUTER_CLASS,
        initial_backoff_s=0.01,
        backoff_multiplier=1.0,
        max_backoff_s=0.01,
    )


def _get_request_router(handle):
    if not handle.is_initialized:
        handle._init()
    return handle._router._asyncio_router._request_router


@ray.remote(num_cpus=0)
class ReplicaBlocker:
    def __init__(self):
        self.replica_id = None
        self.released = False

    def record_replica(self, replica_id: str):
        self.replica_id = replica_id

    def get_replica(self):
        return self.replica_id

    def release(self):
        self.released = True

    def is_released(self):
        return self.released


def _assert_round_robin_wraps_and_cycles(call, num_replicas: int) -> None:
    wait_for_condition(
        lambda: len({call() for _ in range(num_replicas * 2)}) == num_replicas,
        timeout=30,
    )

    replicas = [call() for _ in range(num_replicas * 2)]
    expected_cycle = replicas[:num_replicas]
    assert len(set(expected_cycle)) == num_replicas
    assert replicas[num_replicas] == replicas[0]
    assert replicas[num_replicas:] == expected_cycle


def test_handle_options_and_http_round_robin(serve_instance):
    num_replicas = 3

    @serve.deployment(
        request_router_config=_round_robin_config(),
        num_replicas=num_replicas,
        max_ongoing_requests=100,
        ray_actor_options={"num_cpus": 0},
    )
    class App:
        def __init__(self):
            self.unique_id = _get_internal_replica_context().replica_id.unique_id

        def __call__(self):
            return self.unique_id

        def via_options(self):
            return self.unique_id

    handle = serve.run(App.bind())
    wait_for_condition(check_running, timeout=30)

    def handle_call():
        response = (
            handle.options(method_name="via_options").remote().result(timeout_s=10)
        )
        return response

    _assert_round_robin_wraps_and_cycles(handle_call, num_replicas)

    url = get_application_url()

    def http_call():
        response = httpx.get(url, timeout=10)
        assert response.status_code == 200
        return response.text

    _assert_round_robin_wraps_and_cycles(http_call, num_replicas)


def test_handle_round_robin_backs_off_from_full_replica(serve_instance):
    @serve.deployment(
        request_router_config=_round_robin_config(),
        num_replicas=2,
        max_ongoing_requests=1,
        ray_actor_options={"num_cpus": 0},
    )
    class BlockingApp:
        def __init__(self):
            self.unique_id = _get_internal_replica_context().replica_id.unique_id

        async def __call__(self):
            return self.unique_id

        async def block(self, blocker):
            await blocker.record_replica.remote(self.unique_id)
            while not await blocker.is_released.remote():
                await asyncio.sleep(0.05)
            return self.unique_id

    handle = serve.run(BlockingApp.bind())
    wait_for_condition(check_running, timeout=30)

    blocker = ReplicaBlocker.remote()
    blocked_ref = handle.options(method_name="block").remote(blocker)
    wait_for_condition(
        lambda: ray.get(blocker.get_replica.remote()) is not None,
        timeout=30,
    )
    blocked_replica = ray.get(blocker.get_replica.remote())

    request_router = _get_request_router(handle)

    def _set_counter_to_blocked_replica():
        for index, replica in enumerate(request_router._replicas_list):
            if replica.replica_id.unique_id == blocked_replica:
                request_router._round_robin_counter = index
                return True
        return False

    wait_for_condition(_set_counter_to_blocked_replica, timeout=30)

    try:
        fallback_replica = handle.remote().result(timeout_s=10)
        assert fallback_replica != blocked_replica
    finally:
        ray.get(blocker.release.remote())
        assert blocked_ref.result(timeout_s=10) == blocked_replica


def test_grpc_round_robin(serve_instance):
    if RAY_SERVE_ENABLE_DIRECT_INGRESS:
        pytest.skip("Routing doesn't happen in direct ingress mode.")

    num_replicas = 3
    app_name = "grpc-round-robin"

    @serve.deployment(
        request_router_config=_round_robin_config(),
        num_replicas=num_replicas,
        max_ongoing_requests=100,
        ray_actor_options={"num_cpus": 0},
    )
    class GrpcApp:
        def __init__(self):
            self.unique_id = _get_internal_replica_context().replica_id.unique_id

        def __call__(self, user_message):
            return serve_pb2.UserDefinedResponse(
                greeting=self.unique_id,
                num_x2=user_message.num * 2,
            )

    serve.run(GrpcApp.bind(), name=app_name, route_prefix=f"/{app_name}")
    wait_for_condition(lambda: check_running(app_name), timeout=30)

    channel = grpc.insecure_channel(
        get_application_url("gRPC", app_name=app_name, use_localhost=True)
    )
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    request = serve_pb2.UserDefinedMessage(name="x", num=1, foo="y")
    metadata = (("application", app_name),)

    def grpc_call():
        response, call = stub.__call__.with_call(
            request=request,
            metadata=metadata,
        )
        assert call.code() == grpc.StatusCode.OK
        return response.greeting

    _assert_round_robin_wraps_and_cycles(grpc_call, num_replicas)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
