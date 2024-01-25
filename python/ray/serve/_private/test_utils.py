import asyncio
import threading
import time
from typing import Any, Dict

import grpc
import pytest
import requests
from starlette.requests import Request

import ray
import ray.util.state as state_api
from ray import serve
from ray.actor import ActorHandle
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME, SERVE_NAMESPACE
from ray.serve._private.proxy import DRAINED_MESSAGE
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import TimerBase
from ray.serve.generated import serve_pb2, serve_pb2_grpc

TELEMETRY_ROUTE_PREFIX = "/telemetry"
STORAGE_ACTOR_NAME = "storage"


class MockTimer(TimerBase):
    def __init__(self, start_time=None):
        self._lock = threading.Lock()

        if start_time is None:
            start_time = time.time()
        self._curr = start_time

    def time(self):
        return self._curr

    def advance(self, by):
        with self._lock:
            self._curr += by

    def realistic_sleep(self, amt):
        with self._lock:
            self._curr += amt + 0.001


class MockKVStore:
    def __init__(self):
        self.store = dict()

    def put(self, key: str, val: Any) -> bool:
        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))
        self.store[key] = val
        return True

    def get(self, key: str) -> Any:
        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))
        return self.store.get(key, None)

    def delete(self, key: str) -> bool:
        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))

        if key in self.store:
            del self.store[key]
            return True

        return False


def check_ray_stopped():
    try:
        requests.get("http://localhost:52365/api/ray/version")
        return False
    except Exception:
        return True


def check_ray_started():
    return requests.get("http://localhost:52365/api/ray/version").status_code == 200


def check_telemetry_recorded(storage_handle, key, expected_value):
    report = ray.get(storage_handle.get_report.remote())
    assert report["extra_usage_tags"][key] == expected_value
    return True


def check_telemetry_not_recorded(storage_handle, key):
    report = ray.get(storage_handle.get_report.remote())
    assert (
        ServeUsageTag.DEPLOYMENT_HANDLE_TO_OBJECT_REF_API_USED.get_value_from_report(
            report
        )
        is None
    )


def get_num_running_replicas(
    deployment_name: str, app_name: str = SERVE_DEFAULT_APP_NAME
) -> int:
    """Get the replicas currently running for the given deployment."""

    dep_id = DeploymentID(deployment_name, app_name)
    actors = state_api.list_actors(
        filters=[
            ("class_name", "=", dep_id.to_replica_actor_class_name()),
            ("state", "=", "ALIVE"),
        ]
    )
    return len(actors)


def check_num_replicas_gte(
    name: str, target: int, app_name: str = SERVE_DEFAULT_APP_NAME
) -> int:
    """Check if num replicas is >= target."""

    assert get_num_running_replicas(name) >= target
    return True


def check_num_replicas_eq(
    name: str, target: int, app_name: str = SERVE_DEFAULT_APP_NAME
) -> int:
    """Check if num replicas is == target."""

    assert get_num_running_replicas(name) == target
    return True


def check_num_replicas_lte(
    name: str, target: int, app_name: str = SERVE_DEFAULT_APP_NAME
) -> int:
    """Check if num replicas is <= target."""

    assert get_num_running_replicas(name) <= target
    return True


@ray.remote(name=STORAGE_ACTOR_NAME, namespace=SERVE_NAMESPACE, num_cpus=0)
class TelemetryStorage:
    def __init__(self):
        self.reports_received = 0
        self.current_report = dict()

    def store_report(self, report: Dict) -> None:
        self.reports_received += 1
        self.current_report = report

    def get_report(self) -> Dict:
        return self.current_report

    def get_reports_received(self) -> int:
        return self.reports_received


@serve.deployment(ray_actor_options={"num_cpus": 0})
class TelemetryReceiver:
    def __init__(self):
        self.storage = ray.get_actor(name=STORAGE_ACTOR_NAME, namespace=SERVE_NAMESPACE)

    async def __call__(self, request: Request) -> bool:
        report = await request.json()
        ray.get(self.storage.store_report.remote(report))
        return True


receiver_app = TelemetryReceiver.bind()


def start_telemetry_app():
    """Start a telemetry Serve app.

    Ray should be initialized before calling this method.

    NOTE: If you're running the TelemetryReceiver Serve app to check telemetry,
    remember that the receiver itself is counted in the telemetry. E.g. if you
    deploy a Serve app other than the receiver, the number of apps in the
    cluster is 2- not 1– since the receiver is also running.

    Returns a handle to a TelemetryStorage actor. You can use this actor
    to access the latest telemetry reports.
    """

    storage = TelemetryStorage.remote()
    serve.run(receiver_app, name="telemetry", route_prefix=TELEMETRY_ROUTE_PREFIX)
    return storage


def ping_grpc_list_applications(channel, app_names, test_draining=False):
    stub = serve_pb2_grpc.RayServeAPIServiceStub(channel)
    request = serve_pb2.ListApplicationsRequest()
    if test_draining:
        with pytest.raises(grpc.RpcError) as exception_info:
            _, _ = stub.ListApplications.with_call(request=request)
        rpc_error = exception_info.value
        assert rpc_error.code() == grpc.StatusCode.UNAVAILABLE
        assert rpc_error.details() == DRAINED_MESSAGE
    else:
        response, call = stub.ListApplications.with_call(request=request)
        assert call.code() == grpc.StatusCode.OK
        assert response.application_names == app_names
    return True


def ping_grpc_healthz(channel, test_draining=False):
    stub = serve_pb2_grpc.RayServeAPIServiceStub(channel)
    request = serve_pb2.HealthzRequest()
    if test_draining:
        with pytest.raises(grpc.RpcError) as exception_info:
            _, _ = stub.Healthz.with_call(request=request)
        rpc_error = exception_info.value
        assert rpc_error.code() == grpc.StatusCode.UNAVAILABLE
        assert rpc_error.details() == DRAINED_MESSAGE
    else:
        response, call = stub.Healthz.with_call(request=request)
        assert call.code() == grpc.StatusCode.OK
        assert response.message == "success"


def ping_grpc_call_method(channel, app_name, test_not_found=False):
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")
    metadata = (("application", app_name),)
    if test_not_found:
        with pytest.raises(grpc.RpcError) as exception_info:
            _, _ = stub.__call__.with_call(request=request, metadata=metadata)
        rpc_error = exception_info.value
        assert rpc_error.code() == grpc.StatusCode.NOT_FOUND, rpc_error.code()
        assert f"Application '{app_name}' not found." in rpc_error.details()
    else:
        response, call = stub.__call__.with_call(request=request, metadata=metadata)
        assert call.code() == grpc.StatusCode.OK, call.code()
        assert response.greeting == "Hello foo from bar", response.greeting


def ping_grpc_another_method(channel, app_name):
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")
    metadata = (("application", app_name),)
    response = stub.Method1(request=request, metadata=metadata)
    assert response.greeting == "Hello foo from method1"


def ping_grpc_model_multiplexing(channel, app_name):
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")
    multiplexed_model_id = "999"
    metadata = (
        ("application", app_name),
        ("multiplexed_model_id", multiplexed_model_id),
    )
    response = stub.Method2(request=request, metadata=metadata)
    assert (
        response.greeting
        == f"Method2 called model, loading model: {multiplexed_model_id}"
    )


def ping_grpc_streaming(channel, app_name):
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")
    metadata = (("application", app_name),)
    responses = stub.Streaming(request=request, metadata=metadata)
    for idx, response in enumerate(responses):
        assert response.greeting == f"{idx}: Hello foo from bar"


def ping_fruit_stand(channel, app_name):
    stub = serve_pb2_grpc.FruitServiceStub(channel)
    request = serve_pb2.FruitAmounts(orange=4, apple=8)
    metadata = (("application", app_name),)
    response = stub.FruitStand(request=request, metadata=metadata)
    assert response.costs == 32


async def send_signal_on_cancellation(signal_actor: ActorHandle):
    try:
        await asyncio.sleep(100000)
    except asyncio.CancelledError:
        await signal_actor.send.remote()


class FakeGrpcContext:
    def __init__(self):
        self._auth_context = {"key": "value"}
        self._invocation_metadata = [("key", "value")]
        self._peer = "peer"
        self._peer_identities = b"peer_identities"
        self._peer_identity_key = "peer_identity_key"
        self._code = None
        self._details = None
        self._trailing_metadata = []
        self._invocation_metadata = []

    def auth_context(self):
        return self._auth_context

    def code(self):
        return self._code

    def details(self):
        return self._details

    def peer(self):
        return self._peer

    def peer_identities(self):
        return self._peer_identities

    def peer_identity_key(self):
        return self._peer_identity_key

    def trailing_metadata(self):
        return self._trailing_metadata

    def set_code(self, code):
        self._code = code

    def set_details(self, details):
        self._details = details

    def set_trailing_metadata(self, trailing_metadata):
        self._trailing_metadata = trailing_metadata

    def invocation_metadata(self):
        return self._invocation_metadata
