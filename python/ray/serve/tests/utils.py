import time
from typing import Any
from typing import Dict

import grpc
import pytest
import ray
import requests
from ray import serve
from ray.serve._private.constants import (
    SERVE_NAMESPACE,
    RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
)
from ray.serve._private.http_proxy import DRAINED_MESSAGE
from ray.serve._private.usage import ServeUsageTag
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from starlette.requests import Request

TELEMETRY_ROUTE_PREFIX = "/telemetry"
STORAGE_ACTOR_NAME = "storage"


class MockTimer:
    def __init__(self, start_time=None):
        if start_time is None:
            start_time = time.time()
        self._curr = start_time

    def time(self):
        return self._curr

    def advance(self, by):
        self._curr += by

    def realistic_sleep(self, amt):
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
        assert rpc_error.code() == grpc.StatusCode.NOT_FOUND
        assert f"Application '{app_name}' not found." in rpc_error.details()
    else:
        response, call = stub.__call__.with_call(request=request, metadata=metadata)
        assert call.code() == grpc.StatusCode.OK
        assert response.greeting == "Hello foo from bar"


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
    if not RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING:
        return

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
