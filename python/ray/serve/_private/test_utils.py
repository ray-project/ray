import asyncio
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import grpc
import pytest
import requests
from starlette.requests import Request

import ray
import ray.util.state as state_api
from ray import serve
from ray.actor import ActorHandle
from ray.serve._private.common import DeploymentID, DeploymentStatus
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME, SERVE_NAMESPACE
from ray.serve._private.deployment_state import ALL_REPLICA_STATES, ReplicaState
from ray.serve._private.proxy import DRAINING_MESSAGE
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import TimerBase
from ray.serve.generated import serve_pb2, serve_pb2_grpc

TELEMETRY_ROUTE_PREFIX = "/telemetry"
STORAGE_ACTOR_NAME = "storage"


class MockTimer(TimerBase):
    def __init__(self, start_time: Optional[float] = None):
        self._lock = threading.Lock()
        self.reset(start_time=start_time)

    def reset(self, start_time: Optional[float] = None):
        if start_time is None:
            start_time = time.time()
        self._curr = start_time

    def time(self) -> float:
        return self._curr

    def advance(self, by: float):
        with self._lock:
            self._curr += by

    def realistic_sleep(self, amt: float):
        with self._lock:
            self._curr += amt + 0.001


class MockAsyncTimer:
    def __init__(self, start_time: Optional[float] = 0):
        self.reset(start_time=start_time)
        self._num_sleepers = 0

    def reset(self, start_time: 0):
        self._curr = start_time

    def time(self) -> float:
        return self._curr

    async def sleep(self, amt: float):
        self._num_sleepers += 1
        end = self._curr + amt

        # Give up the event loop
        while self._curr < end:
            await asyncio.sleep(0)

        self._num_sleepers -= 1

    def advance(self, amt: float):
        self._curr += amt

    def num_sleepers(self):
        return self._num_sleepers


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


class MockClusterNodeInfoCache:
    def __init__(self):
        self.alive_node_ids = set()
        self.alive_node_resources = dict()
        self.draining_nodes = dict()

    def get_alive_node_ids(self):
        return self.alive_node_ids

    def get_draining_nodes(self):
        return self.draining_nodes

    def get_active_node_ids(self):
        return self.alive_node_ids - set(self.draining_nodes)

    def get_node_az(self, node_id):
        return None

    def get_available_resources_per_node(self):
        return self.alive_node_resources

    def get_total_resources_per_node(self):
        return self.alive_node_resources

    def add_node(self, node_id: str, resources: Dict = None):
        self.alive_node_ids.add(node_id)
        self.alive_node_resources[node_id] = resources or {}


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


def check_deployment_status(name, expected_status) -> DeploymentStatus:
    app_status = serve.status().applications[SERVE_DEFAULT_APP_NAME]
    assert app_status.deployments[name].status == expected_status
    return True


def get_num_running_replicas(
    deployment_name: str, app_name: str = SERVE_DEFAULT_APP_NAME
) -> int:
    """Get the replicas currently running for the given deployment."""

    dep_id = DeploymentID(name=deployment_name, app_name=app_name)
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

    assert get_num_running_replicas(name, app_name) >= target
    return True


def check_num_replicas_eq(
    name: str, target: int, app_name: str = SERVE_DEFAULT_APP_NAME
) -> int:
    """Check if num replicas is == target."""

    assert get_num_running_replicas(name, app_name) == target
    return True


def check_num_replicas_lte(
    name: str, target: int, app_name: str = SERVE_DEFAULT_APP_NAME
) -> int:
    """Check if num replicas is <= target."""

    assert get_num_running_replicas(name, app_name) <= target
    return True


def check_replica_counts(
    controller: ActorHandle,
    deployment_id: DeploymentID,
    total: Optional[int] = None,
    by_state: Optional[List[Tuple[ReplicaState, int, Callable]]] = None,
):
    """Uses _dump_replica_states_for_testing to check replica counts.

    Args:
        controller: A handle to the Serve controller.
        deployment_id: The deployment to check replica counts for.
        total: The total number of expected replicas for the deployment.
        by_state: A list of tuples of the form
            (replica state, number of replicas, filter function).
            Used for more fine grained checks.
    """
    replicas = ray.get(
        controller._dump_replica_states_for_testing.remote(deployment_id)
    )

    if total is not None:
        replica_counts = {
            state: len(replicas.get([state]))
            for state in ALL_REPLICA_STATES
            if replicas.get([state])
        }
        assert replicas.count() == total, replica_counts

    if by_state is not None:
        for state, count, check in by_state:
            assert isinstance(state, ReplicaState)
            assert isinstance(count, int) and count >= 0
            if check:
                filtered = {r for r in replicas.get(states=[state]) if check(r)}
                curr_count = len(filtered)
            else:
                curr_count = replicas.count(states=[state])
            msg = f"Expected {count} for state {state} but got {curr_count}."
            assert curr_count == count, msg

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
        assert rpc_error.details() == DRAINING_MESSAGE
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
        assert rpc_error.details() == DRAINING_MESSAGE
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
        # Clear the context var to avoid Ray recursively cancelling this method call.
        ray._raylet.async_task_id.set(None)
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


class FakeGauge:
    def __init__(self, name: str = None, tag_keys: Tuple[str] = None):
        self.name = name
        self.value = 0
        if tag_keys:
            self.tags = {key: None for key in tag_keys}
        else:
            self.tags = dict()

    def set_default_tags(self, tags: Dict[str, str]):
        for key, tag in tags.items():
            assert key in self.tags
            self.tags[key] = tag

    def set(self, value: Union[int, float], tags: Dict[str, str] = None):
        self.value = value
        if tags:
            self.tags.update(tags)

    def get_value(self):
        return self.value

    def get_tags(self):
        return self.tags


class FakeCounter:
    def __init__(self, name: str = None, tag_keys: Tuple[str] = None):
        self.name = name
        self.count: int = 0
        if tag_keys:
            self.tags = {key: None for key in tag_keys}
        else:
            self.tags = dict()

    def set_default_tags(self, tags: Dict[str, str]):
        for key, tag in tags.items():
            assert key in self.tags
            self.tags[key] = tag

    def inc(self, value: Union[int, float] = 1.0, tags: Dict[str, str] = None):
        self.count += value
        if tags:
            self.tags.update(tags)

    def get_count(self) -> int:
        return self.count

    def get_tags(self):
        return self.tags
