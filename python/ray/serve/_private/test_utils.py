import asyncio
import datetime
import os
import threading
import time
from contextlib import asynccontextmanager
from copy import copy, deepcopy
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import grpc
import requests
from starlette.requests import Request

import ray
import ray.util.state as state_api
from ray import serve
from ray.actor import ActorHandle
from ray.serve._private.client import ServeControllerClient
from ray.serve._private.common import (
    CreatePlacementGroupRequest,
    DeploymentID,
    DeploymentStatus,
    RequestProtocol,
)
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME, SERVE_NAMESPACE
from ray.serve._private.deployment_state import ALL_REPLICA_STATES, ReplicaState
from ray.serve._private.proxy import DRAINING_MESSAGE
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import TimerBase
from ray.serve.context import _get_global_client
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from ray.serve.schema import ApplicationStatus

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
        self.total_resources_per_node = dict()
        self.available_resources_per_node = dict()
        self.draining_nodes = dict()
        self.node_labels = dict()

    def get_alive_node_ids(self):
        return self.alive_node_ids

    def get_draining_nodes(self):
        return self.draining_nodes

    def get_active_node_ids(self):
        return self.alive_node_ids - set(self.draining_nodes)

    def get_node_az(self, node_id):
        return None

    def get_available_resources_per_node(self):
        return self.available_resources_per_node

    def get_total_resources_per_node(self):
        return self.total_resources_per_node

    def add_node(self, node_id: str, resources: Dict = None, labels: Dict = None):
        self.alive_node_ids.add(node_id)
        self.total_resources_per_node[node_id] = deepcopy(resources) or {}
        self.available_resources_per_node[node_id] = deepcopy(resources) or {}
        self.node_labels[node_id] = labels or {}

    def set_available_resources_per_node(self, node_id: str, resources: Dict):
        self.available_resources_per_node[node_id] = deepcopy(resources)


class FakeRemoteFunction:
    def remote(self):
        pass


class MockActorHandle:
    def __init__(self, **kwargs):
        self._options = kwargs
        self._actor_id = "fake_id"
        self.initialize_and_get_metadata_called = False
        self.is_allocated_called = False

    @property
    def initialize_and_get_metadata(self):
        self.initialize_and_get_metadata_called = True
        # return a mock object so that we can call `remote()` on it.
        return FakeRemoteFunction()

    @property
    def is_allocated(self):
        self.is_allocated_called = True
        return FakeRemoteFunction()


class MockActorClass:
    def __init__(self):
        self._init_args = ()
        self._options = dict()

    def options(self, **kwargs):
        res = copy(self)

        for k, v in kwargs.items():
            res._options[k] = v

        return res

    def remote(self, *args) -> MockActorHandle:
        return MockActorHandle(init_args=args, **self._options)


class MockPlacementGroup:
    def __init__(self, request: CreatePlacementGroupRequest):
        self._bundles = request.bundles
        self._strategy = request.strategy
        self._soft_target_node_id = request.target_node_id
        self._name = request.name
        self._lifetime = "detached"


class MockDeploymentHandle:
    def __init__(self, deployment_name: str, app_name: str = SERVE_DEFAULT_APP_NAME):
        self._deployment_name = deployment_name
        self._app_name = app_name
        self._protocol = RequestProtocol.UNDEFINED
        self._running_replicas_populated = False
        self._initialized = False

    def is_initialized(self):
        return self._initialized

    def _init(self):
        if self._initialized:
            raise RuntimeError("already initialized")

        self._initialized = True

    def options(self, *args, **kwargs):
        return self

    def __eq__(self, dep: Tuple[str]):
        other_deployment_name, other_app_name = dep
        return (
            self._deployment_name == other_deployment_name
            and self._app_name == other_app_name
        )

    def _set_request_protocol(self, protocol: RequestProtocol):
        self._protocol = protocol

    def _get_or_create_router(self):
        pass

    def running_replicas_populated(self) -> bool:
        return self._running_replicas_populated

    def set_running_replicas_populated(self, val: bool):
        self._running_replicas_populated = val


@serve.deployment
class GetPID:
    def __call__(self):
        return os.getpid()


get_pid_entrypoint = GetPID.bind()


def check_ray_stopped():
    try:
        requests.get("http://localhost:52365/api/ray/version")
        return False
    except Exception:
        return True


def check_ray_started():
    return requests.get("http://localhost:52365/api/ray/version").status_code == 200


def check_deployment_status(
    name: str, expected_status: DeploymentStatus, app_name=SERVE_DEFAULT_APP_NAME
) -> bool:
    app_status = serve.status().applications[app_name]
    assert app_status.deployments[name].status == expected_status
    return True


def get_num_alive_replicas(
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

    assert get_num_alive_replicas(name, app_name) >= target
    return True


def check_num_replicas_eq(
    name: str, target: int, app_name: str = SERVE_DEFAULT_APP_NAME
) -> int:
    """Check if num replicas is == target."""

    assert get_num_alive_replicas(name, app_name) == target
    return True


def check_num_replicas_lte(
    name: str, target: int, app_name: str = SERVE_DEFAULT_APP_NAME
) -> int:
    """Check if num replicas is <= target."""

    assert get_num_alive_replicas(name, app_name) <= target
    return True


def check_apps_running(apps: List):
    status = serve.status()

    for app_name in apps:
        assert status.applications[app_name].status == ApplicationStatus.RUNNING

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


def check_telemetry(
    tag: ServeUsageTag, expected: Any, storage_actor_name: str = STORAGE_ACTOR_NAME
):
    storage_handle = ray.get_actor(storage_actor_name, namespace=SERVE_NAMESPACE)
    report = ray.get(storage_handle.get_report.remote())
    print(report["extra_usage_tags"])
    assert tag.get_value_from_report(report) == expected
    return True


def ping_grpc_list_applications(channel, app_names, test_draining=False):
    import pytest

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
    import pytest

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
    import pytest

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


@asynccontextmanager
async def send_signal_on_cancellation(signal_actor: ActorHandle):
    cancelled = False
    try:
        yield
        await asyncio.sleep(100)
    except asyncio.CancelledError:
        cancelled = True
        # Clear the context var to avoid Ray recursively cancelling this method call.
        ray._raylet.async_task_id.set(None)
        await signal_actor.send.remote()

    if not cancelled:
        raise RuntimeError(
            "CancelledError wasn't raised during `send_signal_on_cancellation` block"
        )


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
        self.values = dict()

        self.tags = tag_keys or ()
        self.default_tags = dict()

    def set_default_tags(self, tags: Dict[str, str]):
        for key, tag in tags.items():
            assert key in self.tags
            self.default_tags[key] = tag

    def set(self, value: Union[int, float], tags: Dict[str, str] = None):
        merged_tags = self.default_tags.copy()
        merged_tags.update(tags or {})
        assert set(merged_tags.keys()) == set(self.tags)

        d = self.values
        for tag in self.tags[:-1]:
            tag_value = merged_tags[tag]
            if tag_value not in d:
                d[tag_value] = dict()
            d = d[tag_value]

        d[merged_tags[self.tags[-1]]] = value

    def get_value(self, tags: Dict[str, str]):
        value = self.values
        for tag in self.tags:
            tag_value = tags[tag]
            value = value.get(tag_value)
            if value is None:
                return

        return value


class FakeCounter:
    def __init__(self, name: str = None, tag_keys: Tuple[str] = None):
        self.name = name
        self.counts = dict()

        self.tags = tag_keys or ()
        self.default_tags = dict()

    def set_default_tags(self, tags: Dict[str, str]):
        for key, tag in tags.items():
            assert key in self.tags
            self.default_tags[key] = tag

    def inc(self, value: Union[int, float] = 1.0, tags: Dict[str, str] = None):
        merged_tags = self.default_tags.copy()
        merged_tags.update(tags or {})
        assert set(merged_tags.keys()) == set(self.tags)

        d = self.counts
        for tag in self.tags[:-1]:
            tag_value = merged_tags[tag]
            if tag_value not in d:
                d[tag_value] = dict()
            d = d[tag_value]

        key = merged_tags[self.tags[-1]]
        d[key] = d.get(key, 0) + value

    def get_count(self, tags: Dict[str, str]) -> int:
        value = self.counts
        for tag in self.tags:
            tag_value = tags[tag]
            value = value.get(tag_value)
            if value is None:
                return

        return value

    def get_tags(self):
        return self.tags


@ray.remote
def get_node_id():
    return ray.get_runtime_context().get_node_id()


def check_num_alive_nodes(target: int):
    alive_nodes = [node for node in ray.nodes() if node["Alive"]]
    assert len(alive_nodes) == target
    return True


def get_deployment_details(
    deployment_name: str,
    app_name: str = SERVE_DEFAULT_APP_NAME,
    _client: ServeControllerClient = None,
):
    client = _client or _get_global_client()
    details = client.get_serve_details()
    return details["applications"][app_name]["deployments"][deployment_name]


@ray.remote
class Counter:
    def __init__(self, target: int):
        self.count = 0
        self.target = target
        self.ready_event = asyncio.Event()

    def inc(self):
        self.count += 1
        if self.count == self.target:
            self.ready_event.set()

    async def wait(self):
        await self.ready_event.wait()


def tlog(s: str, level: str = "INFO"):
    """Convenient logging method for testing."""

    now = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{level}] {now} {s}")
