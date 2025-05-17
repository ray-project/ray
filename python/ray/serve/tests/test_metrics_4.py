import random
import sys
from typing import Dict, Optional

import pytest
import requests

import ray
import ray.util.state as state_api
from ray import serve
from ray._private.test_utils import (
    SignalActor,
    fetch_prometheus_metrics,
    wait_for_condition,
)
from ray.serve._private.long_poll import LongPollHost, UpdatedObject
from ray.serve.config import HTTPOptions, gRPCOptions
from ray.serve.handle import DeploymentHandle

TEST_METRICS_EXPORT_PORT = 9999


@pytest.fixture
def serve_start_shutdown(request):
    serve.shutdown()
    ray.shutdown()
    ray._private.utils.reset_ray_address()

    param = request.param if hasattr(request, "param") else None
    request_timeout_s = param if param else None
    """Fixture provides a fresh Ray cluster to prevent metrics state sharing."""
    ray.init(
        _metrics_export_port=TEST_METRICS_EXPORT_PORT,
        _system_config={
            "metrics_report_interval_ms": 100,
            "task_retry_delay_ms": 50,
        },
    )
    grpc_port = 9000
    grpc_servicer_functions = [
        "ray.serve.generated.serve_pb2_grpc.add_UserDefinedServiceServicer_to_server",
        "ray.serve.generated.serve_pb2_grpc.add_FruitServiceServicer_to_server",
    ]
    yield serve.start(
        grpc_options=gRPCOptions(
            port=grpc_port,
            grpc_servicer_functions=grpc_servicer_functions,
            request_timeout_s=request_timeout_s,
        ),
        http_options=HTTPOptions(
            request_timeout_s=request_timeout_s,
        ),
    )
    serve.shutdown()
    ray.shutdown()
    ray._private.utils.reset_ray_address()


def extract_tags(line: str) -> Dict[str, str]:
    """Extracts any tags from the metrics line."""

    try:
        tags_string = line.replace("{", "}").split("}")[1]
    except IndexError:
        # No tags were found in this line.
        return {}

    detected_tags = {}
    for tag_pair in tags_string.split(","):
        sanitized_pair = tag_pair.replace('"', "")
        tag, value = sanitized_pair.split("=")
        detected_tags[tag] = value

    return detected_tags


def contains_tags(line: str, expected_tags: Optional[Dict[str, str]] = None) -> bool:
    """Checks if the metrics line contains the expected tags.

    Does nothing if expected_tags is None.
    """

    if expected_tags is not None:
        detected_tags = extract_tags(line)

        # Check if expected_tags is a subset of detected_tags
        return expected_tags.items() <= detected_tags.items()
    else:
        return True


def get_metric_float(
    metric: str, expected_tags: Optional[Dict[str, str]] = None
) -> float:
    """Gets the float value of metric.

    If tags is specified, searched for metric with matching tags.

    Returns -1 if the metric isn't available.
    """

    metrics = requests.get("http://127.0.0.1:9999").text
    metric_value = -1
    for line in metrics.split("\n"):
        if metric in line and contains_tags(line, expected_tags):
            metric_value = line.split(" ")[-1]
    return metric_value


def check_metric_float_eq(
    metric: str, expected: float, expected_tags: Optional[Dict[str, str]] = None
) -> bool:
    metric_value = get_metric_float(metric, expected_tags)
    assert float(metric_value) == expected
    return True


def check_sum_metric_eq(
    metric_name: str,
    expected: float,
    tags: Optional[Dict[str, str]] = None,
) -> bool:
    if tags is None:
        tags = {}

    metrics = fetch_prometheus_metrics([f"localhost:{TEST_METRICS_EXPORT_PORT}"])
    metric_samples = metrics.get(metric_name, None)
    if metric_samples is None:
        metric_sum = 0
    else:
        metric_samples = [
            sample for sample in metric_samples if tags.items() <= sample.labels.items()
        ]
        metric_sum = sum(sample.value for sample in metric_samples)

    # Check the metrics sum to the expected number
    assert float(metric_sum) == float(
        expected
    ), f"The following metrics don't sum to {expected}: {metric_samples}. {metrics}"

    # # For debugging
    if metric_samples:
        print(f"The following sum to {expected} for '{metric_name}' and tags {tags}:")
        for sample in metric_samples:
            print(sample)

    return True


@serve.deployment
class WaitForSignal:
    async def __call__(self):
        signal = ray.get_actor("signal123")
        await signal.wait.remote()


@serve.deployment
class Router:
    def __init__(self, handles):
        self.handles = handles

    async def __call__(self, index: int):
        return await self.handles[index - 1].remote()


@ray.remote
def call(deployment_name, app_name, *args):
    handle = DeploymentHandle(deployment_name, app_name)
    handle.remote(*args)


@ray.remote
class CallActor:
    def __init__(self, deployment_name: str, app_name: str):
        self.handle = DeploymentHandle(deployment_name, app_name)

    async def call(self, *args):
        await self.handle.remote(*args)


class TestHandleMetrics:
    def test_queued_queries_basic(self, serve_start_shutdown):
        signal = SignalActor.options(name="signal123").remote()
        serve.run(WaitForSignal.options(max_ongoing_requests=1).bind(), name="app1")

        # First call should get assigned to a replica
        # call.remote("WaitForSignal", "app1")
        caller = CallActor.remote("WaitForSignal", "app1")
        caller.call.remote()

        for i in range(5):
            # call.remote("WaitForSignal", "app1")
            # c.call.remote()
            caller.call.remote()
            wait_for_condition(
                check_sum_metric_eq,
                metric_name="ray_serve_deployment_queued_queries",
                tags={"application": "app1"},
                expected=i + 1,
            )

        # Release signal
        ray.get(signal.send.remote())
        wait_for_condition(
            check_sum_metric_eq,
            metric_name="ray_serve_deployment_queued_queries",
            tags={"application": "app1", "deployment": "WaitForSignal"},
            expected=0,
        )

    def test_queued_queries_multiple_handles(self, serve_start_shutdown):
        signal = SignalActor.options(name="signal123").remote()
        serve.run(WaitForSignal.options(max_ongoing_requests=1).bind(), name="app1")

        # Send first request
        call.remote("WaitForSignal", "app1")
        wait_for_condition(
            check_sum_metric_eq,
            metric_name="ray_serve_deployment_queued_queries",
            tags={"application": "app1", "deployment": "WaitForSignal"},
            expected=0,
        )

        # Send second request (which should stay queued)
        call.remote("WaitForSignal", "app1")
        wait_for_condition(
            check_sum_metric_eq,
            metric_name="ray_serve_deployment_queued_queries",
            tags={"application": "app1", "deployment": "WaitForSignal"},
            expected=1,
        )

        # Send third request (which should stay queued)
        call.remote("WaitForSignal", "app1")
        wait_for_condition(
            check_sum_metric_eq,
            metric_name="ray_serve_deployment_queued_queries",
            tags={"application": "app1", "deployment": "WaitForSignal"},
            expected=2,
        )

        # Release signal
        ray.get(signal.send.remote())
        wait_for_condition(
            check_sum_metric_eq,
            metric_name="ray_serve_deployment_queued_queries",
            tags={"application": "app1", "deployment": "WaitForSignal"},
            expected=0,
        )

    def test_queued_queries_disconnected(self, serve_start_shutdown):
        """Check that disconnected queued queries are tracked correctly."""

        signal = SignalActor.remote()

        @serve.deployment(
            max_ongoing_requests=1,
        )
        async def hang_on_first_request():
            await signal.wait.remote()

        serve.run(hang_on_first_request.bind())

        print("Deployed hang_on_first_request deployment.")

        wait_for_condition(
            check_metric_float_eq,
            timeout=15,
            metric="ray_serve_num_scheduling_tasks",
            # Router is eagerly created on HTTP proxy, so there are metrics emitted
            # from proxy router
            expected=0,
            # TODO(zcin): this tag shouldn't be necessary, there shouldn't be a mix of
            # metrics from new and old sessions.
            expected_tags={
                "SessionName": ray._private.worker.global_worker.node.session_name
            },
        )
        print("ray_serve_num_scheduling_tasks updated successfully.")
        wait_for_condition(
            check_metric_float_eq,
            timeout=15,
            metric="serve_num_scheduling_tasks_in_backoff",
            # Router is eagerly created on HTTP proxy, so there are metrics emitted
            # from proxy router
            expected=0,
            # TODO(zcin): this tag shouldn't be necessary, there shouldn't be a mix of
            # metrics from new and old sessions.
            expected_tags={
                "SessionName": ray._private.worker.global_worker.node.session_name
            },
        )
        print("serve_num_scheduling_tasks_in_backoff updated successfully.")

        @ray.remote(num_cpus=0)
        def do_request():
            r = requests.get("http://localhost:8000/")
            r.raise_for_status()
            return r

        # Make a request to block the deployment from accepting other requests.
        request_refs = [do_request.remote()]
        wait_for_condition(
            lambda: ray.get(signal.cur_num_waiters.remote()) == 1, timeout=10
        )

        print("First request is executing.")
        wait_for_condition(
            check_sum_metric_eq,
            timeout=15,
            metric_name="ray_serve_num_ongoing_http_requests",
            expected=1,
        )
        print("ray_serve_num_ongoing_http_requests updated successfully.")

        num_queued_requests = 3
        request_refs.extend([do_request.remote() for _ in range(num_queued_requests)])
        print(f"{num_queued_requests} more requests now queued.")

        # First request should be processing. All others should be queued.
        wait_for_condition(
            check_sum_metric_eq,
            timeout=15,
            metric_name="ray_serve_deployment_queued_queries",
            expected=num_queued_requests,
        )
        print("ray_serve_deployment_queued_queries updated successfully.")
        wait_for_condition(
            check_sum_metric_eq,
            timeout=15,
            metric_name="ray_serve_num_ongoing_http_requests",
            expected=num_queued_requests + 1,
        )
        print("ray_serve_num_ongoing_http_requests updated successfully.")

        # There should be 2 scheduling tasks (which is the max, since
        # 2 = 2 * 1 replica) that are attempting to schedule the hanging requests.
        wait_for_condition(
            check_sum_metric_eq,
            timeout=15,
            metric_name="ray_serve_num_scheduling_tasks",
            expected=2,
        )
        print("ray_serve_num_scheduling_tasks updated successfully.")
        wait_for_condition(
            check_sum_metric_eq,
            timeout=15,
            metric_name="ray_serve_num_scheduling_tasks_in_backoff",
            expected=2,
        )
        print("serve_num_scheduling_tasks_in_backoff updated successfully.")

        # Disconnect all requests by cancelling the Ray tasks.
        [ray.cancel(ref, force=True) for ref in request_refs]
        print("Cancelled all HTTP requests.")

        wait_for_condition(
            check_sum_metric_eq,
            timeout=15,
            metric_name="ray_serve_deployment_queued_queries",
            expected=0,
        )
        print("ray_serve_deployment_queued_queries updated successfully.")

        # Task should get cancelled.
        wait_for_condition(
            check_sum_metric_eq,
            timeout=15,
            metric_name="ray_serve_num_ongoing_http_requests",
            expected=0,
        )
        print("ray_serve_num_ongoing_http_requests updated successfully.")

        wait_for_condition(
            check_sum_metric_eq,
            timeout=15,
            metric_name="ray_serve_num_scheduling_tasks",
            expected=0,
        )
        print("ray_serve_num_scheduling_tasks updated successfully.")
        wait_for_condition(
            check_sum_metric_eq,
            timeout=15,
            metric_name="ray_serve_num_scheduling_tasks_in_backoff",
            expected=0,
        )
        print("serve_num_scheduling_tasks_in_backoff updated successfully.")

        # Unblock hanging request.
        ray.get(signal.send.remote())

    def test_running_requests_gauge(self, serve_start_shutdown):
        signal = SignalActor.options(name="signal123").remote()
        serve.run(
            Router.options(num_replicas=2, ray_actor_options={"num_cpus": 0}).bind(
                [
                    WaitForSignal.options(
                        name="d1",
                        ray_actor_options={"num_cpus": 0},
                        max_ongoing_requests=2,
                        num_replicas=3,
                    ).bind(),
                    WaitForSignal.options(
                        name="d2",
                        ray_actor_options={"num_cpus": 0},
                        max_ongoing_requests=2,
                        num_replicas=3,
                    ).bind(),
                ],
            ),
            name="app1",
        )

        requests_sent = {1: 0, 2: 0}
        for i in range(5):
            index = random.choice([1, 2])
            print(f"Sending request to d{index}")
            call.remote("Router", "app1", index)
            requests_sent[index] += 1

            wait_for_condition(
                check_sum_metric_eq,
                metric_name="ray_serve_num_ongoing_requests_at_replicas",
                tags={"application": "app1", "deployment": "d1"},
                expected=requests_sent[1],
            )

            wait_for_condition(
                check_sum_metric_eq,
                metric_name="ray_serve_num_ongoing_requests_at_replicas",
                tags={"application": "app1", "deployment": "d2"},
                expected=requests_sent[2],
            )

            wait_for_condition(
                check_sum_metric_eq,
                metric_name="ray_serve_num_ongoing_requests_at_replicas",
                tags={"application": "app1", "deployment": "Router"},
                expected=i + 1,
            )

        # Release signal, the number of running requests should drop to 0
        ray.get(signal.send.remote())
        wait_for_condition(
            check_sum_metric_eq,
            metric_name="ray_serve_num_ongoing_requests_at_replicas",
            tags={"application": "app1"},
            expected=0,
        )


def test_long_poll_host_sends_counted(serve_instance):
    """Check that the transmissions by the long_poll are counted."""

    host = ray.remote(LongPollHost).remote(
        listen_for_change_request_timeout_s=(0.01, 0.01)
    )

    # Write a value.
    ray.get(host.notify_changed.remote({"key_1": 999}))
    object_ref = host.listen_for_change.remote({"key_1": -1})

    # Check that the result's size is reported.
    result_1: Dict[str, UpdatedObject] = ray.get(object_ref)
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="serve_long_poll_host_transmission_counter",
        expected=1,
        expected_tags={"namespace_or_state": "key_1"},
    )

    # Write two new values.
    ray.get(host.notify_changed.remote({"key_1": 1000}))
    ray.get(host.notify_changed.remote({"key_2": 1000}))
    object_ref = host.listen_for_change.remote(
        {"key_1": result_1["key_1"].snapshot_id, "key_2": -1}
    )

    # Check that the new objects are transmitted.
    result_2: Dict[str, UpdatedObject] = ray.get(object_ref)
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="serve_long_poll_host_transmission_counter",
        expected=1,
        expected_tags={"namespace_or_state": "key_2"},
    )
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="serve_long_poll_host_transmission_counter",
        expected=2,
        expected_tags={"namespace_or_state": "key_1"},
    )

    # Check that a timeout result is counted.
    object_ref = host.listen_for_change.remote({"key_2": result_2["key_2"].snapshot_id})
    _ = ray.get(object_ref)
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="serve_long_poll_host_transmission_counter",
        expected=1,
        expected_tags={"namespace_or_state": "TIMEOUT"},
    )


def test_actor_summary(serve_instance):
    @serve.deployment
    def f():
        pass

    serve.run(f.bind(), name="app")
    actors = state_api.list_actors(filters=[("state", "=", "ALIVE")])
    class_names = {actor["class_name"] for actor in actors}
    assert class_names.issuperset(
        {"ServeController", "ProxyActor", "ServeReplica:app:f"}
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
