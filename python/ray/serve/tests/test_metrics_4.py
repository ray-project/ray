import random
import sys
from typing import Dict

import pytest
import requests

import ray
import ray.util.state as state_api
from ray import serve
from ray._private.test_utils import (
    SignalActor,
    wait_for_condition,
)
from ray.serve._private.long_poll import LongPollHost, UpdatedObject
from ray.serve.handle import DeploymentHandle
from ray.serve.tests.test_metrics import check_metric_float_eq, check_sum_metric_eq


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
