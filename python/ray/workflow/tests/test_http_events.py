from time import sleep
import pytest

import ray
from ray import workflow, serve
from ray.workflow.http_event_provider import HTTPListener
from ray.tests.conftest import *  # noqa
from ray.workflow.tests import utils

import requests


@pytest.mark.parametrize(
    "workflow_start_regular_shared_serve",
    [
        {
            "num_cpus": 4,  # TODO (Alex): When we switch to the efficient event
            # implementation we shouldn't need these extra cpus.
        }
    ],
    indirect=True,
)
def test_receive_event_by_http(workflow_start_regular_shared_serve):
    """This test has a statically declared event workflow step,
    receiving one externally posted message to a Ray Serve endpoint.
    """

    def send_event():
        resp = requests.post(
            "http://127.0.0.1:8000/event/send_event/"
            + "workflow_test_receive_event_by_http",
            json={"event_key": "event_key", "event_payload": "event_message"},
        )
        return resp

    event_promise = workflow.wait_for_event(HTTPListener, event_key="event_key")
    workflow.run_async(event_promise, workflow_id="workflow_test_receive_event_by_http")

    # wait until HTTPEventProvider is ready
    while len(serve.list_deployments().keys()) < 1:
        sleep(0.1)

    # repeat send_event() until the returned status code is not 404
    while True:
        res = send_event()
        if res.status_code == 404:
            sleep(0.5)
        else:
            break

    key, event_msg = workflow.get_output(
        workflow_id="workflow_test_receive_event_by_http"
    )

    assert event_msg == "event_message"


@pytest.mark.parametrize(
    "workflow_start_regular_shared_serve",
    [
        {
            "num_cpus": 4,  # TODO (Alex): When we switch to the efficient event
            # implementation we shouldn't need these extra cpus.
        }
    ],
    indirect=True,
)
def test_dynamic_event_by_http(workflow_start_regular_shared_serve):
    """If a workflow has dynamically generated event arguments, it should
    return the event as if the event was declared statically.
    """

    def send_event():
        resp = requests.post(
            "http://127.0.0.1:8000/event/send_event/"
            + "workflow_test_dynamic_event_by_http",
            json={"event_key": "event_key", "event_payload": "event_message_dynamic"},
        )
        return resp

    @ray.remote
    def return_dynamically_generated_event():
        event_step = workflow.wait_for_event(HTTPListener, event_key="event_key")
        return workflow.continuation(event_step)

    workflow.run_async(
        return_dynamically_generated_event.bind(),
        workflow_id="workflow_test_dynamic_event_by_http",
    )

    # wait until HTTPEventProvider is ready
    while len(serve.list_deployments().keys()) < 1:
        sleep(0.1)

    # repeat send_event() until the returned status code is not 404
    while True:
        res = send_event()
        if res.status_code == 404:
            sleep(0.5)
        else:
            break

    key, event_msg = workflow.get_output(
        workflow_id="workflow_test_dynamic_event_by_http"
    )

    assert event_msg == "event_message_dynamic"


@pytest.mark.parametrize(
    "workflow_start_regular_shared_serve",
    [
        {
            "num_cpus": 4,  # TODO (Alex): When we switch to the efficient event
            # implementation we shouldn't need these extra cpus.
        }
    ],
    indirect=True,
)
def test_multiple_events_by_http(workflow_start_regular_shared_serve):
    """If a workflow has multiple event arguments, it should wait for them at the
    same time.
    """

    def send_event1():
        resp = requests.post(
            "http://127.0.0.1:8000/event/send_event/"
            + "workflow_test_multiple_event_by_http",
            json={"event_key": "e1", "event_payload": "hello"},
        )
        return resp

    def send_event2():
        sleep(0.5)
        resp = requests.post(
            "http://127.0.0.1:8000/event/send_event/"
            + "workflow_test_multiple_event_by_http",
            json={"event_key": "e2", "event_payload": "world"},
        )
        return resp

    @ray.remote
    def trivial_step(arg1, arg2):
        return f"{arg1[1]} {arg2[1]}"

    event1_promise = workflow.wait_for_event(HTTPListener, event_key="e1")
    event2_promise = workflow.wait_for_event(HTTPListener, event_key="e2")
    workflow.run_async(
        trivial_step.bind(event1_promise, event2_promise),
        workflow_id="workflow_test_multiple_event_by_http",
    )

    # wait until HTTPEventProvider is ready
    while len(serve.list_deployments().keys()) < 1:
        sleep(0.1)

    # repeat send_event1() until the returned status code is not 404
    while True:
        res = send_event1()
        if res.status_code == 404:
            sleep(0.5)
        else:
            break

    while True:
        res = send_event2()
        if res.status_code == 404:
            sleep(0.5)
        else:
            break

    event_msg = workflow.get_output(workflow_id="workflow_test_multiple_event_by_http")

    assert event_msg == "hello world"


@pytest.mark.parametrize(
    "workflow_start_regular_shared_serve",
    [
        {
            "num_cpus": 4,  # TODO (Alex): When we switch to the efficient event
            # implementation we shouldn't need these extra cpus.
        }
    ],
    indirect=True,
)
def test_checkpoint_success_by_http(workflow_start_regular_shared_serve):
    """If the checkpoint succeeded, the HTTP client receives response code 200."""

    class CustomHTTPListener(HTTPListener):
        async def event_checkpointed(self, event):
            key, msg = event
            from ray.workflow import workflow_context

            if utils.check_global_mark("checkpointing_succeed"):
                await self.handle.report_checkpointed.remote(
                    workflow_context.get_current_workflow_id(), key, True
                )
            if utils.check_global_mark("checkpointing_failed"):
                await self.handle.report_checkpointed.remote(
                    workflow_context.get_current_workflow_id(), key, False
                )

    def send_event(msg):
        resp = requests.post(
            "http://127.0.0.1:8000/event/send_event/"
            + "workflow_test_checkpoint_success_by_http",
            json={"event_key": "event_key", "event_payload": msg},
        )
        return resp

    utils.set_global_mark("checkpointing_succeed")
    event_promise = workflow.wait_for_event(CustomHTTPListener, event_key="event_key")
    workflow.run_async(
        event_promise, workflow_id="workflow_test_checkpoint_success_by_http"
    )

    # wait until HTTPEventProvider is ready
    while len(serve.list_deployments().keys()) < 1:
        sleep(0.1)

    test_msg = "new_event_message"

    while True:
        res = send_event(test_msg)
        if res.status_code == 404:
            sleep(0.5)
        else:
            break

    assert res.status_code == 200


@pytest.mark.parametrize(
    "workflow_start_regular_shared_serve",
    [
        {
            "num_cpus": 4,  # TODO (Alex): When we switch to the efficient event
            # implementation we shouldn't need these extra cpus.
        }
    ],
    indirect=True,
)
def test_checkpoint_failed_by_http(workflow_start_regular_shared_serve):
    """If the checkpoint failed, the HTTP client receives response code 500."""

    class CustomHTTPListener(HTTPListener):
        async def event_checkpointed(self, event):
            key, msg = event
            from ray.workflow import workflow_context

            if utils.check_global_mark("checkpointing_succeed"):
                await self.handle.report_checkpointed.remote(
                    workflow_context.get_current_workflow_id(), key, True
                )
            if utils.check_global_mark("checkpointing_failed"):
                await self.handle.report_checkpointed.remote(
                    workflow_context.get_current_workflow_id(), key, False
                )

    def send_event(msg):
        resp = requests.post(
            "http://127.0.0.1:8000/event/send_event/"
            + "workflow_test_checkpoint_failed_by_http",
            json={"event_key": "event_key", "event_payload": msg},
        )
        return resp

    utils.set_global_mark("checkpointing_failed")
    event_promise = workflow.wait_for_event(CustomHTTPListener, event_key="event_key")
    workflow.run_async(
        event_promise, workflow_id="workflow_test_checkpoint_failed_by_http"
    )

    # wait until HTTPEventProvider is ready
    while len(serve.list_deployments().keys()) < 1:
        sleep(0.1)

    test_msg = "new_event_message"

    while True:
        res = send_event(test_msg)
        if res.status_code == 404:
            sleep(0.5)
        else:
            break

    assert res.status_code == 500


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
