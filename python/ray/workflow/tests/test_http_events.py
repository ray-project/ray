from time import sleep
import pytest

import ray
from ray import workflow, serve
from ray.workflow.http_event_provider import HTTPListener
from ray.tests.conftest import *  # noqa

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
    """This test has a statically declared event workflow task,
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
        event_task = workflow.wait_for_event(HTTPListener, event_key="event_key")
        return workflow.continuation(event_task)

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
