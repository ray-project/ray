from time import sleep
import pytest

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
