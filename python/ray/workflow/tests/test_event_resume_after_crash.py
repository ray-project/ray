import asyncio
import subprocess

from time import sleep
import pytest

import ray
from ray import workflow, serve
from ray.workflow.http_event_provider import HTTPListener, WorkflowEventHandleError
from ray.tests.conftest import *  # noqa
from ray.workflow.tests import utils
from ray.workflow.common import WorkflowStatus
from ray.workflow import common, workflow_context

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
def test_cluster_crash_before_checkpoint(workflow_start_regular_shared_serve):
    """If the cluster crashed before the event was checkpointed, after the cluster restarted
    and the workflow resumed, the new event message is processed by the workflow.
    """

    class CustomHTTPListener(HTTPListener):
        async def poll_for_event(self, event_key):
            workflow_id = workflow_context.get_current_workflow_id()
            if event_key is None:
                raise WorkflowEventHandleError(
                    workflow_id, "poll_for_event() needs event_key"
                )
            payload = await self.handle.get_event_payload.remote(workflow_id, event_key)

            if utils.check_global_mark("after_cluster_restarted"):
                return payload
            else:
                utils.set_global_mark("simulate_cluster_crash")
                await asyncio.sleep(10000)

    from ray._private import storage
    from ray.workflow.tests.utils import skip_client_mode_test

    storage_uri = storage._storage_uri

    # This test restarts the cluster, so we cannot test under client mode.
    skip_client_mode_test()

    def send_event(msg):
        try:
            resp = requests.post(
                "http://127.0.0.1:8000/event/send_event/"
                + "workflow_test_cluster_crash_before_checkpoint",
                json={"event_key": "event_key", "event_payload": msg},
                timeout=5,
            )
            return resp
        except requests.Timeout:
            return 500

    event_promise = workflow.wait_for_event(CustomHTTPListener, event_key="event_key")
    workflow.run_async(
        event_promise, workflow_id="workflow_test_cluster_crash_before_checkpoint"
    )

    # wait until HTTPEventProvider is ready
    while len(serve.list_deployments().keys()) < 1:
        sleep(0.1)

    test_msg = "first_try"

    while True:
        res = send_event(test_msg)
        if not isinstance(res, int):
            if res.status_code == 404:
                sleep(0.5)
            else:
                break
        else:
            break

    while not utils.check_global_mark("simulate_cluster_crash"):
        sleep(0.1)

    if utils.check_global_mark("simulate_cluster_crash"):

        serve.get_deployment(common.HTTP_EVENT_PROVIDER_NAME).delete()
        serve.shutdown()
        ray.shutdown()
        subprocess.check_output(["ray", "stop", "--force"])

        ray.init(num_cpus=4, storage=storage_uri)
        serve.start(detached=True)
        utils.set_global_mark("after_cluster_restarted")

        workflow.resume_async(
            workflow_id="workflow_test_cluster_crash_before_checkpoint"
        )
        status_after_resume = workflow.get_status(
            workflow_id="workflow_test_cluster_crash_before_checkpoint"
        )

        while len(serve.list_deployments().keys()) < 1:
            sleep(0.1)

        assert status_after_resume == WorkflowStatus.RUNNING

        test_msg = "second_try"

        while True:
            res = send_event(test_msg)
            if not isinstance(res, int):
                if res.status_code == 404:
                    sleep(0.5)
                else:
                    break
            else:
                break

        key, event_message = workflow.get_output(
            workflow_id="workflow_test_cluster_crash_before_checkpoint"
        )
        assert event_message == "second_try"

    else:
        assert False


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
