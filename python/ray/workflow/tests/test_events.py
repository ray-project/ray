from ray.tests.conftest import *  # noqa
import pytest

import asyncio
import ray
from ray import workflow
from ray.workflow.tests import utils
import time


def test_sleep(workflow_start_regular_shared):
    @workflow.step
    def sleep_helper():
        @workflow.step
        def after_sleep(sleep_start_time, _):
            return (sleep_start_time, time.time())

        return after_sleep.step(time.time(), workflow.sleep(2))

    start, end = sleep_helper.step().run()
    duration = end - start

    assert 1 < duration < 3


@pytest.mark.parametrize(
    "workflow_start_regular_shared",
    [{
        "num_cpus": 4,  # TODO (Alex): When we switch to the efficient event
        # implementation we shouldn't need these extra cpus.
    }],
    indirect=True)
def test_wait_for_multiple_events(workflow_start_regular_shared):
    """If a workflow has multiple event arguments, it should wait for them at the
    same time.
    """

    class EventListener1(workflow.EventListener):
        async def poll_for_event(self):
            utils.set_global_mark("listener1")
            while not utils.check_global_mark("trigger_event"):
                await asyncio.sleep(0.1)
            return "event1"

    class EventListener2(workflow.EventListener):
        async def poll_for_event(self):
            utils.set_global_mark("listener2")
            while not utils.check_global_mark("trigger_event"):
                await asyncio.sleep(0.1)
            return "event2"

    @workflow.step
    def trivial_step(arg1, arg2):
        return f"{arg1} {arg2}"

    event1_promise = workflow.wait_for_event(EventListener1)
    event2_promise = workflow.wait_for_event(EventListener2)

    print("kicking off running step")
    promise = trivial_step.step(event1_promise, event2_promise).run_async()

    print("polling...")
    while not (utils.check_global_mark("listener1")
               and utils.check_global_mark("listener2")):
        print("[driver] waiting for listeners...")
        print(f"{utils.check_global_mark('listener1')}")
        print(f"{utils.check_global_mark('listener2')}")
        time.sleep(0.1)

    print("setting trigger event now")
    utils.set_global_mark("trigger_event")
    assert ray.get(promise) == "event1 event2"


def test_event_after_arg_resolution(workflow_start_regular_shared):
    """Ensure that a workflow resolves all of its non-event arguments while it
    waiting the the event to occur.
    """

    pass


def test_event_during_arg_resolution(workflow_start_regular_shared):
    """If a workflow's arguments are being executed when the event occurs, the
    workflow should run immediately with no issues.
    """


def test_crash_during_event_checkpointing(workflow_start_regular_shared):
    """Ensure that if the cluster dies while the event is being checkpointed, we
       properly re-poll for the event."""


def test_crash_after_commit(workflow_start_regular_shared):
    """Ensure that we don't re-call poll_for_event after `event_checkpointed`
       returns, even after a crash."""


def test_event_as_workflow(workflow_start_regular_shared):
    pass


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
