import asyncio
import subprocess
import time

import pytest

import ray
from ray import workflow
from ray.tests.conftest import *  # noqa
from ray.workflow.tests import utils


def test_sleep(workflow_start_regular_shared):
    @ray.remote
    def after_sleep(sleep_start_time, _):
        return sleep_start_time, time.time()

    @ray.remote
    def sleep_helper():
        return workflow.continuation(after_sleep.bind(time.time(), workflow.sleep(2)))

    start, end = workflow.create(sleep_helper.bind()).run()
    duration = end - start

    assert 1 < duration


def test_sleep_checkpointing(workflow_start_regular_shared):
    """Test that the workflow sleep only starts after `run` not when the step is
    defined."""
    sleep_step = workflow.sleep(2)
    time.sleep(2)
    start_time = time.time()
    workflow.create(sleep_step).run()
    end_time = time.time()
    duration = end_time - start_time
    assert 1 < duration


@pytest.mark.parametrize(
    "workflow_start_regular_shared",
    [
        {
            "num_cpus": 4,  # TODO (Alex): When we switch to the efficient event
            # implementation we shouldn't need these extra cpus.
        }
    ],
    indirect=True,
)
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

    @ray.remote
    def trivial_step(arg1, arg2):
        return f"{arg1} {arg2}"

    event1_promise = workflow.wait_for_event(EventListener1)
    event2_promise = workflow.wait_for_event(EventListener2)

    promise = workflow.create(
        trivial_step.bind(event1_promise, event2_promise)
    ).run_async()

    while not (
        utils.check_global_mark("listener1") and utils.check_global_mark("listener2")
    ):
        time.sleep(0.1)

    utils.set_global_mark("trigger_event")
    assert ray.get(promise) == "event1 event2"


@pytest.mark.parametrize(
    "workflow_start_regular_shared",
    [
        {
            "num_cpus": 4,  # TODO (Alex): When we switch to the efficient event
            # implementation we shouldn't need these extra cpus.
        }
    ],
    indirect=True,
)
def test_event_after_arg_resolution(workflow_start_regular_shared):
    """Ensure that a workflow resolves all of its non-event arguments while it
    waiting the the event to occur.
    """

    class MyEventListener(workflow.EventListener):
        async def poll_for_event(self):
            while not utils.check_global_mark():
                await asyncio.sleep(0.1)
            # Give the other step time to finish.
            await asyncio.sleep(1)

    @ray.remote
    def triggers_event():
        utils.set_global_mark()

    @ray.remote
    def gather(*args):
        return args

    event_promise = workflow.wait_for_event(MyEventListener)

    assert workflow.create(gather.bind(event_promise, triggers_event.bind())).run() == (
        None,
        None,
    )


@pytest.mark.parametrize(
    "workflow_start_regular_shared",
    [
        {
            "num_cpus": 4,  # TODO (Alex): When we switch to the efficient event
            # implementation we shouldn't need these extra cpus.
        }
    ],
    indirect=True,
)
def test_event_during_arg_resolution(workflow_start_regular_shared):
    """If a workflow's arguments are being executed when the event occurs, the
    workflow should run immediately with no issues.
    """

    class MyEventListener(workflow.EventListener):
        async def poll_for_event(self):
            while not utils.check_global_mark():
                await asyncio.sleep(0.1)
            utils.set_global_mark("event_returning")

    @ray.remote
    def triggers_event():
        utils.set_global_mark()
        while not utils.check_global_mark("event_returning"):
            time.sleep(0.1)

    @ray.remote
    def gather(*args):
        return args

    event_promise = workflow.wait_for_event(MyEventListener)
    assert workflow.create(gather.bind(event_promise, triggers_event.bind())).run() == (
        None,
        None,
    )


def test_crash_during_event_checkpointing(workflow_start_regular_shared):
    """Ensure that if the cluster dies while the event is being checkpointed, we
    properly re-poll for the event."""

    from ray._private import storage

    storage_uri = storage._storage_uri

    """Ensure that we don't re-call poll_for_event after `event_checkpointed`
       returns, even after a crash."""

    class MyEventListener(workflow.EventListener):
        async def poll_for_event(self):
            assert not utils.check_global_mark("committed")
            if utils.check_global_mark("first"):
                utils.set_global_mark("second")
            utils.set_global_mark("first")

            utils.set_global_mark("time_to_die")
            while not utils.check_global_mark("resume"):
                time.sleep(0.1)

        async def event_checkpointed(self, event):
            utils.set_global_mark("committed")

    @ray.remote
    def wait_then_finish(arg):
        pass

    event_promise = workflow.wait_for_event(MyEventListener)
    workflow.create(wait_then_finish.bind(event_promise)).run_async("workflow")

    while not utils.check_global_mark("time_to_die"):
        time.sleep(0.1)

    assert utils.check_global_mark("first")
    ray.shutdown()
    subprocess.check_output(["ray", "stop", "--force"])

    # Give the workflow some time to kill the cluster.
    # time.sleep(3)

    ray.init(num_cpus=4, storage=storage_uri)
    workflow.init()
    workflow.resume("workflow")
    utils.set_global_mark("resume")

    ray.get(workflow.get_output("workflow"))
    assert utils.check_global_mark("second")


@pytest.mark.parametrize(
    "workflow_start_regular_shared",
    [
        {
            "num_cpus": 4,  # TODO (Alex): When we switch to the efficient event
            # implementation we shouldn't need these extra cpus.
        }
    ],
    indirect=True,
)
def test_crash_after_commit(workflow_start_regular_shared):
    """Ensure that we don't re-call poll_for_event after `event_checkpointed`
    returns, even after a crash. Here we must call `event_checkpointed`
    twice, because there's no way to know if we called it after
    checkpointing.
    """

    from ray._private import storage

    storage_uri = storage._storage_uri

    class MyEventListener(workflow.EventListener):
        async def poll_for_event(self):
            assert not utils.check_global_mark("committed")

        async def event_checkpointed(self, event):
            utils.set_global_mark("committed")
            if utils.check_global_mark("first"):
                utils.set_global_mark("second")
            else:
                utils.set_global_mark("first")
                await asyncio.sleep(1000000)

    event_promise = workflow.wait_for_event(MyEventListener)
    workflow.create(event_promise).run_async("workflow")

    while not utils.check_global_mark("first"):
        time.sleep(0.1)

    ray.shutdown()
    subprocess.check_output(["ray", "stop", "--force"])

    ray.init(num_cpus=4, storage=storage_uri)
    workflow.init()
    workflow.resume("workflow")

    ray.get(workflow.get_output("workflow"))
    assert utils.check_global_mark("second")


@pytest.mark.parametrize(
    "workflow_start_regular_shared",
    [
        {
            "num_cpus": 4,  # TODO (Alex): When we switch to the efficient event
            # implementation we shouldn't need these extra cpus.
        }
    ],
    indirect=True,
)
def test_event_as_workflow(workflow_start_regular_shared):
    class MyEventListener(workflow.EventListener):
        async def poll_for_event(self):
            while not utils.check_global_mark():
                await asyncio.sleep(1)

    utils.unset_global_mark()
    promise = workflow.create(workflow.wait_for_event(MyEventListener)).run_async("wf")

    assert workflow.get_status("wf") == workflow.WorkflowStatus.RUNNING

    utils.set_global_mark()
    assert ray.get(promise) is None


@pytest.mark.parametrize(
    "workflow_start_regular_shared",
    [
        {
            "num_cpus": 4,  # TODO (Alex): When we switch to the efficient event
            # implementation we shouldn't need these extra cpus.
        }
    ],
    indirect=True,
)
def test_types(workflow_start_regular_shared):
    class NotAnEventListener:
        pass

    with pytest.raises(TypeError):
        workflow.wait_for_event(NotAnEventListener)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
