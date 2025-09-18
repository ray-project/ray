import os
import signal
import sys
import time
import warnings

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray.util.state import list_tasks


@pytest.mark.skipif(sys.platform == "win32", reason="Fails on windows")
def test_was_current_actor_reconstructed(shutdown_only):
    ray.init()

    @ray.remote(max_restarts=10)
    class A(object):
        def __init__(self):
            self._was_reconstructed = (
                ray.get_runtime_context().was_current_actor_reconstructed
            )

        def get_was_reconstructed(self):
            return self._was_reconstructed

        def update_was_reconstructed(self):
            return ray.get_runtime_context().was_current_actor_reconstructed

        def get_pid(self):
            return os.getpid()

    a = A.remote()
    # `was_reconstructed` should be False when it's called in actor.
    assert ray.get(a.get_was_reconstructed.remote()) is False
    # `was_reconstructed` should be False when it's called in a remote method
    # and the actor never fails.
    assert ray.get(a.update_was_reconstructed.remote()) is False

    pid = ray.get(a.get_pid.remote())
    os.kill(pid, signal.SIGKILL)
    time.sleep(2)
    # These 2 methods should be return True because
    # this actor failed and restored.
    assert ray.get(a.get_was_reconstructed.remote()) is True
    assert ray.get(a.update_was_reconstructed.remote()) is True

    @ray.remote(max_restarts=10)
    class A(object):
        def current_job_id(self):
            return ray.get_runtime_context().get_job_id()

        def current_actor_id(self):
            return ray.get_runtime_context().get_actor_id()

    @ray.remote
    def f():
        assert ray.get_runtime_context().get_actor_id() is None
        assert ray.get_runtime_context().get_task_id() is not None
        assert ray.get_runtime_context().get_node_id() is not None
        assert ray.get_runtime_context().get_job_id() is not None
        context = ray.get_runtime_context().get()
        assert "actor_id" not in context
        assert context["task_id"].hex() == ray.get_runtime_context().get_task_id()
        assert context["node_id"].hex() == ray.get_runtime_context().get_node_id()
        assert context["job_id"].hex() == ray.get_runtime_context().get_job_id()

    a = A.remote()
    assert ray.get(a.current_job_id.remote()) is not None
    assert ray.get(a.current_actor_id.remote()) is not None
    ray.get(f.remote())


def test_get_context_dict(ray_start_regular):
    context_dict = ray.get_runtime_context().get()
    assert context_dict["node_id"] is not None
    assert context_dict["job_id"] is not None
    assert "actor_id" not in context_dict
    assert "task_id" not in context_dict

    @ray.remote
    class Actor:
        def check(self, node_id, job_id):
            context_dict = ray.get_runtime_context().get()
            assert context_dict["node_id"] == node_id
            assert context_dict["job_id"] == job_id
            assert context_dict["actor_id"] is not None
            assert context_dict["task_id"] is not None
            assert context_dict["actor_id"] != "not an ActorID"

    a = Actor.remote()
    ray.get(a.check.remote(context_dict["node_id"], context_dict["job_id"]))

    @ray.remote
    def task(node_id, job_id):
        context_dict = ray.get_runtime_context().get()
        assert context_dict["node_id"] == node_id
        assert context_dict["job_id"] == job_id
        assert context_dict["task_id"] is not None
        assert "actor_id" not in context_dict

    ray.get(task.remote(context_dict["node_id"], context_dict["job_id"]))


def test_current_actor(ray_start_regular):
    @ray.remote
    class Echo:
        def __init__(self):
            pass

        def echo(self, s):
            self_actor = ray.get_runtime_context().current_actor
            return self_actor.echo2.remote(s)

        def echo2(self, s):
            return s

    e = Echo.remote()
    obj = e.echo.remote("hello")
    assert ray.get(ray.get(obj)) == "hello"


def test_get_assigned_resources(ray_start_10_cpus):
    @ray.remote
    class Echo:
        def check(self):
            return ray.get_runtime_context().get_assigned_resources()

    e = Echo.remote()
    result = e.check.remote()
    print(ray.get(result))
    assert ray.get(result).get("CPU") is None
    ray.kill(e)

    e = Echo.options(num_cpus=4).remote()
    result = e.check.remote()
    assert ray.get(result)["CPU"] == 4.0
    ray.kill(e)

    @ray.remote
    def check():
        return ray.get_runtime_context().get_assigned_resources()

    result = check.remote()
    assert ray.get(result)["CPU"] == 1.0

    result = check.options(num_cpus=2).remote()
    assert ray.get(result)["CPU"] == 2.0


# Use default filterwarnings behavior for this test
@pytest.mark.filterwarnings("default")
def test_ids(ray_start_regular):
    rtc = ray.get_runtime_context()
    # node id
    assert isinstance(rtc.get_node_id(), str)
    with warnings.catch_warnings(record=True) as w:
        assert rtc.get_node_id() == rtc.node_id.hex()
        assert any("Use get_node_id() instead" in str(warning.message) for warning in w)

    # job id
    assert isinstance(rtc.get_job_id(), str)
    with warnings.catch_warnings(record=True) as w:
        assert rtc.get_job_id() == rtc.job_id.hex()
        assert any("Use get_job_id() instead" in str(warning.message) for warning in w)

    assert rtc.get_actor_name() is None

    # placement group id
    # Driver doesn't belong to any placement group.
    assert rtc.get_placement_group_id() is None
    pg = ray.util.placement_group(
        name="bar",
        strategy="PACK",
        bundles=[
            {"CPU": 1, "GPU": 0},
        ],
    )
    ray.get(pg.ready())

    @ray.remote
    def foo_pg():
        rtc = ray.get_runtime_context()
        assert isinstance(rtc.get_placement_group_id(), str)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert rtc.get_placement_group_id() == rtc.current_placement_group_id.hex()
            assert any(
                "Use get_placement_group_id() instead" in str(warning.message)
                for warning in w
            )

    ray.get(
        foo_pg.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
        ).remote()
    )
    ray.util.remove_placement_group(pg)

    # task id
    assert rtc.get_task_id() is None

    @ray.remote
    def foo_task():
        rtc = ray.get_runtime_context()
        assert isinstance(rtc.get_task_id(), str)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert rtc.get_task_id() == rtc.task_id.hex()
            assert any(
                "Use get_task_id() instead" in str(warning.message) for warning in w
            )

    ray.get(foo_task.remote())

    # actor id
    assert rtc.get_actor_id() is None

    @ray.remote
    class FooActor:
        def foo(self):
            rtc = ray.get_runtime_context()
            assert isinstance(rtc.get_actor_id(), str)
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                assert rtc.get_actor_id() == rtc.actor_id.hex()
                assert any(
                    "Use get_actor_id() instead" in str(warning.message)
                    for warning in w
                )

    actor = FooActor.remote()
    ray.get(actor.foo.remote())

    # actor name
    @ray.remote
    class NamedActor:
        def name(self):
            return ray.get_runtime_context().get_actor_name()

    ACTOR_NAME = "actor_name"
    named_actor = NamedActor.options(name=ACTOR_NAME).remote()
    assert ray.get(named_actor.name.remote()) == ACTOR_NAME

    # unnamed actor name
    unnamed_actor = NamedActor.options().remote()
    assert ray.get(unnamed_actor.name.remote()) == ""

    # task actor name
    @ray.remote
    def task_actor_name():
        ray.get_runtime_context().get_actor_name()

    assert ray.get(task_actor_name.remote()) is None

    # driver actor name
    assert rtc.get_actor_name() is None


def test_auto_init(shutdown_only):
    assert not ray.is_initialized()
    ray.get_runtime_context()
    assert ray.is_initialized()


def test_get_task_name(shutdown_only):
    ray.init()

    # for a normal task
    @ray.remote
    def get_task_name_for_normal_task():
        return ray.get_runtime_context().get_task_name()

    expected_task_name = "normal_task_name"
    task_name = ray.get(
        get_task_name_for_normal_task.options(name=expected_task_name).remote()
    )
    assert (
        task_name == expected_task_name
    ), f"Check normal task name failed. expected={expected_task_name}, \
actual={task_name}"

    # for an actor task
    @ray.remote
    class Actor:
        def get_task_name_for_actor_task(self):
            return ray.get_runtime_context().get_task_name()

    expected_task_name = "Actor.get_task_name_for_actor_task"
    actor = Actor.remote()
    task_name = ray.get(actor.get_task_name_for_actor_task.remote())
    assert (
        task_name == expected_task_name
    ), f"Check actor task name failed. expected={expected_task_name}, \
actual={task_name}"

    # for a threaded actor task
    @ray.remote
    class ThreadedActor:
        def get_task_name_for_threaded_actor_task(self):
            return ray.get_runtime_context().get_task_name()

    expected_task_name = "ThreadedActor.get_task_name_for_threaded_actor_task"
    threaded_actor = ThreadedActor.options(max_concurrency=2).remote()
    task_name = ray.get(threaded_actor.get_task_name_for_threaded_actor_task.remote())
    assert (
        task_name == expected_task_name
    ), f"Check actor task name failed. expected={expected_task_name}, \
actual={task_name}"

    # for a async actor task
    @ray.remote
    class AsyncActor:
        async def get_task_name_for_async_actor_task(self):
            return ray.get_runtime_context().get_task_name()

    expected_task_name = "AsyncActor.get_task_name_for_async_actor_task"
    async_actor = AsyncActor.remote()
    task_name = ray.get(async_actor.get_task_name_for_async_actor_task.remote())
    assert (
        task_name == expected_task_name
    ), f"Check actor task name failed. expected={expected_task_name}, \
actual={task_name}"


def test_get_task_function_name(shutdown_only):
    ray.init()

    # for a normal task
    @ray.remote
    def get_task_function_name_for_normal_task():
        return ray.get_runtime_context().get_task_function_name()

    expected_task_function_name = __name__ + ".get_task_function_name_for_normal_task"
    task_function_name = ray.get(get_task_function_name_for_normal_task.remote())
    assert (
        task_function_name == expected_task_function_name
    ), f"Check normal task function failed. expected={expected_task_function_name}, \
actual={task_function_name}"

    # for an actor task
    @ray.remote
    class Actor:
        def get_task_function_name_for_actor_task(self):
            return ray.get_runtime_context().get_task_function_name()

    expected_task_function_name = (
        __name__ + ".Actor.get_task_function_name_for_actor_task"
    )
    actor = Actor.remote()
    task_function_name = ray.get(actor.get_task_function_name_for_actor_task.remote())
    assert (
        task_function_name == expected_task_function_name
    ), f"Check actor task function failed. expected={expected_task_function_name}, \
actual={task_function_name}"

    # for a threaded actor task
    @ray.remote
    class ThreadedActor:
        def get_task_function_name_for_threaded_actor_task(self):
            return ray.get_runtime_context().get_task_function_name()

    expected_task_function_name = (
        __name__ + ".ThreadedActor.get_task_function_name_for_threaded_actor_task"
    )
    threaded_actor = ThreadedActor.options(max_concurrency=2).remote()
    task_function_name = ray.get(
        threaded_actor.get_task_function_name_for_threaded_actor_task.remote()
    )
    assert (
        task_function_name == expected_task_function_name
    ), f"Check actor task function failed. expected={expected_task_function_name}, \
actual={task_function_name}"

    # for a async actor task
    @ray.remote
    class AsyncActor:
        async def get_task_function_name_for_async_actor_task(self):
            return ray.get_runtime_context().get_task_function_name()

    expected_task_function_name = (
        __name__
        + ".test_get_task_function_name.<locals>.AsyncActor.\
get_task_function_name_for_async_actor_task"
    )
    async_actor = AsyncActor.remote()
    task_function_name = ray.get(
        async_actor.get_task_function_name_for_async_actor_task.remote()
    )
    assert (
        task_function_name == expected_task_function_name
    ), f"Check actor task function failed. expected={expected_task_function_name}, \
actual={task_function_name}"


def test_async_actor_task_id(shutdown_only):
    ray.init()

    @ray.remote
    class A:
        async def f(self):
            task_id = ray.get_runtime_context().get_task_id()
            return task_id

    a = A.remote()
    task_id = ray.get(a.f.remote())

    def verify():
        tasks = list_tasks(filters=[("name", "=", "A.f")])
        assert len(tasks) == 1
        assert tasks[0].task_id == task_id
        return True

    wait_for_condition(verify)


def test_get_node_labels(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    cluster.add_node(
        resources={"worker1": 1},
        num_cpus=1,
        labels={
            "ray.io/accelerator-type": "A100",
            "ray.io/availability-region": "us-west4",
            "ray.io/market-type": "spot",
        },
    )

    @ray.remote
    class Actor:
        def get_node_id(self):
            return ray.get_runtime_context().get_node_id()

        def get_node_labels(self):
            return ray.get_runtime_context().get_node_labels()

    expected_node_labels = {
        "ray.io/accelerator-type": "A100",
        "ray.io/availability-region": "us-west4",
        "ray.io/market-type": "spot",
    }

    # Check node labels from Actor runtime context
    a = Actor.options(label_selector={"ray.io/accelerator-type": "A100"}).remote()
    node_labels = ray.get(a.get_node_labels.remote())
    expected_node_labels["ray.io/node-id"] = ray.get(a.get_node_id.remote())
    assert expected_node_labels == node_labels

    # Check node labels from driver runtime context (none are set except default)
    driver_labels = ray.get_runtime_context().get_node_labels()
    assert {"ray.io/node-id": ray.get_runtime_context().get_node_id()} == driver_labels


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
