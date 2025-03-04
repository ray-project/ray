import os
import pytest
import sys

import ray
from ray._private.test_utils import (
    wait_for_condition,
)
from ray.util.state import list_tasks


@pytest.mark.parametrize(
    "actor_enable_task_events",
    [True, False, None],
    ids=["actor_true", "actor_false", "actor_none"],
)
@pytest.mark.parametrize(
    "actor_method_enable_task_events",
    [True, False, None],
    ids=["actor_method_true", "actor_method_false", "actor_method_none"],
)
@pytest.mark.parametrize(
    "system_enable_task_events",
    [True, False, None],
    ids=["system_true", "system_false", "system_none"],
)
def test_enable_task_events_actor(
    actor_enable_task_events,
    actor_method_enable_task_events,
    system_enable_task_events,
    shutdown_only,
):
    """
    Test that task events is enabled/disabled from the actor's options.

    - If actor sets enable_task_events=True, all tasks from the actor should be traced.
    - If actor sets enable_task_events=False, all tasks from the actor should not be
        traced by default.
        - But it can be traced if the task explicitly sets enable_task_events=True.
    - If actor does not set enable_task_events, it should be traced by default.

    """
    system_configs = {
        "task_events_report_interval_ms": 100,
        "metrics_report_interval_ms": 200,
        "enable_timeline": False,
    }

    if system_enable_task_events is not None:
        # system_configs["enable_task_events"] = system_enable_task_events
        if system_enable_task_events is False:
            system_configs["task_events_report_interval_ms"] = 0

    ray.init(
        num_cpus=1,
        _system_config=system_configs,
    )

    @ray.remote
    def f():
        pass

    @ray.remote(num_cpus=0)
    class Actor:
        def f(self):
            # This should always be traced.
            ray.get(f.options(name="inner-task-traced").remote())

        @ray.method(
            enable_task_events=(
                actor_method_enable_task_events
                if actor_method_enable_task_events is not None
                else actor_enable_task_events  # We don't allow None defaults
            )
        )
        def g(self):
            pass

    if actor_enable_task_events is not None:
        a = Actor.options(enable_task_events=actor_enable_task_events).remote()
    else:
        a = Actor.remote()

    if actor_method_enable_task_events is not None:
        ray.get(
            a.f.options(enable_task_events=actor_method_enable_task_events).remote()
        )
    else:
        ray.get(a.f.remote())

    ray.get(a.g.remote())

    expected_tasks_traced = set()
    if system_enable_task_events is not False:
        expected_tasks_traced = {"inner-task-traced"}

    if actor_enable_task_events is not False and system_enable_task_events is not False:
        expected_tasks_traced.add("Actor.__init__")
        expected_tasks_traced.add("Actor.f")
        expected_tasks_traced.add("Actor.g")

    if (
        actor_method_enable_task_events is True
        and system_enable_task_events is not False
    ):
        expected_tasks_traced.add("Actor.f")
        expected_tasks_traced.add("Actor.g")

    if actor_method_enable_task_events is False:
        if "Actor.f" in expected_tasks_traced:
            expected_tasks_traced.remove("Actor.f")
        if "Actor.g" in expected_tasks_traced:
            expected_tasks_traced.remove("Actor.g")

    def verify():
        tasks = list_tasks()

        assert len(tasks) == len(expected_tasks_traced)
        assert {t["name"] for t in tasks} == expected_tasks_traced

        return True

    wait_for_condition(verify)


@pytest.mark.parametrize(
    "actor_enable_task_events",
    [True, False, None],
    ids=["actor_true", "actor_false", "actor_none"],
)
@pytest.mark.parametrize(
    "remote_task_enable_task_events",
    [True, False, None],
    ids=["remote_task_true", "remote_task_false", "remote_task_none"],
)
def test_enable_task_events_remote_actor(
    actor_enable_task_events, remote_task_enable_task_events, shutdown_only
):
    @ray.remote
    class Actor:
        def f(self):
            pass

    @ray.remote
    def task(actor):
        actor.f.remote()

    if actor_enable_task_events is not None:
        a = Actor.options(enable_task_events=actor_enable_task_events).remote()
    else:
        a = Actor.remote()

    if remote_task_enable_task_events is not None:
        task = task.options(enable_task_events=remote_task_enable_task_events)

    ray.get(task.remote(a))

    expected_tasks = set()
    if actor_enable_task_events is not False:
        expected_tasks.add("Actor.f")
        expected_tasks.add("Actor.__init__")

    if remote_task_enable_task_events is not False:
        expected_tasks.add("task")

    def verify():
        tasks = list_tasks()
        assert len(tasks) == len(expected_tasks)
        assert {t["name"] for t in tasks} == expected_tasks
        return True

    wait_for_condition(verify)


def test_enable_task_events_invalid_options(shutdown_only):
    """
    Test the invalid values for the option.
    """

    @ray.remote
    def f():
        pass

    @ray.remote
    class Actor:
        pass

    with pytest.raises(TypeError):
        ray.get(f.options(enable_task_events="invalid").remote())

    with pytest.raises(TypeError):
        ray.get(f.options(enable_task_events=None).remote())

    with pytest.raises(TypeError):
        ray.get(Actor.options(enable_task_events="invalid").remote())

    with pytest.raises(TypeError):
        ray.get(Actor.options(enable_task_events=None).remote())


@pytest.mark.parametrize(
    "enable_task_events", [True, False], ids=["task_enable", "task_disable"]
)
@pytest.mark.parametrize(
    "system_enable_task_events",
    [True, False, None],
    ids=["system_true", "system_false", "system_none"],
)
def test_enable_task_events_tasks(
    enable_task_events, system_enable_task_events, shutdown_only
):
    system_config = {
        "task_events_report_interval_ms": 100,
        "metrics_report_interval_ms": 200,
        "enable_timeline": False,
    }
    if system_enable_task_events is not None:
        # system_config["enable_task_events"] = system_enable_task_events
        if system_enable_task_events is False:
            system_config["task_events_report_interval_ms"] = 0

    ray.init(
        num_cpus=1,
        _system_config=system_config,
    )

    @ray.remote
    def traced():
        pass

    @ray.remote
    def f():
        ray.get(traced.remote())

    @ray.remote(enable_task_events=enable_task_events)
    def g():
        pass

    ray.get([f.options(enable_task_events=enable_task_events).remote(), g.remote()])

    expected_tasks_traced = set()
    if system_enable_task_events is not False:
        expected_tasks_traced.add("traced")

    if enable_task_events is not False and system_enable_task_events is not False:
        expected_tasks_traced.add("f")
        expected_tasks_traced.add("g")

    def verify():
        tasks = list_tasks()
        assert len(tasks) == len(expected_tasks_traced)
        assert {t["name"] for t in tasks} == expected_tasks_traced
        return True

    wait_for_condition(verify)


@pytest.mark.parametrize("inner_enable_task_events", [True, False])
@pytest.mark.parametrize("outer_enable_task_events", [True, False])
def test_enable_task_events_nested_actor(
    shutdown_only, inner_enable_task_events, outer_enable_task_events
):
    """
    Test that enable_task_events options are independent of each other for
    nested actors.
    """
    ray.init(
        num_cpus=1,
        _system_config={
            "task_events_report_interval_ms": 100,
            "metrics_report_interval_ms": 200,
            "enable_timeline": False,
        },
    )

    @ray.remote(enable_task_events=inner_enable_task_events)
    class A:
        def f(self):
            pass

    @ray.remote(enable_task_events=outer_enable_task_events)
    class B:
        def __init__(self):
            self.a = A.remote()

        def g(self):
            ray.get(self.a.f.remote())

    b = B.remote()
    ray.get(b.g.remote())
    expected_tasks_traced = set()
    if inner_enable_task_events is not False:
        expected_tasks_traced.add("A.f")
        expected_tasks_traced.add("A.__init__")

    if outer_enable_task_events is not False:
        expected_tasks_traced.add("B.g")
        expected_tasks_traced.add("B.__init__")

    def verify():
        tasks = list_tasks()
        assert len(tasks) == len(expected_tasks_traced)
        assert {t["name"] for t in tasks} == expected_tasks_traced
        return True

    wait_for_condition(verify)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
