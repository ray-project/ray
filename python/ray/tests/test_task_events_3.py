import sys

import pytest

import ray
from ray._common.test_utils import (
    wait_for_condition,
)
from ray.util.state import list_tasks


def test_globally_disable_task_events(
    shutdown_only,
):
    """Test that globally disabled task events overrides per-actor/task options."""
    system_config = {
        "task_events_report_interval_ms": 0,
        "metrics_report_interval_ms": 200,
        "enable_timeline": False,
    }
    ray.init(
        num_cpus=1,
        _system_config=system_config,
    )

    @ray.remote(enable_task_events=True)
    def noop():
        pass

    @ray.remote(num_cpus=0, enable_task_events=True)
    class Actor:
        def f(self):
            # This should always be traced.
            ray.get(noop.options(name="inner-task-traced").remote())

        @ray.method(enable_task_events=True)
        def g(self):
            pass

    a = Actor.remote()

    for _ in range(10):
        ray.get(a.f.remote())
        ray.get(a.g.remote())

    assert len(list_tasks()) == 0


def test_enable_task_events_actor(
    shutdown_only,
):
    """Test enabling/disabling actor task events via actor options.

    - enable_task_events=True should enable all tasks from the actor.
    - enable_task_events=False should disable all tasks from the actor.
    - The above can be overridden per method.
    """
    system_config = {
        "task_events_report_interval_ms": 100,
        "metrics_report_interval_ms": 200,
        "enable_timeline": False,
    }

    ray.init(
        num_cpus=1,
        _system_config=system_config,
    )

    @ray.remote(num_cpus=0)
    class A:
        def default_method(self):
            pass

        @ray.method(enable_task_events=True)
        def enabled_method(self):
            pass

        @ray.method(enable_task_events=False)
        def disabled_method(self):
            pass

    a_enabled = A.options(enable_task_events=True).remote()
    a_disabled = A.options(enable_task_events=False).remote()

    ray.get(
        [
            a_enabled.default_method.remote(),
            a_enabled.enabled_method.remote(),
            a_enabled.disabled_method.remote(),
        ]
    )
    ray.get(
        [
            a_disabled.default_method.remote(),
            a_disabled.enabled_method.remote(),
            a_disabled.disabled_method.remote(),
        ]
    )

    expected_task_events = {
        # All methods except the override-disabled method should be exported.
        (a_enabled._actor_id.hex(), "A.__init__"),
        (a_enabled._actor_id.hex(), "A.default_method"),
        (a_enabled._actor_id.hex(), "A.enabled_method"),
        # Only the override-enabled method should be exported.
        (a_disabled._actor_id.hex(), "A.enabled_method"),
    }

    def _verify():
        task_events = {(t.actor_id, t.name) for t in list_tasks()}
        assert task_events == expected_task_events
        return True

    wait_for_condition(_verify)


def test_task_events_actor_called_from_task(shutdown_only):
    """Call an actor method from within a task, verify events are/aren't exported."""

    @ray.remote
    class Actor:
        def f(self):
            pass

    @ray.remote(enable_task_events=False)
    def task(actor):
        actor.f.remote()

    a_enabled = Actor.options(enable_task_events=True).remote()
    a_disabled = Actor.options(enable_task_events=False).remote()
    ray.get(task.remote(a_enabled))
    ray.get(task.remote(a_disabled))

    # Expect to only see events from a_enabled, not a_disabled.
    expected_task_events = {
        (a_enabled._actor_id.hex(), "Actor.__init__"),
        (a_enabled._actor_id.hex(), "Actor.f"),
    }

    def _verify():
        task_events = {(t.actor_id, t.name) for t in list_tasks()}
        assert task_events == expected_task_events
        return True

    wait_for_condition(_verify)


def test_enable_task_events_invalid_options(shutdown_only):
    """Test the invalid values for the option."""

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


def test_enable_task_events_tasks(shutdown_only):
    """Test enabling/disabled task events at the task level."""
    system_config = {
        "task_events_report_interval_ms": 100,
        "metrics_report_interval_ms": 200,
        "enable_timeline": False,
    }

    ray.init(
        num_cpus=1,
        _system_config=system_config,
    )

    @ray.remote(enable_task_events=True)
    def inner_enabled():
        pass

    @ray.remote(enable_task_events=False)
    def inner_disabled():
        pass

    @ray.remote(enable_task_events=True)
    def outer_enabled():
        return ray.get(inner_disabled.remote())

    @ray.remote(enable_task_events=False)
    def outer_disabled():
        return ray.get(inner_enabled.remote())

    ray.get([outer_enabled.remote(), outer_disabled.remote()])

    expected_task_events = {"inner_enabled", "outer_enabled"}

    def _verify():
        task_events = {t.name for t in list_tasks()}
        assert task_events == expected_task_events
        return True

    wait_for_condition(_verify)


def test_enable_task_events_nested_actor(shutdown_only):
    """Test that enabling/disabling is independent for nested actors."""
    ray.init(
        num_cpus=1,
        _system_config={
            "task_events_report_interval_ms": 100,
            "metrics_report_interval_ms": 200,
            "enable_timeline": False,
        },
    )

    @ray.remote
    class Inner:
        def noop(self):
            pass

    @ray.remote
    class Outer:
        def __init__(self, inner: ray.actor.ActorHandle):
            self._inner = inner

        def call_inner(self):
            ray.get(self._inner.noop.remote())

    # Create one copy of Outer that has task events enabled, but calls a copy of
    # inner that has task events disabled.
    inner_disabled = Inner.options(enable_task_events=False).remote()
    outer_enabled = Outer.options(enable_task_events=True).remote(inner_disabled)

    # Create another copy of Outer that has task events disabled, but calls a copy of
    # inner that has task events enabled.
    inner_enabled = Inner.options(enable_task_events=True).remote()
    outer_disabled = Outer.options(enable_task_events=False).remote(inner_enabled)

    ray.get(outer_enabled.call_inner.remote())
    ray.get(outer_disabled.call_inner.remote())

    expected_task_events = {
        (outer_enabled._actor_id.hex(), "Outer.__init__"),
        (outer_enabled._actor_id.hex(), "Outer.call_inner"),
        (inner_enabled._actor_id.hex(), "Inner.__init__"),
        (inner_enabled._actor_id.hex(), "Inner.noop"),
    }

    def _verify():
        task_events = {(t.actor_id, t.name) for t in list_tasks()}
        assert task_events == expected_task_events
        return True

    wait_for_condition(_verify)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
