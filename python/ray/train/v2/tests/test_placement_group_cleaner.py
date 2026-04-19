import threading
import time
from unittest.mock import patch

import pytest

import ray
from ray.exceptions import RayActorError
from ray.tests.client_test_utils import create_remote_signal_actor
from ray.train.v2._internal.execution.controller.placement_group_cleaner import (
    PlacementGroupCleaner,
)
from ray.util.placement_group import placement_group, remove_placement_group


@pytest.fixture(autouse=True)
def ray_start():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


@pytest.fixture
def monitoring_started_signal(monkeypatch):
    """Signal actor set whenever PlacementGroupCleaner begins monitoring."""

    SignalActor = create_remote_signal_actor(ray)
    signal_actor = SignalActor.options(num_cpus=0).remote()

    original_start_monitoring = PlacementGroupCleaner.start_monitoring

    def start_monitoring_with_signal(self):
        started = original_start_monitoring(self)
        if started:
            signal_actor.send.remote()
        return started

    monkeypatch.setattr(
        PlacementGroupCleaner,
        "start_monitoring",
        start_monitoring_with_signal,
    )

    try:
        yield signal_actor
    finally:
        try:
            ray.kill(signal_actor)
        except Exception:
            pass


@ray.remote(num_cpus=0)
class MockController:
    """Mock controller actor for testing."""

    def __init__(self):
        self._alive = True

    def get_actor_id(self):
        return ray.get_runtime_context().get_actor_id()

    def shutdown(self):
        """Simulate controller death."""
        self._alive = False
        ray.actor.exit_actor()


def test_placement_group_cleaner_basic_lifecycle(monitoring_started_signal):
    """Test that the PlacementGroupCleaner can be launched and stopped."""

    # Create a mock controller
    controller = MockController.remote()
    controller_id = ray.get(controller.get_actor_id.remote())

    # Launch cleaner as detached
    cleaner = (
        ray.remote(num_cpus=0)(PlacementGroupCleaner)
        .options(
            name="test_pg_cleaner",
            namespace="test",
            lifetime="detached",
            get_if_exists=False,
        )
        .remote(
            controller_actor_id=controller_id,
            check_interval_s=0.1,
            get_actor_timeout_s=2,
            stop_timeout=5,
        )
    )

    # Create a placement group
    pg = placement_group([{"CPU": 1}], strategy="SPREAD")
    ray.get(pg.ready())

    # Register placement group for cleanup
    ray.get(cleaner.register_placement_group.remote(pg))

    # Start monitoring asynchronously (same behavior as production code)
    cleaner.start_monitoring.remote()

    # Wait deterministically for monitoring to start to avoid flakiness.
    ray.get(monitoring_started_signal.wait.remote())

    # Controller is still alive, so PG should still exist
    assert pg.id is not None

    # Stop the cleaner gracefully; tolerate the actor exiting itself.
    try:
        ray.get(cleaner.stop.remote(), timeout=2.0)
    except RayActorError:
        pass

    # PG should still exist after graceful stop
    try:
        # If PG exists, this should work
        remove_placement_group(pg)
    except Exception as e:
        pytest.fail(f"Placement group should still exist after graceful stop: {e}")
    finally:
        try:
            ray.get(controller.shutdown.remote())
        except RayActorError:
            pass


def test_pg_cleaner_cleans_up_on_controller_death(monitoring_started_signal):
    """Test that the PG cleaner removes PG when controller dies."""

    # Create a mock controller
    controller = MockController.remote()
    controller_id = ray.get(controller.get_actor_id.remote())

    # Launch cleaner as detached
    cleaner = (
        ray.remote(num_cpus=0)(PlacementGroupCleaner)
        .options(
            name="test_pg_cleaner_cleanup",
            namespace="test",
            lifetime="detached",
            get_if_exists=False,
        )
        .remote(
            controller_actor_id=controller_id,
            check_interval_s=0.1,
            get_actor_timeout_s=2,
            stop_timeout=5,
        )
    )

    # Create a placement group
    pg = placement_group([{"CPU": 1}], strategy="SPREAD")
    ray.get(pg.ready())

    # Register placement group for cleanup
    ray.get(cleaner.register_placement_group.remote(pg))

    cleaner.start_monitoring.remote()

    # Wait deterministically for monitoring to start to avoid flakiness.
    ray.get(monitoring_started_signal.wait.remote())

    # Kill the controller
    ray.kill(controller)

    # Wait for cleaner to detect death and clean up
    time.sleep(1.0)

    # Try to verify PG was cleaned up
    # Note: After cleanup, the PG ID should no longer be valid
    try:
        # Try to remove the PG - if it was already removed, this should fail
        remove_placement_group(pg)
    except Exception:
        # This is expected - PG was already cleaned up
        pass

    # The cleaner should exit after performing cleanup since it's detached.
    with pytest.raises(RayActorError):
        ray.get(cleaner.start_monitoring.remote(), timeout=2.0)


def test_pg_cleaner_exits_on_controller_death_without_pg_registration(
    monitoring_started_signal,
):
    """Cleaner should exit cleanly if controller dies before any PG is registered."""

    controller = MockController.remote()
    controller_id = ray.get(controller.get_actor_id.remote())

    cleaner = (
        ray.remote(num_cpus=0)(PlacementGroupCleaner)
        .options(
            name="test_pg_cleaner_no_pg",
            namespace="test",
            lifetime="detached",
            get_if_exists=False,
        )
        .remote(
            controller_actor_id=controller_id,
            check_interval_s=0.1,
            get_actor_timeout_s=2,
            stop_timeout=5,
        )
    )

    cleaner.start_monitoring.remote()
    ray.get(monitoring_started_signal.wait.remote())

    ray.kill(controller)

    # Give the cleaner a moment to observe controller death and self-exit.
    time.sleep(1.0)

    with pytest.raises(RayActorError):
        ray.get(cleaner.start_monitoring.remote(), timeout=2.0)


def test_pg_cleaner_handles_duplicate_start():
    """Test that cleaner handles duplicate start_monitoring calls."""

    controller = MockController.remote()
    controller_id = ray.get(controller.get_actor_id.remote())

    cleaner = (
        ray.remote(num_cpus=0)(PlacementGroupCleaner)
        .options(
            name="test_pg_cleaner_duplicate",
            namespace="test",
            lifetime="detached",
            get_if_exists=False,
        )
        .remote(
            controller_actor_id=controller_id,
            check_interval_s=0.1,
            get_actor_timeout_s=2,
            stop_timeout=5,
        )
    )

    pg = placement_group([{"CPU": 1}], strategy="SPREAD")
    ray.get(pg.ready())
    ray.get(cleaner.register_placement_group.remote(pg))

    # Start monitoring asynchronously
    cleaner.start_monitoring.remote()

    # Stop
    try:
        ray.get(cleaner.stop.remote(), timeout=2.0)
    except RayActorError:
        pass

    # Detached cleaner should be gone after stop.
    with pytest.raises(RayActorError):
        ray.get(cleaner.start_monitoring.remote(), timeout=2.0)


def test_pg_cleaner_race_condition_new_pg_not_orphaned():
    """PG registered during the race window must not be orphaned.

    Race window:
    1. PGC calls queue.get()
    2. controller puts new_pg into queue
    3. controller dies
    4. PGC checks is_actor_alive

    The new_pg never gets cleaned up.
    """
    old_pg = placement_group([{"CPU": 1}])
    new_pg = placement_group([{"CPU": 1}])
    ray.get(old_pg.ready())
    ray.get(new_pg.ready())

    in_race_window = threading.Event()
    release = threading.Event()
    call_count = {"n": 0}

    def controlled_is_actor_alive(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return True  # first iteration: controller alive, keep looping
        # Second call: queue.get() has already timed out
        in_race_window.set()
        release.wait(timeout=5)
        return False  # simulate controller death

    target = (
        "ray.train.v2._internal.execution.controller"
        ".placement_group_cleaner.is_actor_alive"
    )
    with patch(target, side_effect=controlled_is_actor_alive):
        cleaner = PlacementGroupCleaner(
            controller_actor_id="test_pg_cleaner",
            check_interval_s=0.05,
            get_actor_timeout_s=2,
            stop_timeout=5,
        )

        cleaner.register_placement_group(old_pg)
        cleaner.start_monitoring()

        assert in_race_window.wait(timeout=5), "Timed out waiting for race window"

        # new_pg is now in the queue, but cleaner has already passed queue.get()
        cleaner.register_placement_group(new_pg)
        release.set()

        cleaner._monitor_thread.join(timeout=5)

    new_pg_state = ray.util.placement_group_table(new_pg).get("state")
    assert (
        new_pg_state == "REMOVED"
    ), f"Race condition: new_pg was orphaned (state={new_pg_state})"

    try:
        remove_placement_group(old_pg)
    except Exception:
        pass


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
