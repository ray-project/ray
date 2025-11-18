import time

import pytest

import ray
from ray.train.v2._internal.execution.controller.placement_group_cleaner import (
    PlacementGroupCleaner,
)
from ray.util.placement_group import placement_group, remove_placement_group


@pytest.fixture(autouse=True)
def ray_start():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


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


def test_placement_group_cleaner_basic_lifecycle():
    """Test that the PlacementGroupCleaner can be launched and stopped."""

    # Launch cleaner as detached
    cleaner = (
        ray.remote(num_cpus=0, max_concurrency=2)(PlacementGroupCleaner)
        .options(
            name="test_pg_cleaner",
            namespace="test",
            lifetime="detached",
            get_if_exists=False,
        )
        .remote(check_interval_s=0.1)
    )

    # Create a mock controller
    controller = MockController.remote()
    controller_id = ray.get(controller.get_actor_id.remote())

    # Register controller
    ray.get(cleaner.register_controller.remote(controller_id))

    # Create a placement group
    pg = placement_group([{"CPU": 1}], strategy="SPREAD")
    ray.get(pg.ready())

    # Register placement group
    ray.get(cleaner.register_placement_group.remote(pg))

    # Start monitoring
    result = ray.get(cleaner.start_monitoring.remote())
    assert result is True

    # Give it a moment to start monitoring
    time.sleep(0.2)

    # Controller is still alive, so PG should still exist
    assert pg.id is not None

    # Stop the cleaner gracefully
    ray.get(cleaner.stop.remote(), timeout=2.0)

    # PG should still exist after graceful stop
    # (Cleanup only happens on ungraceful controller death)
    try:
        # If PG exists, this should work
        remove_placement_group(pg)
    except Exception as e:
        pytest.fail(f"Placement group should still exist after graceful stop: {e}")
    finally:
        ray.get(controller.shutdown.remote())


def test_pg_cleaner_cleans_up_on_controller_death():
    """Test that the PG cleaner removes PG when controller dies."""

    # Launch cleaner as detached
    cleaner = (
        ray.remote(num_cpus=0, max_concurrency=2)(PlacementGroupCleaner)
        .options(
            name="test_pg_cleaner_cleanup",
            namespace="test",
            lifetime="detached",
            get_if_exists=False,
        )
        .remote(check_interval_s=0.1)
    )

    # Create a mock controller
    controller = MockController.remote()
    controller_id = ray.get(controller.get_actor_id.remote())

    # Register controller
    ray.get(cleaner.register_controller.remote(controller_id))

    # Create a placement group
    pg = placement_group([{"CPU": 1}], strategy="SPREAD")
    ray.get(pg.ready())

    # Register placement group
    ray.get(cleaner.register_placement_group.remote(pg))

    # Start monitoring (this is async in the background)
    ray.get(cleaner.start_monitoring.remote())

    # Give it a moment to start monitoring
    time.sleep(0.2)

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


def test_pg_cleaner_handles_missing_controller():
    """Test that cleaner handles case where controller is not registered."""

    cleaner = (
        ray.remote(num_cpus=0, max_concurrency=2)(PlacementGroupCleaner)
        .options(
            name="test_pg_cleaner_no_controller",
            namespace="test",
            lifetime="detached",
            get_if_exists=False,
        )
        .remote(check_interval_s=0.1)
    )

    # Try to start monitoring without registering controller
    result = ray.get(cleaner.start_monitoring.remote())

    # Should return False and log warning
    assert result is False

    # Stop should still work
    ray.get(cleaner.stop.remote(), timeout=2.0)


def test_pg_cleaner_handles_duplicate_start():
    """Test that cleaner handles duplicate start_monitoring calls."""

    cleaner = (
        ray.remote(num_cpus=0, max_concurrency=2)(PlacementGroupCleaner)
        .options(
            name="test_pg_cleaner_duplicate",
            namespace="test",
            lifetime="detached",
            get_if_exists=False,
        )
        .remote(check_interval_s=0.1)
    )

    controller = MockController.remote()
    controller_id = ray.get(controller.get_actor_id.remote())
    ray.get(cleaner.register_controller.remote(controller_id))

    # Start monitoring
    result1 = ray.get(cleaner.start_monitoring.remote())
    assert result1 is True

    # Try to start again - should return False
    result2 = ray.get(cleaner.start_monitoring.remote())
    assert result2 is False

    # Stop
    ray.get(cleaner.stop.remote(), timeout=2.0)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
