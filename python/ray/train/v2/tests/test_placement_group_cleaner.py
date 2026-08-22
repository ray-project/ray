import threading
import time
from unittest.mock import MagicMock, call, patch

import pytest

import ray
from ray._common.constants import HEAD_NODE_RESOURCE_NAME
from ray._private.test_utils import run_string_as_driver_nonblocking, wait_for_condition
from ray.train.v2._internal.callbacks.placement_group_callback import (
    PlacementGroupCleanerCallback,
)
from ray.train.v2._internal.execution.controller.placement_group_cleaner import (
    PLACEMENT_GROUP_CLEANER_NAME,
    PLACEMENT_GROUP_CLEANER_NAMESPACE,
    PlacementGroupCleaner,
)
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.state.exception import RayStateApiException


@pytest.fixture(autouse=True)
def ray_start():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


@ray.remote(num_cpus=0)
class MockController:
    def get_actor_id(self):
        return ray.get_runtime_context().get_actor_id()


def _create_cleaner(name="test_pg_cleaner", **actor_options):
    actor_options.setdefault(
        "runtime_env",
        {
            "env_vars": {
                "RAY_ADDRESS": ray._private.worker._global_node.gcs_address,
            }
        },
    )
    return (
        ray.remote(num_cpus=0)(PlacementGroupCleaner)
        .options(
            name=name,
            namespace="test_placement_group_cleaner",
            lifetime="detached",
            get_if_exists=True,
            **actor_options,
        )
        .remote(
            check_interval_s=0.05,
            get_actor_timeout_s=2,
            stop_timeout=5,
        )
    )


def _controller_id(controller):
    return ray.get(controller.get_actor_id.remote())


def _pg_state(pg):
    return ray.util.placement_group_table(pg).get("state")


def test_get_or_create_returns_shared_cleaner():
    """Concurrent creators resolve to the same named detached actor."""
    handles = []
    errors = []

    def create():
        try:
            handles.append(_create_cleaner())
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=create) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert len({handle._actor_id for handle in handles}) == 1

    looked_up = ray.get_actor(
        "test_pg_cleaner", namespace="test_placement_group_cleaner"
    )
    assert looked_up._actor_id == handles[0]._actor_id


def test_separate_jobs_get_same_shared_cleaner():
    """Two independent drivers concurrently get or create one cluster actor."""
    address = ray._private.worker._global_node.gcs_address
    driver = f"""
import ray
from ray.train.v2._internal.execution.controller.placement_group_cleaner import PlacementGroupCleaner
ray.init(address={address!r})
cleaner = ray.remote(num_cpus=0)(PlacementGroupCleaner).options(
    name="test_cross_job_cleaner",
    namespace="test_cross_job_cleaner",
    lifetime="detached",
    get_if_exists=True,
).remote(check_interval_s=1, get_actor_timeout_s=1, stop_timeout=2)
print("CLEANER_ID=" + cleaner._actor_id.hex(), flush=True)
"""
    processes = [run_string_as_driver_nonblocking(driver) for _ in range(2)]
    outputs = []
    for process in processes:
        assert process.wait(timeout=30) == 0, process.stderr.read().decode()
        lines = process.stdout.read().decode().splitlines()
        outputs.append(
            next(
                line[len("CLEANER_ID=") :]
                for line in lines
                if line.startswith("CLEANER_ID=")
            )
        )

    assert outputs[0] == outputs[1]


def test_shared_cleaner_isolates_controllers_and_placement_groups():
    cleaner = _create_cleaner(name="test_shared_cleanup")
    controller_1 = MockController.remote()
    controller_2 = MockController.remote()
    controller_1_id = _controller_id(controller_1)
    controller_2_id = _controller_id(controller_2)

    pg_1 = placement_group([{"CPU": 1}])
    pg_1_replacement = placement_group([{"CPU": 1}])
    pg_2 = placement_group([{"CPU": 1}])
    ray.get([pg_1.ready(), pg_1_replacement.ready(), pg_2.ready()])

    assert ray.get(cleaner.register_controller.remote(controller_1_id))
    assert ray.get(cleaner.register_controller.remote(controller_2_id))
    assert ray.get(cleaner.register_placement_group.remote(controller_1_id, pg_1))
    assert ray.get(
        cleaner.register_placement_group.remote(controller_1_id, pg_1_replacement)
    )
    assert ray.get(cleaner.register_placement_group.remote(controller_2_id, pg_2))
    wait_for_condition(
        lambda: ray.get(cleaner.start_monitoring.remote()),
        timeout=10,
        retry_interval_ms=100,
    )

    ray.kill(controller_1)
    wait_for_condition(lambda: _pg_state(pg_1) == "REMOVED", timeout=10)
    wait_for_condition(lambda: _pg_state(pg_1_replacement) == "REMOVED", timeout=10)
    assert _pg_state(pg_2) != "REMOVED"

    # Duplicate registration is idempotent and a graceful unregister does not
    # clean up the surviving controller's placement group.
    assert ray.get(cleaner.register_controller.remote(controller_2_id))
    assert ray.get(cleaner.register_placement_group.remote(controller_2_id, pg_2))
    ray.get(cleaner.unregister_controller.remote(controller_2_id))
    ray.get(cleaner.unregister_controller.remote(controller_2_id))
    time.sleep(0.2)
    assert _pg_state(pg_2) != "REMOVED"

    remove_placement_group(pg_2)
    ray.kill(controller_2)


def test_duplicate_start_is_idempotent():
    cleaner = _create_cleaner(name="test_duplicate_start")
    assert ray.get(cleaner.start_monitoring.remote())
    assert ray.get(cleaner.start_monitoring.remote())


def test_delayed_registration_for_dead_controller_is_rejected():
    cleaner = PlacementGroupCleaner(0.01, 1, 1)
    cleaner.register_controller("dead")
    cleaner._mark_controller_dead("dead")

    pg = MagicMock()
    pg.id.hex.return_value = "pg"
    assert not cleaner.register_controller("dead")
    assert not cleaner.register_placement_group("dead", pg)
    assert cleaner._controller_states["dead"].placement_groups == {"pg": pg}

    cleaner.register_controller("unregistered")
    cleaner.unregister_controller("unregistered")
    late_pg = MagicMock()
    late_pg.id.hex.return_value = "late-pg"
    assert not cleaner.register_placement_group("unregistered", late_pg)
    assert cleaner._controller_states["unregistered"].cleaning
    assert cleaner._controller_states["unregistered"].placement_groups == {
        "late-pg": late_pg
    }

    cleaner.register_controller("active")
    active_pgs = [MagicMock(), MagicMock()]
    for index, active_pg in enumerate(active_pgs):
        active_pg.id.hex.return_value = f"active-pg-{index}"
        assert cleaner.register_placement_group("active", active_pg)
    cleaner.unregister_placement_group("active", active_pgs[0])
    assert cleaner._controller_states["active"].placement_groups == {
        "active-pg-1": active_pgs[1]
    }


def test_unregister_keeps_in_memory_state_if_persistence_fails():
    cleaner = PlacementGroupCleaner(0.01, 1, 1)
    cleaner.register_controller("controller")

    with patch.object(
        cleaner, "_delete_controller_state", side_effect=RuntimeError("KV unavailable")
    ):
        with pytest.raises(RuntimeError, match="KV unavailable"):
            cleaner.unregister_controller("controller")

    with patch.object(cleaner, "_persist_controller_state") as persist:
        assert cleaner.register_controller("controller")
    persist.assert_not_called()

    pg = MagicMock()
    pg.id.hex.return_value = "pg"
    cleaner.register_placement_group("controller", pg)
    with patch.object(
        cleaner, "_persist_controller_state", side_effect=RuntimeError("KV unavailable")
    ):
        with pytest.raises(RuntimeError, match="KV unavailable"):
            cleaner.unregister_placement_group("controller", pg)
    assert cleaner._controller_states["controller"].placement_groups == {"pg": pg}


def test_registration_during_death_check_is_cleaned():
    """A PG registered while liveness is checked must not be orphaned."""
    cleaner = PlacementGroupCleaner(0.01, 1, 2)
    cleaner.register_controller("controller")
    old_pg = MagicMock()
    old_pg.id.hex.return_value = "old"
    new_pg = MagicMock()
    new_pg.id.hex.return_value = "new"
    cleaner.register_placement_group("controller", old_pg)

    in_liveness_check = threading.Event()
    release_liveness_check = threading.Event()

    def controller_died(*args, **kwargs):
        in_liveness_check.set()
        release_liveness_check.wait(timeout=5)
        return False

    with (
        patch(
            "ray.train.v2._internal.execution.controller."
            "placement_group_cleaner.is_actor_alive",
            side_effect=controller_died,
        ),
        patch.object(cleaner, "_cleanup_placement_group") as cleanup,
    ):
        cleaner.start_monitoring()
        assert in_liveness_check.wait(timeout=5)
        assert cleaner.register_placement_group("controller", new_pg)
        release_liveness_check.set()
        wait_for_condition(lambda: cleanup.call_count == 2, timeout=5)
        cleaner._stop_monitor_thread()

    assert cleanup.call_args_list == [
        call("controller", old_pg),
        call("controller", new_pg),
    ]


def test_registration_during_cleanup_is_retried():
    cleaner = PlacementGroupCleaner(0.01, 1, 2)
    cleaner.register_controller("controller")
    old_pg = MagicMock()
    old_pg.id.hex.return_value = "old"
    late_pg = MagicMock()
    late_pg.id.hex.return_value = "late"
    cleaner.register_placement_group("controller", old_pg)

    cleaning_old_pg = threading.Event()
    release_cleanup = threading.Event()
    cleaned = []

    def cleanup(controller_actor_id, placement_group):
        if placement_group is old_pg:
            cleaning_old_pg.set()
            release_cleanup.wait(timeout=5)
        cleaned.append((controller_actor_id, placement_group))
        return True

    with (
        patch(
            "ray.train.v2._internal.execution.controller."
            "placement_group_cleaner.is_actor_alive",
            return_value=False,
        ),
        patch.object(cleaner, "_cleanup_placement_group", side_effect=cleanup),
    ):
        cleaner.start_monitoring()
        assert cleaning_old_pg.wait(timeout=5)
        assert not cleaner.register_placement_group("controller", late_pg)
        release_cleanup.set()
        wait_for_condition(lambda: len(cleaned) == 2, timeout=5)
        cleaner._stop_monitor_thread()

    assert cleaned == [("controller", old_pg), ("controller", late_pg)]


def test_state_api_failure_does_not_block_other_controllers():
    cleaner = PlacementGroupCleaner(0.01, 1, 2)
    cleaner.register_controller("unavailable")
    cleaner.register_controller("unexpected-error")
    cleaner.register_controller("dead-without-placement-groups")
    cleaner.register_controller("dead")
    dead_pg = MagicMock()
    dead_pg.id.hex.return_value = "dead-pg"
    cleaner.register_placement_group("dead", dead_pg)

    def actor_alive(actor_id, **kwargs):
        if actor_id == "unavailable":
            raise RayStateApiException("temporarily unavailable")
        if actor_id == "unexpected-error":
            raise RuntimeError("unexpected state API failure")
        return False

    with (
        patch(
            "ray.train.v2._internal.execution.controller."
            "placement_group_cleaner.is_actor_alive",
            side_effect=actor_alive,
        ),
        patch.object(cleaner, "_cleanup_placement_group") as cleanup,
    ):
        cleaner.start_monitoring()
        wait_for_condition(
            lambda: cleanup.call_count == 1
            and "dead-without-placement-groups" not in cleaner._controller_states,
            timeout=5,
        )
        cleaner._stop_monitor_thread()

    cleanup.assert_called_once_with("dead", dead_pg)


def test_missing_controller_state_does_not_trigger_cleanup():
    cleaner = PlacementGroupCleaner(0.01, 1, 2)
    cleaner.register_controller("controller")
    placement_group = MagicMock()
    placement_group.id.hex.return_value = "pg"
    cleaner.register_placement_group("controller", placement_group)

    with (
        patch(
            "ray.train.v2._internal.execution.controller."
            "placement_group_cleaner.is_actor_alive",
            side_effect=RayStateApiException("controller not found yet"),
        ),
        patch.object(cleaner, "_cleanup_placement_group") as cleanup,
    ):
        cleaner.start_monitoring()
        time.sleep(0.1)
        cleaner._stop_monitor_thread()

    cleanup.assert_not_called()
    assert "controller" in cleaner._controller_states


def test_permanently_missing_controller_is_eventually_cleaned():
    cleaner = PlacementGroupCleaner(0.01, 1, 2)
    cleaner.register_controller("controller")
    placement_group = MagicMock()
    placement_group.id.hex.return_value = "pg"
    cleaner.register_placement_group("controller", placement_group)

    with (
        patch(
            "ray.train.v2._internal.execution.controller."
            "placement_group_cleaner.is_actor_alive",
            return_value=None,
        ),
        patch.object(cleaner, "_cleanup_placement_group", return_value=True) as cleanup,
    ):
        cleaner.start_monitoring()
        wait_for_condition(lambda: cleanup.called, timeout=5)
        cleaner._stop_monitor_thread()

    cleanup.assert_called_once_with("controller", placement_group)


def test_monitor_queries_its_own_cluster_address():
    cleaner = PlacementGroupCleaner(0.01, 1, 2)
    cleaner.register_controller("alive")

    with patch(
        "ray.train.v2._internal.execution.controller."
        "placement_group_cleaner.is_actor_alive",
        return_value=True,
    ) as actor_alive:
        cleaner.start_monitoring()
        wait_for_condition(lambda: actor_alive.call_count > 0, timeout=5)
        cleaner._stop_monitor_thread()

    actor_alive.assert_called_with(
        actor_id="alive", timeout=1, address=ray.get_runtime_context().gcs_address
    )


def test_placement_group_cleanup_failure_does_not_block_other_groups():
    cleaner = PlacementGroupCleaner(0.01, 1, 2)
    cleaner.register_controller("dead")
    placement_groups = [MagicMock(), MagicMock()]
    for index, pg in enumerate(placement_groups):
        pg.id.hex.return_value = f"pg-{index}"
        cleaner.register_placement_group("dead", pg)

    second_cleanup_attempted = threading.Event()
    cleanup_calls = 0

    def remove_with_first_failure(placement_group):
        nonlocal cleanup_calls
        cleanup_calls += 1
        if cleanup_calls == 1:
            raise RuntimeError("first cleanup failed")
        second_cleanup_attempted.set()
        # Stop before the next retry cycle; this test only verifies isolation
        # between PGs within the current cleanup pass.
        cleaner._stop_event.set()

    with (
        patch(
            "ray.train.v2._internal.execution.controller."
            "placement_group_cleaner.is_actor_alive",
            return_value=False,
        ),
        patch.object(cleaner, "_is_placement_group_removed", return_value=False),
        patch(
            "ray.train.v2._internal.execution.controller."
            "placement_group_cleaner.remove_placement_group",
            side_effect=remove_with_first_failure,
        ),
    ):
        cleaner.start_monitoring()
        assert second_cleanup_attempted.wait(timeout=5)
        cleaner._stop_monitor_thread()

    assert cleanup_calls == 2


def test_cleaner_restarts_and_accepts_new_registration():
    """The singleton restores registrations after an unexpected process exit."""
    cleaner = _create_cleaner(name="test_restart", max_restarts=1)
    actor_id = cleaner._actor_id
    controller = MockController.remote()
    controller_id = _controller_id(controller)
    pg = placement_group([{"CPU": 1}])
    ray.get(pg.ready())
    assert ray.get(cleaner.register_controller.remote(controller_id))
    assert ray.get(cleaner.register_placement_group.remote(controller_id, pg))

    ray.kill(cleaner, no_restart=False)

    wait_for_condition(
        # Registering a PG without registering the controller again proves that
        # the controller record was restored from the GCS-backed checkpoint.
        lambda: ray.get(cleaner.register_placement_group.remote(controller_id, pg)),
        timeout=10,
        retry_interval_ms=100,
    )
    assert cleaner._actor_id == actor_id
    wait_for_condition(
        lambda: (ray.get(cleaner.unregister_controller.remote(controller_id)) is None),
        timeout=10,
        retry_interval_ms=100,
    )
    remove_placement_group(pg)
    ray.kill(controller)


def test_cleaner_resumes_incomplete_cleanup_after_restart():
    cleaner = _create_cleaner(name="test_restart_during_cleanup", max_restarts=1)
    pg = placement_group([{"CPU": 1}])
    ray.get(pg.ready())
    assert ray.get(cleaner.register_controller.remote("dead-controller"))
    assert ray.get(cleaner.register_placement_group.remote("dead-controller", pg))
    ray.get(cleaner._mark_controller_dead.remote("dead-controller"))

    ray.kill(cleaner, no_restart=False)

    wait_for_condition(
        lambda: _pg_state(pg) == "REMOVED",
        timeout=15,
        retry_interval_ms=100,
    )


def test_callback_uses_shared_actor_options_and_controller_scoped_calls():
    callback = PlacementGroupCleanerCallback(check_interval_s=0.1)
    controller_id = "controller-id"
    context = MagicMock()
    context.get_actor_id.return_value = controller_id

    with (
        patch.object(ray, "remote") as mock_remote,
        patch.object(ray, "get", return_value=True),
        patch.object(ray.runtime_context, "get_runtime_context", return_value=context),
    ):
        callback.after_controller_start(train_run_context=MagicMock())

    actor = mock_remote.return_value.return_value
    options = actor.options.call_args.kwargs
    assert options == {
        "name": PLACEMENT_GROUP_CLEANER_NAME,
        "namespace": PLACEMENT_GROUP_CLEANER_NAMESPACE,
        "lifetime": "detached",
        "get_if_exists": True,
        "resources": {HEAD_NODE_RESOURCE_NAME: 0.001},
        "scheduling_strategy": "DEFAULT",
        "max_restarts": -1,
        "max_task_retries": -1,
    }
    cleaner = actor.options.return_value.remote.return_value
    cleaner.register_controller.remote.assert_called_once_with(controller_id)
    cleaner.start_monitoring.remote.assert_called_once_with()


def test_callback_unregisters_only_its_controller():
    callback = PlacementGroupCleanerCallback(check_interval_s=0.1)
    callback._controller_actor_id = "controller-id"
    cleaner = MagicMock()
    callback._cleaner = cleaner
    pg = MagicMock()
    callback._registered_placement_group = pg

    with patch.object(ray, "get") as mock_get:
        callback.after_worker_group_shutdown(MagicMock())

        cleaner.unregister_placement_group.remote.assert_called_once_with(
            "controller-id", pg
        )
        mock_get.assert_called_once_with(
            cleaner.unregister_placement_group.remote.return_value,
            timeout=callback._stop_timeout,
        )
        assert callback._registered_placement_group is None

        cleaner.reset_mock()
        mock_get.reset_mock()
        callback._stop_cleaner()

    assert callback._cleaner is None
    cleaner.unregister_controller.remote.assert_called_once_with("controller-id")
    mock_get.assert_called_once_with(
        cleaner.unregister_controller.remote.return_value,
        timeout=callback._stop_timeout,
    )


def test_callback_keeps_registration_during_controller_abort():
    callback = PlacementGroupCleanerCallback(check_interval_s=0.1)
    callback._controller_actor_id = "controller-id"
    cleaner = MagicMock()
    callback._cleaner = cleaner
    placement_group = MagicMock()
    callback._registered_placement_group = placement_group

    with patch.object(ray, "get"):
        callback.before_controller_abort()
        cleaner.unregister_controller.remote.assert_not_called()

        callback.after_worker_group_abort(MagicMock())

    cleaner.unregister_placement_group.remote.assert_called_once_with(
        "controller-id", placement_group
    )
    cleaner.unregister_controller.remote.assert_called_once_with("controller-id")


def test_callback_warns_if_late_registration_is_rejected():
    callback = PlacementGroupCleanerCallback(check_interval_s=0.1)
    callback._controller_actor_id = "dead-controller"
    callback._cleaner = MagicMock()
    pg = MagicMock()
    worker_group = MagicMock()
    worker_group.get_worker_group_state.return_value.placement_group_handle.placement_group = (
        pg
    )

    with patch.object(ray, "get", return_value=False):
        callback.after_worker_group_start(worker_group)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
