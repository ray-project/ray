import sys
from collections import Counter
from pathlib import Path
from unittest import mock

import pytest

import ray
from ray.air.execution import FixedResourceManager, PlacementGroupResourceManager
from ray.train.tests.util import mock_storage_context
from ray.tune import PlacementGroupFactory, register_trainable
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experiment import Trial
from ray.tune.utils.mock_trainable import (
    MOCK_TRAINABLE_NAME,
    MyTrainableClass,
    register_mock_trainable,
)

STORAGE = mock_storage_context()


@pytest.fixture(scope="function")
def ray_start_4_cpus_2_gpus_extra():
    address_info = ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
    yield address_info
    ray.shutdown()


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_stop_trial(ray_start_4_cpus_2_gpus_extra, resource_manager_cls):
    """Stopping a trial while RUNNING or PENDING should work.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testStopTrial
    """

    register_mock_trainable()
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(), storage=STORAGE
    )
    kwargs = {
        "stopping_criterion": {"training_iteration": 10},
        "placement_group_factory": PlacementGroupFactory([{"CPU": 2, "GPU": 1}]),
        "config": {"sleep": 1},
        "storage": STORAGE,
    }
    trials = [
        Trial(MOCK_TRAINABLE_NAME, **kwargs),
        Trial(MOCK_TRAINABLE_NAME, **kwargs),
        Trial(MOCK_TRAINABLE_NAME, **kwargs),
        Trial(MOCK_TRAINABLE_NAME, **kwargs),
    ]
    for t in trials:
        runner.add_trial(t)

    counter = Counter(t.status for t in trials)

    # Wait until 2 trials started
    while counter.get("RUNNING", 0) != 2:
        runner.step()
        counter = Counter(t.status for t in trials)

    assert counter.get("RUNNING", 0) == 2
    assert counter.get("PENDING", 0) == 2

    # Stop trial that is running
    for trial in trials:
        if trial.status == Trial.RUNNING:
            runner._schedule_trial_stop(trial)
            break

    counter = Counter(t.status for t in trials)

    # Wait until the next trial started
    while counter.get("RUNNING", 0) < 2:
        runner.step()
        counter = Counter(t.status for t in trials)

    assert counter.get("RUNNING", 0) == 2
    assert counter.get("TERMINATED", 0) == 1
    assert counter.get("PENDING", 0) == 1

    # Stop trial that is pending
    for trial in trials:
        if trial.status == Trial.PENDING:
            runner._schedule_trial_stop(trial)
            break

    counter = Counter(t.status for t in trials)

    # Wait until 2 trials are running again
    while counter.get("RUNNING", 0) < 2:
        runner.step()
        counter = Counter(t.status for t in trials)

    assert counter.get("RUNNING", 0) == 2
    assert counter.get("TERMINATED", 0) == 2
    assert counter.get("PENDING", 0) == 0


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
@pytest.mark.parametrize("status", [Trial.PENDING, Trial.PAUSED, Trial.RUNNING])
def test_stop_trial_with_export_formats(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls, status, tmp_path
):
    """Export only trials with an actor, and stop actorless trials normally."""

    class ExportingTrainable(MyTrainableClass):
        def _export_model(self, export_formats, export_dir):
            export_path = Path(export_dir, "exported_model")
            export_path.write_text("exported")
            return {export_formats[0]: str(export_path)}

    register_trainable("exporting_trainable", ExportingTrainable)
    storage = mock_storage_context(storage_path=str(tmp_path))
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(), storage=storage
    )
    trial = Trial("exporting_trainable", export_formats=["model"], storage=storage)
    runner.add_trial(trial)

    try:
        if status != Trial.PENDING:
            while trial.status != Trial.RUNNING:
                runner.step()

        if status == Trial.PAUSED:
            runner._schedule_trial_pause(trial, should_checkpoint=True)
            while trial.status != Trial.PAUSED:
                runner.step()

        assert trial.status == status
        assert (trial in runner._trial_to_actor) == (status == Trial.RUNNING)

        runner.stop_trial(trial)

        assert trial.status == Trial.TERMINATED
        assert trial not in runner.get_live_trials()
        assert not runner._has_errored
        export_path = Path(trial.storage.trial_working_directory, "exported_model")
        if status == Trial.RUNNING:
            assert export_path.read_text() == "exported"
        else:
            assert not export_path.exists()
    finally:
        runner.cleanup()


@pytest.mark.parametrize("status", [Trial.PENDING, Trial.PAUSED])
@pytest.mark.parametrize("export_formats", [None, [], ["model"]])
def test_stop_actorless_trial_with_export_formats(status, export_formats, monkeypatch):
    monkeypatch.setenv("TUNE_MAX_PENDING_TRIALS_PG", "1")
    register_mock_trainable()
    storage = mock_storage_context()
    runner = TuneController(storage=storage)
    trial = Trial(MOCK_TRAINABLE_NAME, storage=storage, export_formats=export_formats)
    runner.add_trial(trial)
    runner._set_trial_status(trial, status)

    assert trial not in runner._trial_to_actor
    with mock.patch.object(
        runner._scheduler_alg, "on_trial_remove"
    ) as on_remove, mock.patch.object(
        runner._search_alg, "on_trial_complete"
    ) as on_search_complete, mock.patch.object(
        runner._callbacks, "on_trial_complete"
    ) as on_callback_complete:
        runner.stop_trial(trial)
        runner.stop_trial(trial)

    on_remove.assert_called_once_with(runner, trial)
    on_search_complete.assert_called_once_with(trial.trial_id)
    on_callback_complete.assert_called_once_with(
        iteration=runner._iteration, trials=runner.get_trials(), trial=trial
    )

    assert trial.status == Trial.TERMINATED
    assert trial not in runner.get_live_trials()
    assert not runner._has_errored
    assert not runner._actor_manager._actor_task_events.get_futures()


@pytest.mark.parametrize("export_error", [False, True])
def test_export_trial_with_actor(monkeypatch, export_error):
    monkeypatch.setenv("TUNE_MAX_PENDING_TRIALS_PG", "1")
    register_mock_trainable()
    storage = mock_storage_context()
    runner = TuneController(storage=storage)
    trial = Trial(MOCK_TRAINABLE_NAME, storage=storage, export_formats=["model"])
    runner.add_trial(trial)
    runner._set_trial_status(trial, Trial.RUNNING)
    runner._trial_to_actor[trial] = mock.sentinel.actor
    error = RuntimeError("export failed") if export_error else None

    with mock.patch.object(
        runner, "_schedule_trial_task", return_value=mock.sentinel.export_future
    ) as schedule_task, mock.patch.object(
        runner._actor_manager._actor_task_events, "resolve_future", side_effect=error
    ) as resolve_future:
        if export_error:
            with pytest.raises(RuntimeError) as exc_info:
                runner._schedule_trial_export(trial)
            assert exc_info.value is error
        else:
            runner._schedule_trial_export(trial)

    schedule_task.assert_called_once_with(
        trial=trial,
        method_name="export_model",
        args=(["model"],),
        on_result=None,
        on_error=runner._trial_task_failure,
        _return_future=True,
    )
    resolve_future.assert_called_once_with(mock.sentinel.export_future)


@pytest.mark.parametrize("error_callback", [False, True])
def test_stop_trial_with_export_error(monkeypatch, error_callback):
    monkeypatch.setenv("TUNE_MAX_PENDING_TRIALS_PG", "1")
    register_mock_trainable()
    storage = mock_storage_context()
    runner = TuneController(storage=storage)
    trial = Trial(MOCK_TRAINABLE_NAME, storage=storage, export_formats=["model"])
    runner.add_trial(trial)
    runner._set_trial_status(trial, Trial.RUNNING)
    runner._trial_to_actor[trial] = mock.sentinel.actor
    runner._actor_to_trial[mock.sentinel.actor] = trial

    def fail_export(future):
        error = RuntimeError("export failed")
        if error_callback:
            schedule_task.call_args.kwargs["on_error"](trial, error)
        else:
            raise error

    with mock.patch.object(
        runner, "_schedule_trial_task", return_value=mock.sentinel.export_future
    ) as schedule_task, mock.patch.object(
        runner._actor_manager._actor_task_events,
        "resolve_future",
        side_effect=fail_export,
    ), mock.patch.object(
        runner._actor_manager, "clear_actor_task_futures"
    ), mock.patch.object(
        runner, "_remove_actor"
    ):
        runner.stop_trial(trial)

    assert trial.status == Trial.ERROR
    assert runner._has_errored
    assert trial not in runner._trial_to_actor


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_remove_actor_tracking(ray_start_4_cpus_2_gpus_extra, resource_manager_cls):
    """When we reuse actors, actors that have been requested but not started
    should not be tracked in ``_stopping_actors``.

    When actors are re-used, we cancel original actor requests for the trial.
    If these actors haven't been alive, there won't be a stop future to be resolved,
    and thus they would remain in ``TuneController._stopping_actors`` until they
    get cleaned up after 600 seconds.

    This test asserts that these actors are not tracked in
    ``TuneController._stopping_actors`` at all.

    We start 4 actors, and one can run at a time. Actors are re-used across trials.
    When the experiment ends, we expect that only one actor is left to track
    in ``self._stopping_trials``.
    """
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        reuse_actors=True,
        storage=STORAGE,
    )

    def train_fn(config):
        return 1

    register_trainable("test_remove_actor_tracking", train_fn)

    kwargs = {
        "placement_group_factory": PlacementGroupFactory([{"CPU": 4, "GPU": 2}]),
        "storage": STORAGE,
    }
    trials = [Trial("test_remove_actor_tracking", **kwargs) for i in range(4)]
    for t in trials:
        runner.add_trial(t)

    while not runner.is_finished():
        runner.step()

    # Only one actor should be left to stop
    assert len(runner._stopping_actors) == 1

    runner.cleanup()

    assert len(runner._stopping_actors) == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "--reruns", "3", __file__]))
