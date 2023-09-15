import os
from unittest.mock import patch

import pytest
import sys

import ray
from ray import tune
from ray.air import CheckpointConfig
from ray.air.execution import FixedResourceManager, PlacementGroupResourceManager
from ray.tune import Experiment, PlacementGroupFactory
from ray.tune.execution.tune_controller import TuneController

from ray.train.tests.util import mock_storage_context
from ray.tune.experiment import Trial
from ray.tune.impl.placeholder import create_resolvers_map, inject_placeholders
from ray.tune.search import BasicVariantGenerator

STORAGE = mock_storage_context()


@pytest.fixture(scope="function")
def ray_start_4_cpus_2_gpus_extra():
    address_info = ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
    yield address_info
    ray.shutdown()


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_controller_restore_dataset_references(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls
):
    """Check that references to Ray Datasets are replaced on resume.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::
        testSearcherCorrectReferencesAfterRestore
    """

    class FakeDataset:
        def __init__(self, name):
            self.name = name

    config = {
        "param1": {
            "param2": tune.grid_search(
                [FakeDataset("1"), FakeDataset("2"), FakeDataset("3")]
            ),
        },
        "param4": tune.sample_from(lambda: 1),
        "param5": tune.sample_from(lambda spec: spec.config["param1"]["param2"]),
    }
    resolvers = create_resolvers_map()
    config = inject_placeholders(config, resolvers)

    def create_searcher():
        search_alg = BasicVariantGenerator()
        experiment_spec = {
            "run": "__fake",
            "stop": {"training_iteration": 2},
            "config": config,
        }
        experiments = [Experiment.from_json("test", experiment_spec)]
        search_alg.add_configurations(experiments)
        return search_alg

    searcher = create_searcher()

    restored_config = {
        "param1": {
            "param2": tune.grid_search(
                [FakeDataset("4"), FakeDataset("5"), FakeDataset("6")]
            ),
        },
        "param4": tune.sample_from(lambda: 8),
        "param5": tune.sample_from(lambda spec: spec["config"]["param1"]["param2"]),
    }
    replaced_resolvers = create_resolvers_map()
    inject_placeholders(restored_config, replaced_resolvers)

    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        reuse_actors=False,
        search_alg=searcher,
        placeholder_resolvers=replaced_resolvers,
        checkpoint_period=-1,
        storage=STORAGE,
    )

    while len(runner.get_trials()) < 3 or any(
        trial.status not in {Trial.RUNNING, Trial.TERMINATED}
        for trial in runner.get_trials()
    ):
        runner.step()

    assert len(runner.get_trials()) == 3, [t.config for t in runner.get_trials()]
    for t in runner.get_trials():
        # Make sure that all the trials carry updated config values.
        assert t.config["param1"]["param2"].name in ["4", "5", "6"]
        assert t.config["param4"] == 8
        assert t.config["param5"].name in ["4", "5", "6"]


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_controller_restore_no_error_resume(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls
):
    """Check that `resume=True` does not resume errored trials.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testTrialErrorResumeFalse
    """
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        storage=STORAGE,
    )

    kwargs = {
        "stopping_criterion": {"training_iteration": 4},
        "placement_group_factory": PlacementGroupFactory([{"CPU": 1, "GPU": 0}]),
        "storage": STORAGE,
    }
    trials = [
        Trial("__fake", config={"mock_error": True}, **kwargs),
        Trial("__fake", **kwargs),
        Trial("__fake", **kwargs),
    ]
    for t in trials:
        runner.add_trial(t)

    while not runner.is_finished():
        runner.step()

    runner.checkpoint(force=True)

    assert trials[0].status == Trial.ERROR
    del runner

    new_runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        storage=STORAGE,
        resume=True,
    )

    assert len(new_runner.get_trials()) == 3
    assert Trial.ERROR in (t.status for t in new_runner.get_trials())


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_controller_restore_error_only_resume(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls
):
    """Check that `resume=ERRORED_ONLY` only resumes errored trials.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testTrialErrorResumeTrue
    """
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        storage=STORAGE,
    )
    kwargs = {
        "stopping_criterion": {"training_iteration": 4},
        "placement_group_factory": PlacementGroupFactory([{"CPU": 1, "GPU": 0}]),
        "storage": STORAGE,
    }
    trials = [
        Trial("__fake", config={"mock_error": True}, **kwargs),
        Trial("__fake", **kwargs),
        Trial("__fake", **kwargs),
    ]
    for t in trials:
        runner.add_trial(t)

    while not runner.is_finished():
        runner.step()

    runner.checkpoint(force=True)

    assert trials[0].status == Trial.ERROR
    del runner

    new_runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        storage=STORAGE,
        resume="ERRORED_ONLY",
    )

    assert len(new_runner.get_trials()) == 3
    assert Trial.ERROR not in (t.status for t in new_runner.get_trials())
    # The below is just a check for standard behavior.
    disable_error = False
    for t in new_runner.get_trials():
        if t.config.get("mock_error"):
            t.config["mock_error"] = False
            disable_error = True
    assert disable_error

    while not new_runner.is_finished():
        new_runner.step()
    assert Trial.ERROR not in (t.status for t in new_runner.get_trials())


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_controller_restore_trial_save_restore(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls
):
    """Creates different trials to test runner.checkpoint/restore.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testTrialSaveRestore
    """
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        checkpoint_period=0,
        storage=STORAGE,
    )
    trials = [
        Trial(
            "__fake",
            trial_id="trial_terminate",
            stopping_criterion={"training_iteration": 1},
            checkpoint_config=CheckpointConfig(checkpoint_frequency=1),
            storage=STORAGE,
        )
    ]
    runner.add_trial(trials[0])
    while not runner.is_finished():
        # Start trial, process result, dispatch save and process save.
        runner.step()
    assert trials[0].status == Trial.TERMINATED

    trials += [
        Trial(
            "__fake",
            trial_id="trial_fail",
            stopping_criterion={"training_iteration": 3},
            checkpoint_config=CheckpointConfig(checkpoint_frequency=1),
            config={"mock_error": True},
            storage=STORAGE,
        )
    ]
    runner.add_trial(trials[1])
    while not runner.is_finished():
        runner.step()
    assert trials[1].status == Trial.ERROR

    trials += [
        Trial(
            "__fake",
            trial_id="trial_succ",
            stopping_criterion={"training_iteration": 2},
            checkpoint_config=CheckpointConfig(checkpoint_frequency=1),
            storage=STORAGE,
        )
    ]
    runner.add_trial(trials[2])

    while not trials[2].status == Trial.RUNNING:
        runner.step()  # Start trial
    assert len(runner._get_trial_checkpoints()) == 3

    runner2 = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        storage=STORAGE,
        resume="LOCAL",
    )
    for tid in ["trial_terminate", "trial_fail"]:
        original_trial = runner.get_trial(tid)
        restored_trial = runner2.get_trial(tid)
        assert original_trial.status == restored_trial.status

    restored_trial = runner2.get_trial("trial_succ")
    assert Trial.PENDING == restored_trial.status

    while not runner2.is_finished():
        runner2.step()
    assert restored_trial.status == Trial.TERMINATED


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_controller_restore_trial_no_checkpoint_save(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls
):
    """Check that non-checkpointing trials *are* saved.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testTrialNoCheckpointSave
    """
    with patch.dict(os.environ, {"TUNE_MAX_PENDING_TRIALS_PG": "1"}):
        runner = TuneController(
            resource_manager_factory=lambda: resource_manager_cls(),
            checkpoint_period=0,
            storage=STORAGE,
        )

        runner.add_trial(
            Trial(
                "__fake",
                trial_id="non_checkpoint",
                stopping_criterion={"training_iteration": 2},
                storage=STORAGE,
            )
        )

        while not all(t.status == Trial.TERMINATED for t in runner.get_trials()):
            runner.step()

        runner.add_trial(
            Trial(
                "__fake",
                trial_id="checkpoint",
                checkpoint_config=CheckpointConfig(
                    checkpoint_at_end=True,
                ),
                stopping_criterion={"training_iteration": 2},
                storage=STORAGE,
            )
        )

        while not all(t.status == Trial.TERMINATED for t in runner.get_trials()):
            runner.step()

        runner.add_trial(
            Trial(
                "__fake",
                trial_id="pending",
                stopping_criterion={"training_iteration": 2},
                storage=STORAGE,
            )
        )

        old_trials = runner.get_trials()
        while not old_trials[2].has_reported_at_least_once:
            runner.step()

        runner2 = TuneController(
            resource_manager_factory=lambda: resource_manager_cls(),
            storage=STORAGE,
            resume="LOCAL",
        )
        new_trials = runner2.get_trials()
        assert len(new_trials) == 3
        assert runner2.get_trial("non_checkpoint").status == Trial.TERMINATED
        assert runner2.get_trial("checkpoint").status == Trial.TERMINATED
        assert runner2.get_trial("pending").status == Trial.PENDING
        assert runner2.get_trial("pending").has_reported_at_least_once
        runner2.step()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
