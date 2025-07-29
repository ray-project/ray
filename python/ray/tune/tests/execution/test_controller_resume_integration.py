import os
import sys
from unittest.mock import patch

import pandas as pd
import pytest

import ray
from ray import tune
from ray.air.execution import FixedResourceManager, PlacementGroupResourceManager
from ray.train.tests.util import mock_storage_context
from ray.tune import CheckpointConfig, Experiment, PlacementGroupFactory, ResumeConfig
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experiment import Trial
from ray.tune.impl.placeholder import create_resolvers_map, inject_placeholders
from ray.tune.search import BasicVariantGenerator
from ray.tune.utils.mock_trainable import (
    MOCK_ERROR_KEY,
    MOCK_TRAINABLE_NAME,
    register_mock_trainable,
)

STORAGE = mock_storage_context()


@pytest.fixture(autouse=True)
def register_test_trainable():
    register_mock_trainable()


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
            "run": MOCK_TRAINABLE_NAME,
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
        Trial(MOCK_TRAINABLE_NAME, config={MOCK_ERROR_KEY: True}, **kwargs),
        Trial(MOCK_TRAINABLE_NAME, **kwargs),
        Trial(MOCK_TRAINABLE_NAME, **kwargs),
    ]
    for t in trials:
        runner.add_trial(t)

    while not runner.is_finished():
        runner.step()

    runner.checkpoint(force=True, wait=True)

    assert trials[0].status == Trial.ERROR
    del runner

    new_runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        storage=STORAGE,
        resume_config=ResumeConfig(
            unfinished=ResumeConfig.ResumeType.RESUME,
            errored=ResumeConfig.ResumeType.SKIP,
            finished=ResumeConfig.ResumeType.SKIP,
        ),
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
        Trial(MOCK_TRAINABLE_NAME, config={MOCK_ERROR_KEY: True}, **kwargs),
        Trial(MOCK_TRAINABLE_NAME, **kwargs),
        Trial(MOCK_TRAINABLE_NAME, **kwargs),
    ]
    for t in trials:
        runner.add_trial(t)

    while not runner.is_finished():
        runner.step()

    runner.checkpoint(force=True, wait=True)

    assert trials[0].status == Trial.ERROR
    del runner

    new_runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        storage=STORAGE,
        resume_config=ResumeConfig(
            unfinished=ResumeConfig.ResumeType.SKIP,
            errored=ResumeConfig.ResumeType.RESUME,
            finished=ResumeConfig.ResumeType.SKIP,
        ),
    )

    assert len(new_runner.get_trials()) == 3
    assert Trial.ERROR not in (t.status for t in new_runner.get_trials())
    # The below is just a check for standard behavior.
    disable_error = False
    for t in new_runner.get_trials():
        if t.config.get(MOCK_ERROR_KEY):
            t.config[MOCK_ERROR_KEY] = False
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
            MOCK_TRAINABLE_NAME,
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
            MOCK_TRAINABLE_NAME,
            trial_id="trial_fail",
            stopping_criterion={"training_iteration": 3},
            checkpoint_config=CheckpointConfig(checkpoint_frequency=1),
            config={MOCK_ERROR_KEY: True},
            storage=STORAGE,
        )
    ]
    runner.add_trial(trials[1])
    while not runner.is_finished():
        runner.step()
    assert trials[1].status == Trial.ERROR

    trials += [
        Trial(
            MOCK_TRAINABLE_NAME,
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

    runner.checkpoint(force=True, wait=True)

    runner2 = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        storage=STORAGE,
        resume_config=ResumeConfig(
            unfinished=ResumeConfig.ResumeType.RESUME,
            errored=ResumeConfig.ResumeType.SKIP,
            finished=ResumeConfig.ResumeType.SKIP,
        ),
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
                MOCK_TRAINABLE_NAME,
                trial_id="non_checkpoint",
                stopping_criterion={"training_iteration": 2},
                storage=STORAGE,
            )
        )

        while not all(t.status == Trial.TERMINATED for t in runner.get_trials()):
            runner.step()

        runner.add_trial(
            Trial(
                MOCK_TRAINABLE_NAME,
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
                MOCK_TRAINABLE_NAME,
                trial_id="pending",
                stopping_criterion={"training_iteration": 2},
                storage=STORAGE,
            )
        )

        old_trials = runner.get_trials()
        while not old_trials[2].has_reported_at_least_once:
            runner.step()

        runner.checkpoint(force=True, wait=True)

        runner2 = TuneController(
            resource_manager_factory=lambda: resource_manager_cls(),
            storage=STORAGE,
            resume_config=ResumeConfig(
                unfinished=ResumeConfig.ResumeType.RESUME,
                errored=ResumeConfig.ResumeType.SKIP,
                finished=ResumeConfig.ResumeType.SKIP,
            ),
        )
        new_trials = runner2.get_trials()
        assert len(new_trials) == 3
        assert runner2.get_trial("non_checkpoint").status == Trial.TERMINATED
        assert runner2.get_trial("checkpoint").status == Trial.TERMINATED
        assert runner2.get_trial("pending").status == Trial.PENDING
        assert runner2.get_trial("pending").has_reported_at_least_once
        runner2.step()


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_controller_restore_checkpoint_overwrite(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls
):
    """Check that experiment state checkpoint are not overwritten on continue.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testCheckpointOverwrite
    """
    storage = mock_storage_context()

    def count_checkpoints(cdir):
        return sum(
            (fname.startswith("experiment_state") and fname.endswith(".json"))
            for fname in os.listdir(cdir)
        )

    tmpdir = storage.experiment_driver_staging_path
    # The Trial `local_dir` must match the TrialRunner `local_checkpoint_dir`
    # to match the directory structure assumed by `TrialRunner.resume`.
    # See `test_trial_runner2.TrialRunnerTest2.testPauseResumeCheckpointCount`
    # for more details.
    trial = Trial(
        MOCK_TRAINABLE_NAME,
        checkpoint_config=CheckpointConfig(checkpoint_frequency=1),
        storage=storage,
    )
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        storage=storage,
        checkpoint_period=0,
    )
    runner.add_trial(trial)
    while not trial.status == Trial.RUNNING:
        runner.step()
    # force checkpoint
    runner.checkpoint(force=True, wait=True)
    # Only one experiment state file
    assert count_checkpoints(tmpdir) == 1

    runner2 = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        storage=storage,
        resume_config=ResumeConfig(
            unfinished=ResumeConfig.ResumeType.RESUME,
            errored=ResumeConfig.ResumeType.SKIP,
            finished=ResumeConfig.ResumeType.SKIP,
        ),
    )
    trial = runner2.get_trials()[0]
    while not trial.status == Trial.RUNNING:
        runner2.step()
    # After resume, we have a new experiment state file in the directory
    assert count_checkpoints(tmpdir) == 2

    runner2.checkpoint()
    assert count_checkpoints(tmpdir) == 2


@pytest.mark.skip("TODO(justinvyu): Data lineage serialization context is broken.")
@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_controller_restore_with_dataset(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls, tmp_path
):
    """Test trial runner checkpointing where trials contain Datasets.
    When possible, a dataset plan should be saved (for read_* APIs).
    See `Dataset.serialize_lineage` for more information.

    If a dataset cannot be serialized, an experiment checkpoint
    should still be created. Users can pass in the dataset again by
    re-specifying the `param_space`.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::
        testExperimentCheckpointWithDatasets
    """
    # Save some test data to load
    data_filepath = os.path.join(tmp_path, "test.csv")
    pd.DataFrame({"x": list(range(10))}).to_csv(data_filepath)

    def create_trial_config():
        return {
            "datasets": {
                "with_lineage": ray.data.read_csv(data_filepath),
                "no_lineage": ray.data.from_items([{"x": i} for i in range(10)]),
            }
        }

    resolvers = create_resolvers_map()
    config_with_placeholders = inject_placeholders(create_trial_config(), resolvers)
    trial = Trial(
        MOCK_TRAINABLE_NAME,
        config=config_with_placeholders,
        storage=STORAGE,
    )
    trial.init_local_path()
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        storage=STORAGE,
        placeholder_resolvers=resolvers,
    )
    runner.add_trial(trial)
    # Req: TrialRunner checkpointing shouldn't error
    runner.checkpoint(force=True, wait=True)

    # Manually clear all block refs that may have been created
    ray.shutdown()
    ray.init(num_cpus=2)

    register_mock_trainable()
    new_runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        storage=STORAGE,
    )
    new_runner.resume(resume_config=ResumeConfig())
    [loaded_trial] = new_runner.get_trials()
    loaded_datasets = loaded_trial.config["datasets"]

    # Req: The deserialized dataset (w/ lineage) should be usable.
    assert [el["x"] for el in loaded_datasets["with_lineage"].take()] == list(range(10))

    replaced_resolvers = create_resolvers_map()
    inject_placeholders(create_trial_config(), replaced_resolvers)

    respecified_config_runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        storage=STORAGE,
        placeholder_resolvers=replaced_resolvers,
    )
    respecified_config_runner.resume(resume_config=ResumeConfig())
    [loaded_trial] = respecified_config_runner.get_trials()
    ray_ds_no_lineage = loaded_trial.config["datasets"]["no_lineage"]

    # Req: The dataset (w/o lineage) can be re-specified and is usable after.
    assert [el["x"] for el in ray_ds_no_lineage.take()] == list(range(10))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
