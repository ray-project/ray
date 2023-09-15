import pytest
import sys

import ray
from ray import tune
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
