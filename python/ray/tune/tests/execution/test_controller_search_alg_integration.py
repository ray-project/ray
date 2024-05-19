import os
import pickle
import sys
from collections import Counter

import pytest

import ray
from ray.air.constants import TRAINING_ITERATION
from ray.air.execution import FixedResourceManager, PlacementGroupResourceManager
from ray.train.tests.util import mock_storage_context
from ray.tune import Experiment, PlacementGroupFactory
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experiment import Trial
from ray.tune.schedulers import FIFOScheduler, TrialScheduler
from ray.tune.search import ConcurrencyLimiter, Repeater, Searcher, SearchGenerator
from ray.tune.search._mock import _MockSuggestionAlgorithm


class TestTuneController(TuneController):
    def __init__(self, *args, **kwargs):
        kwargs.update(dict(storage=mock_storage_context()))
        super().__init__(*args, **kwargs)


@pytest.fixture(scope="function")
def ray_start_8_cpus():
    address_info = ray.init(num_cpus=8, num_gpus=0)
    yield address_info
    ray.shutdown()


@pytest.fixture(scope="function")
def ray_start_4_cpus_2_gpus_extra():
    address_info = ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
    yield address_info
    ray.shutdown()


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_search_alg_notification(ray_start_4_cpus_2_gpus_extra, resource_manager_cls):
    """Check that the searchers gets notified of trial results + completions.

    Also check that the searcher is "finished" before the runner, i.e. the runner
    continues processing trials when the searcher finished.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testSearchAlgNotification
    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testSearchAlgFinished
    """

    experiment_spec = {"run": "__fake", "stop": {"training_iteration": 2}}
    experiments = [Experiment.from_json("test", experiment_spec)]
    search_alg = _MockSuggestionAlgorithm()
    searcher = search_alg.searcher
    search_alg.add_configurations(experiments)

    runner = TestTuneController(
        resource_manager_factory=lambda: resource_manager_cls(), search_alg=search_alg
    )

    # Run until trial is running
    while not search_alg.is_finished():
        runner.step()

    trials = runner.get_trials()

    # Make sure trial started
    while trials[0].status != Trial.RUNNING:
        runner.step()

    assert trials[0].status == Trial.RUNNING
    assert search_alg.is_finished()
    assert not runner.is_finished()

    # Run until everything finished
    while not runner.is_finished():
        runner.step()

    assert trials[0].status == Trial.TERMINATED
    assert search_alg.is_finished()
    assert runner.is_finished()

    assert searcher.counter["result"] == 1
    assert searcher.counter["complete"] == 1


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_search_alg_scheduler_stop(ray_start_4_cpus_2_gpus_extra, resource_manager_cls):
    """Check that a scheduler-issued stop also notifies the search algorithm.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testSearchAlgSchedulerInteraction  # noqa
    """

    class _MockScheduler(FIFOScheduler):
        def on_trial_result(self, *args, **kwargs):
            return TrialScheduler.STOP

    experiment_spec = {"run": "__fake", "stop": {"training_iteration": 5}}
    experiments = [Experiment.from_json("test", experiment_spec)]
    search_alg = _MockSuggestionAlgorithm()
    searcher = search_alg.searcher
    search_alg.add_configurations(experiments)

    runner = TestTuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        search_alg=search_alg,
        scheduler=_MockScheduler(),
    )

    trials = runner.get_trials()

    while not runner.is_finished():
        runner.step()

    # Result is not processed because trial stop takes precedence
    assert searcher.counter["result"] == 0
    # But on_trial_complete is triggered...
    assert searcher.counter["complete"] == 1
    # ... and still updates the last result.
    assert trials[0].last_result[TRAINING_ITERATION] == 1


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_search_alg_stalled(ray_start_4_cpus_2_gpus_extra, resource_manager_cls):
    """Checks that runner and searcher state is maintained when stalled.

    We use a concurrency limit of 1, meaning each trial is added one-by-one
    from the searchers.

    We then run three samples. During the second trial, we stall the searcher,
    which means we don't suggest new trials after it finished.

    In this case, the runner should still be considered "running". Once we unstall,
    the experiment finishes regularly.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testSearchAlgStalled
    """
    experiment_spec = {
        "run": "__fake",
        "num_samples": 3,
        "stop": {"training_iteration": 1},
    }
    experiments = [Experiment.from_json("test", experiment_spec)]
    search_alg = _MockSuggestionAlgorithm(max_concurrent=1)
    search_alg.add_configurations(experiments)
    searcher = search_alg.searcher
    runner = TestTuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        search_alg=search_alg,
    )
    runner.step()
    trials = runner.get_trials()
    while trials[0].status != Trial.TERMINATED:
        runner.step()

    # On next step, trials[1] is created
    runner.step()

    trials = runner.get_trials()

    while trials[1].status != Trial.RUNNING:
        runner.step()

    assert trials[1].status == Trial.RUNNING
    assert len(searcher.live_trials) == 1

    # Stall: We don't suggest new algorithms
    searcher.stall = True

    while trials[1].status != Trial.TERMINATED:
        runner.step()

    assert trials[1].status == Trial.TERMINATED
    assert len(searcher.live_trials) == 0

    assert all(trial.is_finished() for trial in trials)
    assert not search_alg.is_finished()
    assert not runner.is_finished()

    # Unstall
    searcher.stall = False

    # Create trials[2]
    runner.step()

    trials = runner.get_trials()

    while trials[2].status != Trial.RUNNING:
        runner.step()

    assert trials[2].status == Trial.RUNNING
    assert len(searcher.live_trials) == 1

    while trials[2].status != Trial.TERMINATED:
        runner.step()

    assert len(searcher.live_trials) == 0
    assert search_alg.is_finished()
    assert runner.is_finished()


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_search_alg_finishes(ray_start_4_cpus_2_gpus_extra, resource_manager_cls):
    """Empty SearchAlg changing state in `next_trials` does not crash.

    The search algorithm changes to ``finished`` mid-run. This should not
    affect processing of the experiment.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testSearchAlgFinishes
    """
    os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

    class FinishFastAlg(_MockSuggestionAlgorithm):
        _index = 0

        def next_trial(self):
            spec = self._experiment.spec
            trial = None
            if self._index < spec["num_samples"]:
                trial = Trial(
                    spec.get("run"),
                    stopping_criterion=spec.get("stop"),
                    storage=spec.get("storage"),
                )
            self._index += 1

            if self._index > 4:
                self.set_finished()

            return trial

        def suggest(self, trial_id):
            return {}

    experiment_spec = {
        "run": "__fake",
        "num_samples": 2,
        "stop": {"training_iteration": 1},
    }
    searcher = FinishFastAlg()
    experiments = [Experiment.from_json("test", experiment_spec)]
    searcher.add_configurations(experiments)

    runner = TestTuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        search_alg=searcher,
    )

    assert not runner.is_finished()

    while len(runner.get_trials()) < 2:
        runner.step()  # Launch 2 runs

    assert not searcher.is_finished()
    assert not runner.is_finished()

    searcher_finished_before = False
    while not runner.is_finished():
        runner.step()
        searcher_finished_before = searcher.is_finished()

    # searcher_finished_before will be True if the searcher was finished before
    # the controller.
    assert searcher_finished_before


# Todo (krfricke): Fix in next batch
@pytest.mark.skip("This test is currently flaky as it can fail due to timing issues.")
@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_searcher_save_restore(ray_start_8_cpus, resource_manager_cls, tmpdir):
    """Searchers state should be saved and restored in the experiment checkpoint.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testSearcherSaveRestore
    """

    def create_searcher():
        class TestSuggestion(Searcher):
            def __init__(self, index):
                self.index = index
                self.returned_result = []
                super().__init__(metric="episode_reward_mean", mode="max")

            def suggest(self, trial_id):
                self.index += 1
                return {"test_variable": self.index}

            def on_trial_complete(self, trial_id, result=None, **kwargs):
                self.returned_result.append(result)

            def save(self, checkpoint_path):
                with open(checkpoint_path, "wb") as f:
                    pickle.dump(self.__dict__, f)

            def restore(self, checkpoint_path):
                with open(checkpoint_path, "rb") as f:
                    self.__dict__.update(pickle.load(f))

        searcher = TestSuggestion(0)
        searcher = ConcurrencyLimiter(searcher, max_concurrent=2)
        searcher = Repeater(searcher, repeat=3, set_index=False)
        search_alg = SearchGenerator(searcher)
        experiment_spec = {
            "run": "__fake",
            "num_samples": 20,
            "config": {"sleep": 10},
            "stop": {"training_iteration": 2},
            "resources_per_trial": PlacementGroupFactory([{"CPU": 1}]),
        }
        experiments = [Experiment.from_json("test", experiment_spec)]
        search_alg.add_configurations(experiments)
        return search_alg

    searcher = create_searcher()

    runner = TestTuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        search_alg=searcher,
        checkpoint_period=-1,
        experiment_path=str(tmpdir),
    )

    while len(runner.get_trials()) < 6:
        runner.step()

    assert len(runner.get_trials()) == 6, [t.config for t in runner.get_trials()]
    runner.checkpoint()
    trials = runner.get_trials()
    [runner._schedule_trial_stop(t) for t in trials if t.status is not Trial.ERROR]

    runner.cleanup()

    del runner

    searcher = create_searcher()

    runner2 = TestTuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        search_alg=searcher,
        experiment_path=str(tmpdir),
        resume="LOCAL",
    )

    assert len(runner2.get_trials()) == 6, [t.config for t in runner2.get_trials()]

    def trial_statuses():
        return [t.status for t in runner2.get_trials()]

    def num_running_trials():
        return sum(t.status == Trial.RUNNING for t in runner2.get_trials())

    while num_running_trials() < 6:
        runner2.step()

    assert len(set(trial_statuses())) == 1
    assert Trial.RUNNING in trial_statuses()

    for i in range(20):
        runner2.step()
        assert 1 <= num_running_trials() <= 6

    evaluated = [t.evaluated_params["test_variable"] for t in runner2.get_trials()]
    count = Counter(evaluated)
    assert all(v <= 3 for v in count.values())


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
