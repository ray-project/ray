import time
import logging
import os
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

from freezegun import freeze_time

import ray
from ray.train import CheckpointConfig
from ray.air.execution import PlacementGroupResourceManager, FixedResourceManager
from ray.rllib import _register_all

from ray.tune.execution.ray_trial_executor import RayTrialExecutor
from ray.tune.search import BasicVariantGenerator
from ray.tune.experiment import Trial
from ray.tune.execution.trial_runner import TrialRunner
from ray.tune.search.repeater import Repeater
from ray.tune.search import Searcher, ConcurrencyLimiter
from ray.tune.search.search_generator import SearchGenerator
from ray.tune.syncer import SyncConfig, Syncer


class TrialRunnerTest3(unittest.TestCase):
    def _resourceManager(self):
        return PlacementGroupResourceManager()

    def setUp(self):
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "auto"  # Reset default

        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects
        if "CUDA_VISIBLE_DEVICES" in os.environ:
            del os.environ["CUDA_VISIBLE_DEVICES"]
        shutil.rmtree(self.tmpdir)

    @patch.dict(
        os.environ, {"TUNE_WARN_EXCESSIVE_EXPERIMENT_CHECKPOINT_SYNC_THRESHOLD_S": "2"}
    )
    def testCloudCheckpointForceWithNumToKeep(self):
        """Test that cloud syncing is forced if one of the trials has made more
        than num_to_keep checkpoints since last sync."""
        ray.init(num_cpus=3)

        class CustomSyncer(Syncer):
            def __init__(self, sync_period: float = float("inf")):
                super(CustomSyncer, self).__init__(sync_period=sync_period)
                self._sync_status = {}
                self.sync_up_counter = 0

            def sync_up(
                self, local_dir: str, remote_dir: str, exclude: list = None
            ) -> bool:
                self.sync_up_counter += 1
                return True

            def sync_down(
                self, remote_dir: str, local_dir: str, exclude: list = None
            ) -> bool:
                return True

            def delete(self, remote_dir: str) -> bool:
                pass

        num_to_keep = 2
        checkpoint_config = CheckpointConfig(
            num_to_keep=num_to_keep, checkpoint_frequency=1
        )
        syncer = CustomSyncer()

        runner = TrialRunner(
            experiment_path="fake://somewhere",
            sync_config=SyncConfig(syncer=syncer),
            trial_checkpoint_config=checkpoint_config,
            checkpoint_period=100,  # Only rely on forced syncing
            trial_executor=RayTrialExecutor(resource_manager=self._resourceManager()),
        )

        class CheckpointingTrial(Trial):
            def should_checkpoint(self):
                return True

        trial = CheckpointingTrial(
            "__fake",
            checkpoint_config=checkpoint_config,
            stopping_criterion={"training_iteration": 10},
        )
        runner.add_trial(trial)

        # also check if the warning is printed
        buffer = []
        from ray.tune.execution.experiment_state import logger

        with patch.object(logger, "warning", lambda x: buffer.append(x)):
            while not runner.is_finished():
                runner.step()
        assert any("syncing has been triggered multiple" in x for x in buffer)

        # We should sync 6 times:
        # The first checkpoint happens when the experiment starts,
        # since no checkpoints have happened yet
        # (This corresponds to the new_trial event in the runner loop)
        # Then, every num_to_keep=2 checkpoints, we should perform a forced checkpoint
        # which results in 5 more checkpoints (running for 10 iterations),
        # giving a total of 6
        assert syncer.sync_up_counter == 6

    def getHangingSyncer(self, sync_period: float, sync_timeout: float):
        def _hanging_sync_up_command(*args, **kwargs):
            time.sleep(200)

        from ray.tune.syncer import _DefaultSyncer

        class HangingSyncer(_DefaultSyncer):
            def __init__(self, sync_period: float, sync_timeout: float):
                super(HangingSyncer, self).__init__(
                    sync_period=sync_period, sync_timeout=sync_timeout
                )
                self.sync_up_counter = 0

            def sync_up(
                self, local_dir: str, remote_dir: str, exclude: list = None
            ) -> bool:
                self.sync_up_counter += 1
                super(HangingSyncer, self).sync_up(local_dir, remote_dir, exclude)

            def _sync_up_command(self, local_path: str, uri: str, exclude: list = None):
                return _hanging_sync_up_command, {}

        return HangingSyncer(sync_period=sync_period, sync_timeout=sync_timeout)

    def testForcedCloudCheckpointSyncTimeout(self):
        """Test that trial runner experiment checkpointing with forced cloud syncing
        times out correctly when the sync process hangs."""
        ray.init(num_cpus=3)

        syncer = self.getHangingSyncer(sync_period=60, sync_timeout=0.5)
        runner = TrialRunner(
            experiment_path="fake://somewhere/exp",
            sync_config=SyncConfig(syncer=syncer),
        )
        # Checkpoint for the first time starts the first sync in the background
        runner.checkpoint(force=True)
        assert syncer.sync_up_counter == 1

        buffer = []
        logger = logging.getLogger("ray.tune.execution.experiment_state")
        with patch.object(logger, "warning", lambda x: buffer.append(x)):
            # The second checkpoint will log a warning about the previous sync
            # timing out. Then, it will launch a new sync process in the background.
            runner.checkpoint(force=True)
        assert any("timed out" in x for x in buffer)
        assert syncer.sync_up_counter == 2

    def testPeriodicCloudCheckpointSyncTimeout(self):
        """Test that trial runner experiment checkpointing with the default periodic
        cloud syncing times out and retries correctly when the sync process hangs."""
        ray.init(num_cpus=3)

        sync_period = 60
        syncer = self.getHangingSyncer(sync_period=sync_period, sync_timeout=0.5)
        runner = TrialRunner(
            experiment_path="fake://somewhere/exp",
            sync_config=SyncConfig(syncer=syncer),
        )

        with freeze_time() as frozen:
            runner.checkpoint()
            assert syncer.sync_up_counter == 1

            frozen.tick(sync_period / 2)
            # Cloud sync has already timed out, but we shouldn't retry until
            # the next sync_period
            runner.checkpoint()
            assert syncer.sync_up_counter == 1

            frozen.tick(sync_period / 2)
            # We've now reached the sync_period - a new sync process should be
            # started, with the old one timing out
            buffer = []
            logger = logging.getLogger("ray.tune.syncer")
            with patch.object(logger, "warning", lambda x: buffer.append(x)):
                runner.checkpoint()
            assert any("did not finish running within the timeout" in x for x in buffer)
            assert syncer.sync_up_counter == 2


class FixedResourceTrialRunnerTest3(TrialRunnerTest3):
    def _resourceManager(self):
        return FixedResourceManager()


class SearchAlgorithmTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=4, num_gpus=0, local_mode=True, include_dashboard=False)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()
        _register_all()

    def testNestedSuggestion(self):
        class TestSuggestion(Searcher):
            def suggest(self, trial_id):
                return {"a": {"b": {"c": {"d": 4, "e": 5}}}}

        searcher = TestSuggestion()
        alg = SearchGenerator(searcher)
        alg.add_configurations({"test": {"run": "__fake"}})
        trial = alg.next_trial()
        self.assertTrue("e=5" in trial.experiment_tag)
        self.assertTrue("d=4" in trial.experiment_tag)

    def _test_repeater(self, num_samples, repeat):
        class TestSuggestion(Searcher):
            index = 0

            def suggest(self, trial_id):
                self.index += 1
                return {"test_variable": 5 + self.index}

            def on_trial_complete(self, *args, **kwargs):
                return

        searcher = TestSuggestion(metric="episode_reward_mean")
        repeat_searcher = Repeater(searcher, repeat=repeat, set_index=False)
        alg = SearchGenerator(repeat_searcher)
        experiment_spec = {
            "run": "__fake",
            "num_samples": num_samples,
            "stop": {"training_iteration": 1},
        }
        alg.add_configurations({"test": experiment_spec})
        runner = TrialRunner(search_alg=alg)
        while not runner.is_finished():
            runner.step()

        return runner.get_trials()

    def testRepeat1(self):
        trials = self._test_repeater(num_samples=2, repeat=1)
        self.assertEqual(len(trials), 2)
        parameter_set = {t.evaluated_params["test_variable"] for t in trials}
        self.assertEqual(len(parameter_set), 2)

    def testRepeat4(self):
        trials = self._test_repeater(num_samples=12, repeat=4)
        self.assertEqual(len(trials), 12)
        parameter_set = {t.evaluated_params["test_variable"] for t in trials}
        self.assertEqual(len(parameter_set), 3)

    def testOddRepeat(self):
        trials = self._test_repeater(num_samples=11, repeat=5)
        self.assertEqual(len(trials), 11)
        parameter_set = {t.evaluated_params["test_variable"] for t in trials}
        self.assertEqual(len(parameter_set), 3)

    def testSetGetRepeater(self):
        class TestSuggestion(Searcher):
            def __init__(self, index):
                self.index = index
                self.returned_result = []
                super().__init__(metric="result", mode="max")

            def suggest(self, trial_id):
                self.index += 1
                return {"score": self.index}

            def on_trial_complete(self, trial_id, result=None, **kwargs):
                self.returned_result.append(result)

        searcher = TestSuggestion(0)
        repeater1 = Repeater(searcher, repeat=3, set_index=False)
        for i in range(3):
            assert repeater1.suggest(f"test_{i}")["score"] == 1
        for i in range(2):  # An incomplete set of results
            assert repeater1.suggest(f"test_{i}_2")["score"] == 2

        # Restore a new one
        state = repeater1.get_state()
        del repeater1
        new_repeater = Repeater(searcher, repeat=1, set_index=True)
        new_repeater.set_state(state)
        assert new_repeater.repeat == 3
        assert new_repeater.suggest("test_2_2")["score"] == 2
        assert new_repeater.suggest("test_x")["score"] == 3

        # Report results
        for i in range(3):
            new_repeater.on_trial_complete(f"test_{i}", {"result": 2})

        for i in range(3):
            new_repeater.on_trial_complete(f"test_{i}_2", {"result": -i * 10})

        assert len(new_repeater.searcher.returned_result) == 2
        assert new_repeater.searcher.returned_result[-1] == {"result": -10}

        # Finish the rest of the last trial group
        new_repeater.on_trial_complete("test_x", {"result": 3})
        assert new_repeater.suggest("test_y")["score"] == 3
        new_repeater.on_trial_complete("test_y", {"result": 3})
        assert len(new_repeater.searcher.returned_result) == 2
        assert new_repeater.suggest("test_z")["score"] == 3
        new_repeater.on_trial_complete("test_z", {"result": 3})
        assert len(new_repeater.searcher.returned_result) == 3
        assert new_repeater.searcher.returned_result[-1] == {"result": 3}

    def testSetGetLimiter(self):
        class TestSuggestion(Searcher):
            def __init__(self, index):
                self.index = index
                self.returned_result = []
                super().__init__(metric="result", mode="max")

            def suggest(self, trial_id):
                self.index += 1
                return {"score": self.index}

            def on_trial_complete(self, trial_id, result=None, **kwargs):
                self.returned_result.append(result)

        searcher = TestSuggestion(0)
        limiter = ConcurrencyLimiter(searcher, max_concurrent=2)
        assert limiter.suggest("test_1")["score"] == 1
        assert limiter.suggest("test_2")["score"] == 2
        assert limiter.suggest("test_3") is None

        state = limiter.get_state()
        del limiter
        limiter2 = ConcurrencyLimiter(searcher, max_concurrent=3)
        limiter2.set_state(state)
        assert limiter2.suggest("test_4") is None
        assert limiter2.suggest("test_5") is None
        limiter2.on_trial_complete("test_1", {"result": 3})
        limiter2.on_trial_complete("test_2", {"result": 3})
        assert limiter2.suggest("test_3")["score"] == 3

    def testBasicVariantLimiter(self):
        search_alg = BasicVariantGenerator(max_concurrent=2)

        experiment_spec = {
            "run": "__fake",
            "num_samples": 5,
            "stop": {"training_iteration": 1},
        }
        search_alg.add_configurations({"test": experiment_spec})

        trial1 = search_alg.next_trial()
        self.assertTrue(trial1)

        trial2 = search_alg.next_trial()
        self.assertTrue(trial2)

        # Returns None because of limiting
        trial3 = search_alg.next_trial()
        self.assertFalse(trial3)

        # Finish trial, now trial 3 should be created
        search_alg.on_trial_complete(trial1.trial_id, None, False)
        trial3 = search_alg.next_trial()
        self.assertTrue(trial3)

        trial4 = search_alg.next_trial()
        self.assertFalse(trial4)

        search_alg.on_trial_complete(trial2.trial_id, None, False)
        search_alg.on_trial_complete(trial3.trial_id, None, False)

        trial4 = search_alg.next_trial()
        self.assertTrue(trial4)

        trial5 = search_alg.next_trial()
        self.assertTrue(trial5)

        search_alg.on_trial_complete(trial4.trial_id, None, False)

        # Should also be None because search is finished
        trial6 = search_alg.next_trial()
        self.assertFalse(trial6)

    def testBatchLimiter(self):
        class TestSuggestion(Searcher):
            def __init__(self, index):
                self.index = index
                self.returned_result = []
                super().__init__(metric="result", mode="max")

            def suggest(self, trial_id):
                self.index += 1
                return {"score": self.index}

            def on_trial_complete(self, trial_id, result=None, **kwargs):
                self.returned_result.append(result)

        searcher = TestSuggestion(0)
        limiter = ConcurrencyLimiter(searcher, max_concurrent=2, batch=True)
        assert limiter.suggest("test_1")["score"] == 1
        assert limiter.suggest("test_2")["score"] == 2
        assert limiter.suggest("test_3") is None

        limiter.on_trial_complete("test_1", {"result": 3})
        assert limiter.suggest("test_3") is None
        limiter.on_trial_complete("test_2", {"result": 3})
        assert limiter.suggest("test_3") is not None

    def testBatchLimiterInfiniteLoop(self):
        """Check whether an infinite loop when less than max_concurrent trials
        are suggested with batch mode is avoided.
        """

        class TestSuggestion(Searcher):
            def __init__(self, index, max_suggestions=10):
                self.index = index
                self.max_suggestions = max_suggestions
                self.returned_result = []
                super().__init__(metric="result", mode="max")

            def suggest(self, trial_id):
                self.index += 1
                if self.index > self.max_suggestions:
                    return None
                return {"score": self.index}

            def on_trial_complete(self, trial_id, result=None, **kwargs):
                self.returned_result.append(result)
                self.index = 0

        searcher = TestSuggestion(0, 2)
        limiter = ConcurrencyLimiter(searcher, max_concurrent=5, batch=True)
        limiter.suggest("test_1")
        limiter.suggest("test_2")
        limiter.suggest("test_3")  # TestSuggestion return None

        limiter.on_trial_complete("test_1", {"result": 3})
        limiter.on_trial_complete("test_2", {"result": 3})
        assert limiter.searcher.returned_result

        searcher = TestSuggestion(0, 10)
        limiter = ConcurrencyLimiter(searcher, max_concurrent=5, batch=True)
        limiter.suggest("test_1")
        limiter.suggest("test_2")
        limiter.suggest("test_3")

        limiter.on_trial_complete("test_1", {"result": 3})
        limiter.on_trial_complete("test_2", {"result": 3})
        assert not limiter.searcher.returned_result

    def testSetMaxConcurrency(self):
        """Test whether ``set_max_concurrency`` is called by the
        ``ConcurrencyLimiter`` and works correctly.
        """

        class TestSuggestion(Searcher):
            def __init__(self, index):
                self.index = index
                self.returned_result = []
                self._max_concurrent = 1
                super().__init__(metric="result", mode="max")

            def suggest(self, trial_id):
                self.index += 1
                return {"score": self.index}

            def on_trial_complete(self, trial_id, result=None, **kwargs):
                self.returned_result.append(result)

            def set_max_concurrency(self, max_concurrent: int) -> bool:
                self._max_concurrent = max_concurrent
                return True

        searcher = TestSuggestion(0)
        limiter_max_concurrent = 2
        limiter = ConcurrencyLimiter(
            searcher, max_concurrent=limiter_max_concurrent, batch=True
        )
        assert limiter.searcher._max_concurrent == limiter_max_concurrent
        # Since set_max_concurrency returns True, ConcurrencyLimiter should not
        # be limiting concurrency itself
        assert not limiter._limit_concurrency
        assert limiter.suggest("test_1")["score"] == 1
        assert limiter.suggest("test_2")["score"] == 2
        assert limiter.suggest("test_3")["score"] == 3


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", "--reruns", "3", __file__]))
