import time
from collections import Counter
import os
import pickle
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

import ray
from ray.rllib import _register_all

from ray.tune import TuneError
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.result import TRAINING_ITERATION
from ray.tune.schedulers import TrialScheduler, FIFOScheduler
from ray.tune.experiment import Experiment
from ray.tune.suggest import BasicVariantGenerator
from ray.tune.trial import Trial
from ray.tune.trial_runner import TrialRunner
from ray.tune.resources import Resources, json_to_resources, resources_to_json
from ray.tune.suggest.repeater import Repeater
from ray.tune.suggest._mock import _MockSuggestionAlgorithm
from ray.tune.suggest.suggestion import Searcher, ConcurrencyLimiter
from ray.tune.suggest.search_generator import SearchGenerator
from ray.tune.syncer import SyncConfig
from ray.tune.tests.utils_for_test_trial_runner import TrialResultObserver


class TrialRunnerTest3(unittest.TestCase):
    def setUp(self):
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "auto"  # Reset default

        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects
        if "CUDA_VISIBLE_DEVICES" in os.environ:
            del os.environ["CUDA_VISIBLE_DEVICES"]
        shutil.rmtree(self.tmpdir)

    def testStepHook(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner()

        def on_step_begin(self, trialrunner):
            self._resource_updater.update_avail_resources()
            cnt = self.pre_step if hasattr(self, "pre_step") else 0
            self.pre_step = cnt + 1

        def on_step_end(self, trialrunner):
            cnt = self.pre_step if hasattr(self, "post_step") else 0
            self.post_step = 1 + cnt

        import types

        runner.trial_executor.on_step_begin = types.MethodType(
            on_step_begin, runner.trial_executor
        )
        runner.trial_executor.on_step_end = types.MethodType(
            on_step_end, runner.trial_executor
        )

        kwargs = {
            "stopping_criterion": {"training_iteration": 5},
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        runner.step()
        self.assertEqual(runner.trial_executor.pre_step, 1)
        self.assertEqual(runner.trial_executor.post_step, 1)

    def testStopTrial(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {"training_iteration": 5},
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs),
        ]
        for t in trials:
            runner.add_trial(t)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        # Stop trial while running
        runner.stop_trial(trials[0])
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(trials[-1].status, Trial.PENDING)

        # Stop trial while pending
        runner.stop_trial(trials[-1])
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(trials[-1].status, Trial.TERMINATED)

        time.sleep(2)  # Wait for stopped placement group to free resources
        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(trials[2].status, Trial.RUNNING)
        self.assertEqual(trials[-1].status, Trial.TERMINATED)

    def testSearchAlgNotification(self):
        """Checks notification of trial to the Search Algorithm."""
        os.environ["TUNE_RESULT_BUFFER_LENGTH"] = "1"  # Don't finish early
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

        ray.init(num_cpus=4, num_gpus=2)
        experiment_spec = {"run": "__fake", "stop": {"training_iteration": 2}}
        experiments = [Experiment.from_json("test", experiment_spec)]
        search_alg = _MockSuggestionAlgorithm()
        searcher = search_alg.searcher
        search_alg.add_configurations(experiments)
        runner = TrialRunner(search_alg=search_alg)

        while not runner.is_finished():
            runner.step()

        self.assertEqual(searcher.counter["result"], 1)
        self.assertEqual(searcher.counter["complete"], 1)

    def testSearchAlgFinished(self):
        """Checks that SearchAlg is Finished before all trials are done."""
        ray.init(num_cpus=4, local_mode=True, include_dashboard=False)
        experiment_spec = {"run": "__fake", "stop": {"training_iteration": 1}}
        experiments = [Experiment.from_json("test", experiment_spec)]
        searcher = _MockSuggestionAlgorithm()
        searcher.add_configurations(experiments)
        runner = TrialRunner(search_alg=searcher)
        runner.step()
        trials = runner.get_trials()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertTrue(searcher.is_finished())
        self.assertFalse(runner.is_finished())

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(len(searcher.live_trials), 0)
        self.assertTrue(searcher.is_finished())
        self.assertTrue(runner.is_finished())

    def testSearchAlgSchedulerInteraction(self):
        """Checks that TrialScheduler killing trial will notify SearchAlg."""

        class _MockScheduler(FIFOScheduler):
            def on_trial_result(self, *args, **kwargs):
                return TrialScheduler.STOP

        ray.init(num_cpus=4, local_mode=True, include_dashboard=False)
        experiment_spec = {"run": "__fake", "stop": {"training_iteration": 2}}
        experiments = [Experiment.from_json("test", experiment_spec)]
        searcher = _MockSuggestionAlgorithm()
        searcher.add_configurations(experiments)
        runner = TrialRunner(search_alg=searcher, scheduler=_MockScheduler())
        runner.step()
        trials = runner.get_trials()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertTrue(searcher.is_finished())
        self.assertFalse(runner.is_finished())

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(len(searcher.live_trials), 0)
        self.assertTrue(searcher.is_finished())
        self.assertTrue(runner.is_finished())

    def testSearchAlgStalled(self):
        """Checks that runner and searcher state is maintained when stalled."""
        ray.init(num_cpus=4, num_gpus=2)
        experiment_spec = {
            "run": "__fake",
            "num_samples": 3,
            "stop": {"training_iteration": 1},
        }
        experiments = [Experiment.from_json("test", experiment_spec)]
        search_alg = _MockSuggestionAlgorithm(max_concurrent=1)
        search_alg.add_configurations(experiments)
        searcher = search_alg.searcher
        runner = TrialRunner(search_alg=search_alg)
        runner.step()
        trials = runner.get_trials()
        while trials[0].status != Trial.TERMINATED:
            runner.step()

        runner.step()
        trials = runner.get_trials()
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(len(searcher.live_trials), 1)

        searcher.stall = True

        while trials[1].status != Trial.TERMINATED:
            runner.step()
        self.assertEqual(trials[1].status, Trial.TERMINATED)
        self.assertEqual(len(searcher.live_trials), 0)

        self.assertTrue(all(trial.is_finished() for trial in trials))
        self.assertFalse(search_alg.is_finished())
        self.assertFalse(runner.is_finished())

        searcher.stall = False

        runner.step()
        trials = runner.get_trials()
        self.assertEqual(trials[2].status, Trial.RUNNING)
        self.assertEqual(len(searcher.live_trials), 1)

        while trials[2].status != Trial.TERMINATED:
            runner.step()

        self.assertEqual(len(searcher.live_trials), 0)
        self.assertTrue(search_alg.is_finished())
        self.assertTrue(runner.is_finished())

    def testSearchAlgFinishes(self):
        """Empty SearchAlg changing state in `next_trials` does not crash."""
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

        class FinishFastAlg(_MockSuggestionAlgorithm):
            _index = 0

            def next_trial(self):
                spec = self._experiment.spec
                trial = None
                if self._index < spec["num_samples"]:
                    trial = Trial(spec.get("run"), stopping_criterion=spec.get("stop"))
                self._index += 1

                if self._index > 4:
                    self.set_finished()

                return trial

            def suggest(self, trial_id):
                return {}

        ray.init(num_cpus=2, local_mode=True, include_dashboard=False)
        experiment_spec = {
            "run": "__fake",
            "num_samples": 2,
            "stop": {"training_iteration": 1},
        }
        searcher = FinishFastAlg()
        experiments = [Experiment.from_json("test", experiment_spec)]
        searcher.add_configurations(experiments)

        runner = TrialRunner(search_alg=searcher)
        self.assertFalse(runner.is_finished())
        runner.step()  # This launches a new run
        runner.step()  # This launches a 2nd run
        self.assertFalse(searcher.is_finished())
        self.assertFalse(runner.is_finished())
        runner.step()  # This kills the first run
        self.assertFalse(searcher.is_finished())
        self.assertFalse(runner.is_finished())
        runner.step()  # This kills the 2nd run
        self.assertFalse(searcher.is_finished())
        self.assertFalse(runner.is_finished())
        runner.step()  # this converts self._finished to True
        self.assertTrue(searcher.is_finished())
        self.assertRaises(TuneError, runner.step)

    def testSearcherSaveRestore(self):
        ray.init(num_cpus=8, local_mode=True)

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
                "stop": {"training_iteration": 2},
            }
            experiments = [Experiment.from_json("test", experiment_spec)]
            search_alg.add_configurations(experiments)
            return search_alg

        searcher = create_searcher()
        runner = TrialRunner(
            search_alg=searcher, local_checkpoint_dir=self.tmpdir, checkpoint_period=-1
        )
        for i in range(6):
            runner.step()

        assert len(runner.get_trials()) == 6, [t.config for t in runner.get_trials()]
        runner.checkpoint()
        trials = runner.get_trials()
        [
            runner.trial_executor.stop_trial(t)
            for t in trials
            if t.status is not Trial.ERROR
        ]
        del runner
        # stop_all(runner.get_trials())

        searcher = create_searcher()
        runner2 = TrialRunner(
            search_alg=searcher, local_checkpoint_dir=self.tmpdir, resume="LOCAL"
        )
        assert len(runner2.get_trials()) == 6, [t.config for t in runner2.get_trials()]

        def trial_statuses():
            return [t.status for t in runner2.get_trials()]

        def num_running_trials():
            return sum(t.status == Trial.RUNNING for t in runner2.get_trials())

        for i in range(6):
            runner2.step()
        assert len(set(trial_statuses())) == 1
        assert Trial.RUNNING in trial_statuses()
        for i in range(20):
            runner2.step()
            assert 1 <= num_running_trials() <= 6
        evaluated = [t.evaluated_params["test_variable"] for t in runner2.get_trials()]
        count = Counter(evaluated)
        assert all(v <= 3 for v in count.values())

    def testTrialErrorResumeFalse(self):
        ray.init(num_cpus=3, local_mode=True, include_dashboard=False)
        runner = TrialRunner(local_checkpoint_dir=self.tmpdir)
        kwargs = {
            "stopping_criterion": {"training_iteration": 4},
            "resources": Resources(cpu=1, gpu=0),
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

        new_runner = TrialRunner(resume=True, local_checkpoint_dir=self.tmpdir)
        assert len(new_runner.get_trials()) == 3
        assert Trial.ERROR in (t.status for t in new_runner.get_trials())

    def testTrialErrorResumeTrue(self):
        ray.init(num_cpus=3, local_mode=True, include_dashboard=False)
        runner = TrialRunner(local_checkpoint_dir=self.tmpdir)
        kwargs = {
            "stopping_criterion": {"training_iteration": 4},
            "resources": Resources(cpu=1, gpu=0),
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

        new_runner = TrialRunner(
            resume="ERRORED_ONLY", local_checkpoint_dir=self.tmpdir
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

    def testTrialSaveRestore(self):
        """Creates different trials to test runner.checkpoint/restore."""
        ray.init(num_cpus=3)

        runner = TrialRunner(local_checkpoint_dir=self.tmpdir, checkpoint_period=0)
        trials = [
            Trial(
                "__fake",
                trial_id="trial_terminate",
                stopping_criterion={"training_iteration": 1},
                checkpoint_freq=1,
            )
        ]
        runner.add_trial(trials[0])
        while not runner.is_finished():
            # Start trial, process result, dispatch save and process save.
            runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)

        trials += [
            Trial(
                "__fake",
                trial_id="trial_fail",
                stopping_criterion={"training_iteration": 3},
                checkpoint_freq=1,
                config={"mock_error": True},
            )
        ]
        runner.add_trial(trials[1])
        while not runner.is_finished():
            # Start trial,
            # Process result,
            # Dispatch save,
            # Process save and
            # Error.
            runner.step()
        self.assertEqual(trials[1].status, Trial.ERROR)

        trials += [
            Trial(
                "__fake",
                trial_id="trial_succ",
                stopping_criterion={"training_iteration": 2},
                checkpoint_freq=1,
            )
        ]
        runner.add_trial(trials[2])
        runner.step()  # Start trial
        self.assertEqual(len(runner.trial_executor.get_checkpoints()), 3)
        self.assertEqual(trials[2].status, Trial.RUNNING)

        runner2 = TrialRunner(resume="LOCAL", local_checkpoint_dir=self.tmpdir)
        for tid in ["trial_terminate", "trial_fail"]:
            original_trial = runner.get_trial(tid)
            restored_trial = runner2.get_trial(tid)
            self.assertEqual(original_trial.status, restored_trial.status)

        restored_trial = runner2.get_trial("trial_succ")
        self.assertEqual(Trial.PENDING, restored_trial.status)

        while not runner2.is_finished():
            # Start trial,
            # Process result, dispatch save
            # Process save
            # Process result, dispatch save
            # Process save.
            runner2.step()
        self.assertEqual(restored_trial.status, Trial.TERMINATED)

    def testTrialNoCheckpointSave(self):
        """Check that non-checkpointing trials *are* saved."""
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

        ray.init(num_cpus=3)

        runner = TrialRunner(local_checkpoint_dir=self.tmpdir, checkpoint_period=0)
        runner.add_trial(
            Trial(
                "__fake",
                trial_id="non_checkpoint",
                stopping_criterion={"training_iteration": 2},
            )
        )

        while not all(t.status == Trial.TERMINATED for t in runner.get_trials()):
            runner.step()

        runner.add_trial(
            Trial(
                "__fake",
                trial_id="checkpoint",
                checkpoint_at_end=True,
                stopping_criterion={"training_iteration": 2},
            )
        )

        while not all(t.status == Trial.TERMINATED for t in runner.get_trials()):
            runner.step()

        runner.add_trial(
            Trial(
                "__fake",
                trial_id="pending",
                stopping_criterion={"training_iteration": 2},
            )
        )

        old_trials = runner.get_trials()
        while not old_trials[2].has_reported_at_least_once:
            runner.step()

        runner2 = TrialRunner(resume="LOCAL", local_checkpoint_dir=self.tmpdir)
        new_trials = runner2.get_trials()
        self.assertEqual(len(new_trials), 3)
        self.assertTrue(runner2.get_trial("non_checkpoint").status == Trial.TERMINATED)
        self.assertTrue(runner2.get_trial("checkpoint").status == Trial.TERMINATED)
        self.assertTrue(runner2.get_trial("pending").status == Trial.PENDING)
        self.assertTrue(runner2.get_trial("pending").has_reported_at_least_once)
        runner2.step()

    def testCheckpointWithFunction(self):
        ray.init(num_cpus=2)

        trial = Trial(
            "__fake",
            config={
                "callbacks": {
                    "on_episode_start": lambda i: i,
                }
            },
            checkpoint_freq=1,
        )
        runner = TrialRunner(local_checkpoint_dir=self.tmpdir, checkpoint_period=0)
        runner.add_trial(trial)
        for _ in range(5):
            runner.step()
        # force checkpoint
        runner.checkpoint()
        runner2 = TrialRunner(resume="LOCAL", local_checkpoint_dir=self.tmpdir)
        new_trial = runner2.get_trials()[0]
        self.assertTrue("callbacks" in new_trial.config)
        self.assertTrue("on_episode_start" in new_trial.config["callbacks"])

    def testCheckpointOverwrite(self):
        def count_checkpoints(cdir):
            return sum(
                (fname.startswith("experiment_state") and fname.endswith(".json"))
                for fname in os.listdir(cdir)
            )

        ray.init(num_cpus=2)

        trial = Trial("__fake", checkpoint_freq=1)
        tmpdir = tempfile.mkdtemp()
        runner = TrialRunner(local_checkpoint_dir=tmpdir, checkpoint_period=0)
        runner.add_trial(trial)
        for _ in range(5):
            runner.step()
        # force checkpoint
        runner.checkpoint()
        self.assertEqual(count_checkpoints(tmpdir), 1)

        runner2 = TrialRunner(resume="LOCAL", local_checkpoint_dir=tmpdir)
        for _ in range(5):
            runner2.step()
        self.assertEqual(count_checkpoints(tmpdir), 2)

        runner2.checkpoint()
        self.assertEqual(count_checkpoints(tmpdir), 2)
        shutil.rmtree(tmpdir)

    def testCheckpointFreqBuffered(self):
        os.environ["TUNE_RESULT_BUFFER_LENGTH"] = "7"
        os.environ["TUNE_RESULT_BUFFER_MIN_TIME_S"] = "1"

        def num_checkpoints(trial):
            return sum(
                item.startswith("checkpoint_") for item in os.listdir(trial.logdir)
            )

        ray.init(num_cpus=2)

        trial = Trial("__fake", checkpoint_freq=3)
        runner = TrialRunner(local_checkpoint_dir=self.tmpdir, checkpoint_period=0)
        runner.add_trial(trial)

        runner.step()  # start trial
        runner.step()  # run iteration 1-3
        runner.step()  # process save
        self.assertEqual(trial.last_result[TRAINING_ITERATION], 3)
        self.assertEqual(num_checkpoints(trial), 1)

        runner.step()  # run iteration 4-6
        runner.step()  # process save
        self.assertEqual(trial.last_result[TRAINING_ITERATION], 6)
        self.assertEqual(num_checkpoints(trial), 2)

        runner.step()  # run iteration 7-9
        runner.step()  # process save
        self.assertEqual(trial.last_result[TRAINING_ITERATION], 9)
        self.assertEqual(num_checkpoints(trial), 3)

    def testCheckpointAtEndNotBuffered(self):
        os.environ["TUNE_RESULT_BUFFER_LENGTH"] = "7"
        os.environ["TUNE_RESULT_BUFFER_MIN_TIME_S"] = "0.5"

        def num_checkpoints(trial):
            return sum(
                item.startswith("checkpoint_") for item in os.listdir(trial.logdir)
            )

        ray.init(num_cpus=2)

        trial = Trial(
            "__fake",
            checkpoint_at_end=True,
            stopping_criterion={"training_iteration": 4},
        )
        observer = TrialResultObserver()
        runner = TrialRunner(
            local_checkpoint_dir=self.tmpdir,
            checkpoint_period=0,
            trial_executor=RayTrialExecutor(result_buffer_length=7),
            callbacks=[observer],
        )
        runner.add_trial(trial)

        while not observer.just_received_a_result():
            runner.step()
        self.assertEqual(trial.last_result[TRAINING_ITERATION], 1)
        self.assertEqual(num_checkpoints(trial), 0)

        while True:
            runner.step()
            if observer.just_received_a_result():
                break
        self.assertEqual(trial.last_result[TRAINING_ITERATION], 2)
        self.assertEqual(num_checkpoints(trial), 0)

        while True:
            runner.step()
            if observer.just_received_a_result():
                break
        self.assertEqual(trial.last_result[TRAINING_ITERATION], 3)
        self.assertEqual(num_checkpoints(trial), 0)

        while True:
            runner.step()
            if observer.just_received_a_result():
                break
        self.assertEqual(trial.last_result[TRAINING_ITERATION], 4)

        while not runner.is_finished():
            runner.step()
        self.assertEqual(num_checkpoints(trial), 1)

    def testUserCheckpoint(self):
        os.environ["TUNE_RESULT_BUFFER_LENGTH"] = "1"  # Don't finish early
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

        ray.init(num_cpus=3)
        runner = TrialRunner(local_checkpoint_dir=self.tmpdir, checkpoint_period=0)
        runner.add_trial(Trial("__fake", config={"user_checkpoint_freq": 2}))
        trials = runner.get_trials()

        runner.step()  # Start trial
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.set_info.remote(1)), 1)
        runner.step()  # Process result
        self.assertFalse(trials[0].has_checkpoint())
        runner.step()  # Process result
        self.assertFalse(trials[0].has_checkpoint())
        runner.step()  # Process result, dispatch save
        runner.step()  # Process save
        self.assertTrue(trials[0].has_checkpoint())

        runner2 = TrialRunner(resume="LOCAL", local_checkpoint_dir=self.tmpdir)
        runner2.step()  # 5: Start trial and dispatch restore
        trials2 = runner2.get_trials()
        self.assertEqual(ray.get(trials2[0].runner.get_info.remote()), 1)

    def testUserCheckpointBuffered(self):
        os.environ["TUNE_RESULT_BUFFER_LENGTH"] = "8"
        os.environ["TUNE_RESULT_BUFFER_MIN_TIME_S"] = "1"

        def num_checkpoints(trial):
            return sum(
                item.startswith("checkpoint_") for item in os.listdir(trial.logdir)
            )

        ray.init(num_cpus=3)
        runner = TrialRunner(local_checkpoint_dir=self.tmpdir, checkpoint_period=0)
        runner.add_trial(Trial("__fake", config={"user_checkpoint_freq": 10}))
        trials = runner.get_trials()

        runner.step()  # Start trial, schedule 1-8
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.set_info.remote(1)), 1)
        self.assertEqual(num_checkpoints(trials[0]), 0)

        runner.step()  # Process results 0-8, schedule 9-11 (CP)
        self.assertEqual(trials[0].last_result.get(TRAINING_ITERATION), 8)
        self.assertFalse(trials[0].has_checkpoint())
        self.assertEqual(num_checkpoints(trials[0]), 0)

        runner.step()  # Process results 9-11
        runner.step()  # handle CP, schedule 12-19
        self.assertEqual(trials[0].last_result.get(TRAINING_ITERATION), 11)
        self.assertTrue(trials[0].has_checkpoint())
        self.assertEqual(num_checkpoints(trials[0]), 1)

        runner.step()  # Process results 12-19, schedule 20-21
        self.assertEqual(trials[0].last_result.get(TRAINING_ITERATION), 19)
        self.assertTrue(trials[0].has_checkpoint())
        self.assertEqual(num_checkpoints(trials[0]), 1)

        runner.step()  # Process results 20-21
        runner.step()  # handle CP, schedule 21-29
        self.assertEqual(trials[0].last_result.get(TRAINING_ITERATION), 21)
        self.assertTrue(trials[0].has_checkpoint())
        self.assertEqual(num_checkpoints(trials[0]), 2)

        runner.step()  # Process results 21-29, schedule 30-31
        self.assertEqual(trials[0].last_result.get(TRAINING_ITERATION), 29)
        self.assertTrue(trials[0].has_checkpoint())
        self.assertTrue(trials[0].has_checkpoint())
        self.assertEqual(num_checkpoints(trials[0]), 2)

    @patch("ray.tune.syncer.SYNC_PERIOD", 0)
    def testCheckpointAutoPeriod(self):
        ray.init(num_cpus=3)

        # This makes checkpointing take 2 seconds.
        def sync_up(source, target, exclude=None):
            time.sleep(2)
            return True

        runner = TrialRunner(
            local_checkpoint_dir=self.tmpdir,
            checkpoint_period="auto",
            sync_config=SyncConfig(upload_dir="fake", syncer=sync_up),
            remote_checkpoint_dir="fake",
        )
        runner.add_trial(Trial("__fake", config={"user_checkpoint_freq": 1}))

        runner.step()  # Run one step, this will trigger checkpointing

        self.assertGreaterEqual(runner._checkpoint_manager._checkpoint_period, 38.0)


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


class ResourcesTest(unittest.TestCase):
    def testSubtraction(self):
        resource_1 = Resources(
            1,
            0,
            0,
            1,
            custom_resources={"a": 1, "b": 2},
            extra_custom_resources={"a": 1, "b": 1},
        )
        resource_2 = Resources(
            1,
            0,
            0,
            1,
            custom_resources={"a": 1, "b": 2},
            extra_custom_resources={"a": 1, "b": 1},
        )
        new_res = Resources.subtract(resource_1, resource_2)
        self.assertTrue(new_res.cpu == 0)
        self.assertTrue(new_res.gpu == 0)
        self.assertTrue(new_res.extra_cpu == 0)
        self.assertTrue(new_res.extra_gpu == 0)
        self.assertTrue(all(k == 0 for k in new_res.custom_resources.values()))
        self.assertTrue(all(k == 0 for k in new_res.extra_custom_resources.values()))

    def testDifferentResources(self):
        resource_1 = Resources(1, 0, 0, 1, custom_resources={"a": 1, "b": 2})
        resource_2 = Resources(1, 0, 0, 1, custom_resources={"a": 1, "c": 2})
        new_res = Resources.subtract(resource_1, resource_2)
        assert "c" in new_res.custom_resources
        assert "b" in new_res.custom_resources
        self.assertTrue(new_res.cpu == 0)
        self.assertTrue(new_res.gpu == 0)
        self.assertTrue(new_res.extra_cpu == 0)
        self.assertTrue(new_res.extra_gpu == 0)
        self.assertTrue(new_res.get("a") == 0)

    def testSerialization(self):
        original = Resources(1, 0, 0, 1, custom_resources={"a": 1, "b": 2})
        jsoned = resources_to_json(original)
        new_resource = json_to_resources(jsoned)
        self.assertEqual(original, new_resource)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
