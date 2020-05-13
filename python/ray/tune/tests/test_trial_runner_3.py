import os
import shutil
import sys
import tempfile
import unittest

import ray
from ray.rllib import _register_all

from ray.tune import TuneError
from ray.tune.schedulers import TrialScheduler, FIFOScheduler
from ray.tune.experiment import Experiment
from ray.tune.trial import Trial
from ray.tune.trial_runner import TrialRunner
from ray.tune.resources import Resources, json_to_resources, resources_to_json
from ray.tune.suggest.repeater import Repeater
from ray.tune.suggest.suggestion import (_MockSuggestionAlgorithm,
                                         SearchGenerator, Searcher)


class TrialRunnerTest3(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def testStepHook(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner()

        def on_step_begin(self, trialrunner):
            self._update_avail_resources()
            cnt = self.pre_step if hasattr(self, "pre_step") else 0
            self.pre_step = cnt + 1

        def on_step_end(self, trialrunner):
            cnt = self.pre_step if hasattr(self, "post_step") else 0
            self.post_step = 1 + cnt

        import types
        runner.trial_executor.on_step_begin = types.MethodType(
            on_step_begin, runner.trial_executor)
        runner.trial_executor.on_step_end = types.MethodType(
            on_step_end, runner.trial_executor)

        kwargs = {
            "stopping_criterion": {
                "training_iteration": 5
            },
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
            "stopping_criterion": {
                "training_iteration": 5
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs)
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

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(trials[2].status, Trial.RUNNING)
        self.assertEqual(trials[-1].status, Trial.TERMINATED)

    def testSearchAlgNotification(self):
        """Checks notification of trial to the Search Algorithm."""
        ray.init(num_cpus=4, num_gpus=2)
        experiment_spec = {"run": "__fake", "stop": {"training_iteration": 2}}
        experiments = [Experiment.from_json("test", experiment_spec)]
        search_alg = _MockSuggestionAlgorithm()
        searcher = search_alg.searcher
        search_alg.add_configurations(experiments)
        runner = TrialRunner(search_alg=search_alg)
        runner.step()
        trials = runner.get_trials()
        self.assertEqual(trials[0].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)

        self.assertEqual(searcher.counter["result"], 1)
        self.assertEqual(searcher.counter["complete"], 1)

    def testSearchAlgFinished(self):
        """Checks that SearchAlg is Finished before all trials are done."""
        ray.init(num_cpus=4, num_gpus=2)
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

        ray.init(num_cpus=4, num_gpus=2)
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
            "stop": {
                "training_iteration": 1
            }
        }
        experiments = [Experiment.from_json("test", experiment_spec)]
        search_alg = _MockSuggestionAlgorithm(max_concurrent=1)
        search_alg.add_configurations(experiments)
        searcher = search_alg.searcher
        runner = TrialRunner(search_alg=search_alg)
        runner.step()
        trials = runner.get_trials()
        self.assertEqual(trials[0].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)

        trials = runner.get_trials()
        runner.step()
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(len(searcher.live_trials), 1)

        searcher.stall = True

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

        runner.step()
        self.assertEqual(trials[2].status, Trial.TERMINATED)
        self.assertEqual(len(searcher.live_trials), 0)
        self.assertTrue(search_alg.is_finished())
        self.assertTrue(runner.is_finished())

    def testSearchAlgFinishes(self):
        """Empty SearchAlg changing state in `next_trials` does not crash."""

        class FinishFastAlg(_MockSuggestionAlgorithm):
            _index = 0

            def next_trials(self):
                spec = self._experiment.spec
                trials = []
                if self._index < spec["num_samples"]:
                    trial = Trial(
                        spec.get("run"), stopping_criterion=spec.get("stop"))
                    trials.append(trial)
                self._index += 1

                if self._index > 4:
                    self.set_finished()

                return trials

            def suggest(self, trial_id):
                return {}

        ray.init(num_cpus=2)
        experiment_spec = {
            "run": "__fake",
            "num_samples": 2,
            "stop": {
                "training_iteration": 1
            }
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

    def testTrialSaveRestore(self):
        """Creates different trials to test runner.checkpoint/restore."""
        ray.init(num_cpus=3)
        tmpdir = tempfile.mkdtemp()

        runner = TrialRunner(local_checkpoint_dir=tmpdir, checkpoint_period=0)
        trials = [
            Trial(
                "__fake",
                trial_id="trial_terminate",
                stopping_criterion={"training_iteration": 1},
                checkpoint_freq=1)
        ]
        runner.add_trial(trials[0])
        runner.step()  # Start trial
        runner.step()  # Process result, dispatch save
        runner.step()  # Process save
        self.assertEquals(trials[0].status, Trial.TERMINATED)

        trials += [
            Trial(
                "__fake",
                trial_id="trial_fail",
                stopping_criterion={"training_iteration": 3},
                checkpoint_freq=1,
                config={"mock_error": True})
        ]
        runner.add_trial(trials[1])
        runner.step()  # Start trial
        runner.step()  # Process result, dispatch save
        runner.step()  # Process save
        runner.step()  # Error
        self.assertEquals(trials[1].status, Trial.ERROR)

        trials += [
            Trial(
                "__fake",
                trial_id="trial_succ",
                stopping_criterion={"training_iteration": 2},
                checkpoint_freq=1)
        ]
        runner.add_trial(trials[2])
        runner.step()  # Start trial
        self.assertEquals(len(runner.trial_executor.get_checkpoints()), 3)
        self.assertEquals(trials[2].status, Trial.RUNNING)

        runner2 = TrialRunner(resume="LOCAL", local_checkpoint_dir=tmpdir)
        for tid in ["trial_terminate", "trial_fail"]:
            original_trial = runner.get_trial(tid)
            restored_trial = runner2.get_trial(tid)
            self.assertEqual(original_trial.status, restored_trial.status)

        restored_trial = runner2.get_trial("trial_succ")
        self.assertEqual(Trial.PENDING, restored_trial.status)

        runner2.step()  # Start trial
        runner2.step()  # Process result, dispatch save
        runner2.step()  # Process save
        runner2.step()  # Process result, dispatch save
        runner2.step()  # Process save
        self.assertRaises(TuneError, runner2.step)
        shutil.rmtree(tmpdir)

    def testTrialNoSave(self):
        """Check that non-checkpointing trials are not saved."""
        ray.init(num_cpus=3)
        tmpdir = tempfile.mkdtemp()

        runner = TrialRunner(local_checkpoint_dir=tmpdir, checkpoint_period=0)
        runner.add_trial(
            Trial(
                "__fake",
                trial_id="non_checkpoint",
                stopping_criterion={"training_iteration": 2}))

        while not all(t.status == Trial.TERMINATED
                      for t in runner.get_trials()):
            runner.step()

        runner.add_trial(
            Trial(
                "__fake",
                trial_id="checkpoint",
                checkpoint_at_end=True,
                stopping_criterion={"training_iteration": 2}))

        while not all(t.status == Trial.TERMINATED
                      for t in runner.get_trials()):
            runner.step()

        runner.add_trial(
            Trial(
                "__fake",
                trial_id="pending",
                stopping_criterion={"training_iteration": 2}))

        runner.step()
        runner.step()

        runner2 = TrialRunner(resume="LOCAL", local_checkpoint_dir=tmpdir)
        new_trials = runner2.get_trials()
        self.assertEquals(len(new_trials), 3)
        self.assertTrue(
            runner2.get_trial("non_checkpoint").status == Trial.TERMINATED)
        self.assertTrue(
            runner2.get_trial("checkpoint").status == Trial.TERMINATED)
        self.assertTrue(runner2.get_trial("pending").status == Trial.PENDING)
        self.assertTrue(not runner2.get_trial("pending").last_result)
        runner2.step()
        shutil.rmtree(tmpdir)

    def testCheckpointWithFunction(self):
        ray.init()
        trial = Trial(
            "__fake",
            config={"callbacks": {
                "on_episode_start": lambda i: i,
            }},
            checkpoint_freq=1)
        tmpdir = tempfile.mkdtemp()
        runner = TrialRunner(local_checkpoint_dir=tmpdir, checkpoint_period=0)
        runner.add_trial(trial)
        for _ in range(5):
            runner.step()
        # force checkpoint
        runner.checkpoint()
        runner2 = TrialRunner(resume="LOCAL", local_checkpoint_dir=tmpdir)
        new_trial = runner2.get_trials()[0]
        self.assertTrue("callbacks" in new_trial.config)
        self.assertTrue("on_episode_start" in new_trial.config["callbacks"])
        shutil.rmtree(tmpdir)

    def testCheckpointOverwrite(self):
        def count_checkpoints(cdir):
            return sum((fname.startswith("experiment_state")
                        and fname.endswith(".json"))
                       for fname in os.listdir(cdir))

        ray.init()
        trial = Trial("__fake", checkpoint_freq=1)
        tmpdir = tempfile.mkdtemp()
        runner = TrialRunner(local_checkpoint_dir=tmpdir, checkpoint_period=0)
        runner.add_trial(trial)
        for _ in range(5):
            runner.step()
        # force checkpoint
        runner.checkpoint()
        self.assertEquals(count_checkpoints(tmpdir), 1)

        runner2 = TrialRunner(resume="LOCAL", local_checkpoint_dir=tmpdir)
        for _ in range(5):
            runner2.step()
        self.assertEquals(count_checkpoints(tmpdir), 2)

        runner2.checkpoint()
        self.assertEquals(count_checkpoints(tmpdir), 2)
        shutil.rmtree(tmpdir)

    def testUserCheckpoint(self):
        ray.init(num_cpus=3)
        tmpdir = tempfile.mkdtemp()
        runner = TrialRunner(local_checkpoint_dir=tmpdir, checkpoint_period=0)
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

        runner2 = TrialRunner(resume="LOCAL", local_checkpoint_dir=tmpdir)
        runner2.step()  # 5: Start trial and dispatch restore
        trials2 = runner2.get_trials()
        self.assertEqual(ray.get(trials2[0].runner.get_info.remote()), 1)
        shutil.rmtree(tmpdir)


class SearchAlgorithmTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()
        _register_all()

    def testNestedSuggestion(self):
        class TestSuggestion(Searcher):
            def suggest(self, trial_id):
                return {"a": {"b": {"c": {"d": 4, "e": 5}}}}

        searcher = TestSuggestion()
        alg = SearchGenerator(searcher)
        alg.add_configurations({"test": {"run": "__fake"}})
        trial = alg.next_trials()[0]
        self.assertTrue("e=5" in trial.experiment_tag)
        self.assertTrue("d=4" in trial.experiment_tag)

    def _test_repeater(self, num_samples, repeat):
        ray.init(num_cpus=4)

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
            "stop": {
                "training_iteration": 1
            }
        }
        alg.add_configurations({"test": experiment_spec})
        runner = TrialRunner(search_alg=alg)
        while not runner.is_finished():
            runner.step()

        return runner.get_trials()

    def testRepeat1(self):
        trials = self._test_repeater(num_samples=2, repeat=1)
        self.assertEquals(len(trials), 2)
        parameter_set = {t.evaluated_params["test_variable"] for t in trials}
        self.assertEquals(len(parameter_set), 2)

    def testRepeat4(self):
        trials = self._test_repeater(num_samples=12, repeat=4)
        self.assertEquals(len(trials), 12)
        parameter_set = {t.evaluated_params["test_variable"] for t in trials}
        self.assertEquals(len(parameter_set), 3)

    def testOddRepeat(self):
        trials = self._test_repeater(num_samples=11, repeat=5)
        self.assertEquals(len(trials), 11)
        parameter_set = {t.evaluated_params["test_variable"] for t in trials}
        self.assertEquals(len(parameter_set), 3)


class ResourcesTest(unittest.TestCase):
    def testSubtraction(self):
        resource_1 = Resources(
            1,
            0,
            0,
            1,
            custom_resources={
                "a": 1,
                "b": 2
            },
            extra_custom_resources={
                "a": 1,
                "b": 1
            })
        resource_2 = Resources(
            1,
            0,
            0,
            1,
            custom_resources={
                "a": 1,
                "b": 2
            },
            extra_custom_resources={
                "a": 1,
                "b": 1
            })
        new_res = Resources.subtract(resource_1, resource_2)
        self.assertTrue(new_res.cpu == 0)
        self.assertTrue(new_res.gpu == 0)
        self.assertTrue(new_res.extra_cpu == 0)
        self.assertTrue(new_res.extra_gpu == 0)
        self.assertTrue(all(k == 0 for k in new_res.custom_resources.values()))
        self.assertTrue(
            all(k == 0 for k in new_res.extra_custom_resources.values()))

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
        self.assertEquals(original, new_resource)


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
