import pickle
from collections import Counter
import shutil
import tempfile
import copy
import numpy as np
import os
import sys
import time
import unittest
from unittest.mock import patch

import ray
from ray.rllib import _register_all

from ray import tune
from ray.tune import (DurableTrainable, Trainable, TuneError, Stopper,
                      EarlyStopping, run)
from ray.tune import register_env, register_trainable, run_experiments
from ray.tune.durable_trainable import durable
from ray.tune.schedulers import (TrialScheduler, FIFOScheduler,
                                 AsyncHyperBandScheduler)
from ray.tune.stopper import MaximumIterationStopper, TrialPlateauStopper
from ray.tune.trial import Trial
from ray.tune.result import (TIMESTEPS_TOTAL, DONE, HOSTNAME, NODE_IP, PID,
                             EPISODES_TOTAL, TRAINING_ITERATION,
                             TIMESTEPS_THIS_ITER, TIME_THIS_ITER_S,
                             TIME_TOTAL_S, TRIAL_ID, EXPERIMENT_TAG)
from ray.tune.logger import Logger
from ray.tune.experiment import Experiment
from ray.tune.resources import Resources
from ray.tune.suggest import grid_search
from ray.tune.suggest.hyperopt import HyperOptSearch
from ray.tune.suggest.ax import AxSearch
from ray.tune.suggest._mock import _MockSuggestionAlgorithm
from ray.tune.utils import (flatten_dict, get_pinned_object,
                            pin_in_object_store)
from ray.tune.utils.mock import mock_storage_client, MOCK_REMOTE_DIR


class TrainableFunctionApiTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=4, num_gpus=0, object_store_memory=150 * 1024 * 1024)
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects
        shutil.rmtree(self.tmpdir)

    def checkAndReturnConsistentLogs(self, results, sleep_per_iter=None):
        """Checks logging is the same between APIs.

        Ignore "DONE" for logging but checks that the
        scheduler is notified properly with the last result.
        """
        class_results = copy.deepcopy(results)
        function_results = copy.deepcopy(results)

        class_output = []
        function_output = []
        scheduler_notif = []

        class MockScheduler(FIFOScheduler):
            def on_trial_complete(self, runner, trial, result):
                scheduler_notif.append(result)

        class ClassAPILogger(Logger):
            def on_result(self, result):
                class_output.append(result)

        class FunctionAPILogger(Logger):
            def on_result(self, result):
                function_output.append(result)

        class _WrappedTrainable(Trainable):
            def setup(self, config):
                del config
                self._result_iter = copy.deepcopy(class_results)

            def step(self):
                if sleep_per_iter:
                    time.sleep(sleep_per_iter)
                res = self._result_iter.pop(0)  # This should not fail
                if not self._result_iter:  # Mark "Done" for last result
                    res[DONE] = True
                return res

        def _function_trainable(config, reporter):
            for result in function_results:
                if sleep_per_iter:
                    time.sleep(sleep_per_iter)
                reporter(**result)

        class_trainable_name = "class_trainable"
        register_trainable(class_trainable_name, _WrappedTrainable)

        [trial1] = run(
            _function_trainable,
            loggers=[FunctionAPILogger],
            raise_on_failed_trial=False,
            scheduler=MockScheduler()).trials

        [trial2] = run(
            class_trainable_name,
            loggers=[ClassAPILogger],
            raise_on_failed_trial=False,
            scheduler=MockScheduler()).trials

        trials = [trial1, trial2]

        # Ignore these fields
        NO_COMPARE_FIELDS = {
            HOSTNAME,
            NODE_IP,
            TRIAL_ID,
            EXPERIMENT_TAG,
            PID,
            TIME_THIS_ITER_S,
            TIME_TOTAL_S,
            DONE,  # This is ignored because FunctionAPI has different handling
            "timestamp",
            "time_since_restore",
            "experiment_id",
            "date",
        }

        self.assertEqual(len(class_output), len(results))
        self.assertEqual(len(function_output), len(results))

        def as_comparable_result(result):
            return {
                k: v
                for k, v in result.items() if k not in NO_COMPARE_FIELDS
            }

        function_comparable = [
            as_comparable_result(result) for result in function_output
        ]
        class_comparable = [
            as_comparable_result(result) for result in class_output
        ]

        self.assertEqual(function_comparable, class_comparable)

        self.assertEqual(sum(t.get(DONE) for t in scheduler_notif), 2)
        self.assertEqual(
            as_comparable_result(scheduler_notif[0]),
            as_comparable_result(scheduler_notif[1]))

        # Make sure the last result is the same.
        self.assertEqual(
            as_comparable_result(trials[0].last_result),
            as_comparable_result(trials[1].last_result))

        return function_output, trials

    def testPinObject(self):
        X = pin_in_object_store("hello")

        @ray.remote
        def f():
            return get_pinned_object(X)

        self.assertEqual(ray.get(f.remote()), "hello")

    def testFetchPinned(self):
        X = pin_in_object_store("hello")

        def train(config, reporter):
            get_pinned_object(X)
            reporter(timesteps_total=100, done=True)

        register_trainable("f1", train)
        [trial] = run_experiments({
            "foo": {
                "run": "f1",
            }
        })
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 100)

    def testRegisterEnv(self):
        register_env("foo", lambda: None)
        self.assertRaises(TypeError, lambda: register_env("foo", 2))

    def testRegisterEnvOverwrite(self):
        def train(config, reporter):
            reporter(timesteps_total=100, done=True)

        def train2(config, reporter):
            reporter(timesteps_total=200, done=True)

        register_trainable("f1", train)
        register_trainable("f1", train2)
        [trial] = run_experiments({
            "foo": {
                "run": "f1",
            }
        })
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 200)

    def testRegisterTrainable(self):
        def train(config, reporter):
            pass

        class A:
            pass

        class B(Trainable):
            pass

        register_trainable("foo", train)
        Experiment("test", train)
        register_trainable("foo", B)
        Experiment("test", B)
        self.assertRaises(TypeError, lambda: register_trainable("foo", B()))
        self.assertRaises(TuneError, lambda: Experiment("foo", B()))
        self.assertRaises(TypeError, lambda: register_trainable("foo", A))
        self.assertRaises(TypeError, lambda: Experiment("foo", A))

    def testTrainableCallable(self):
        def dummy_fn(config, reporter, steps):
            reporter(timesteps_total=steps, done=True)

        from functools import partial
        steps = 500
        register_trainable("test", partial(dummy_fn, steps=steps))
        [trial] = run_experiments({
            "foo": {
                "run": "test",
            }
        })
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], steps)
        [trial] = tune.run(partial(dummy_fn, steps=steps)).trials
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], steps)

    def testBuiltInTrainableResources(self):
        os.environ["TUNE_PLACEMENT_GROUP_AUTO_DISABLED"] = "1"

        class B(Trainable):
            @classmethod
            def default_resource_request(cls, config):
                return Resources(cpu=config["cpu"], gpu=config["gpu"])

            def step(self):
                return {"timesteps_this_iter": 1, "done": True}

        register_trainable("B", B)

        def f(cpus, gpus, queue_trials):
            return run_experiments(
                {
                    "foo": {
                        "run": "B",
                        "config": {
                            "cpu": cpus,
                            "gpu": gpus,
                        },
                    }
                },
                queue_trials=queue_trials)[0]

        # Should all succeed
        self.assertEqual(f(0, 0, False).status, Trial.TERMINATED)
        self.assertEqual(f(1, 0, True).status, Trial.TERMINATED)
        self.assertEqual(f(1, 0, True).status, Trial.TERMINATED)

        # Too large resource request
        self.assertRaises(TuneError, lambda: f(100, 100, False))
        self.assertRaises(TuneError, lambda: f(0, 100, False))
        self.assertRaises(TuneError, lambda: f(100, 0, False))

        # TODO(ekl) how can we test this is queued (hangs)?
        # f(100, 0, True)

    def testRewriteEnv(self):
        def train(config, reporter):
            reporter(timesteps_total=1)

        register_trainable("f1", train)

        [trial] = run_experiments({
            "foo": {
                "run": "f1",
                "env": "CartPole-v0",
            }
        })
        self.assertEqual(trial.config["env"], "CartPole-v0")

    def testConfigPurity(self):
        def train(config, reporter):
            assert config == {"a": "b"}, config
            reporter(timesteps_total=1)

        register_trainable("f1", train)
        run_experiments({
            "foo": {
                "run": "f1",
                "config": {
                    "a": "b"
                },
            }
        })

    def testLogdir(self):
        def train(config, reporter):
            assert os.path.join(ray._private.utils.get_user_temp_dir(),
                                "logdir", "foo") in os.getcwd(), os.getcwd()
            reporter(timesteps_total=1)

        register_trainable("f1", train)
        run_experiments({
            "foo": {
                "run": "f1",
                "local_dir": os.path.join(
                    ray._private.utils.get_user_temp_dir(), "logdir"),
                "config": {
                    "a": "b"
                },
            }
        })

    def testLogdirStartingWithTilde(self):
        local_dir = "~/ray_results/local_dir"

        def train(config, reporter):
            cwd = os.getcwd()
            assert cwd.startswith(os.path.expanduser(local_dir)), cwd
            assert not cwd.startswith("~"), cwd
            reporter(timesteps_total=1)

        register_trainable("f1", train)
        run_experiments({
            "foo": {
                "run": "f1",
                "local_dir": local_dir,
                "config": {
                    "a": "b"
                },
            }
        })

    def testLongFilename(self):
        def train(config, reporter):
            assert os.path.join(ray._private.utils.get_user_temp_dir(),
                                "logdir", "foo") in os.getcwd(), os.getcwd()
            reporter(timesteps_total=1)

        register_trainable("f1", train)
        run_experiments({
            "foo": {
                "run": "f1",
                "local_dir": os.path.join(
                    ray._private.utils.get_user_temp_dir(), "logdir"),
                "config": {
                    "a" * 50: tune.sample_from(lambda spec: 5.0 / 7),
                    "b" * 50: tune.sample_from(lambda spec: "long" * 40),
                },
            }
        })

    def testBadParams(self):
        def f():
            run_experiments({"foo": {}})

        self.assertRaises(TuneError, f)

    def testBadParams2(self):
        def f():
            run_experiments({
                "foo": {
                    "run": "asdf",
                    "bah": "this param is not allowed",
                }
            })

        self.assertRaises(TuneError, f)

    def testBadParams3(self):
        def f():
            run_experiments({
                "foo": {
                    "run": grid_search("invalid grid search"),
                }
            })

        self.assertRaises(TuneError, f)

    def testBadParams4(self):
        def f():
            run_experiments({
                "foo": {
                    "run": "asdf",
                }
            })

        self.assertRaises(TuneError, f)

    def testBadParams5(self):
        def f():
            run_experiments({"foo": {"run": "PPO", "stop": {"asdf": 1}}})

        self.assertRaises(TuneError, f)

    def testBadParams6(self):
        def f():
            run_experiments({
                "foo": {
                    "run": "PPO",
                    "resources_per_trial": {
                        "asdf": 1
                    }
                }
            })

        self.assertRaises(TuneError, f)

    def testBadStoppingReturn(self):
        def train(config, reporter):
            reporter()

        register_trainable("f1", train)

        def f():
            run_experiments({
                "foo": {
                    "run": "f1",
                    "stop": {
                        "time": 10
                    },
                }
            })

        self.assertRaises(TuneError, f)

    def testNestedStoppingReturn(self):
        def train(config, reporter):
            for i in range(10):
                reporter(test={"test1": {"test2": i}})

        with self.assertRaises(TuneError):
            [trial] = tune.run(
                train, stop={
                    "test": {
                        "test1": {
                            "test2": 6
                        }
                    }
                }).trials
        [trial] = tune.run(train, stop={"test/test1/test2": 6}).trials
        self.assertEqual(trial.last_result["training_iteration"], 7)

    def testStoppingFunction(self):
        def train(config, reporter):
            for i in range(10):
                reporter(test=i)

        def stop(trial_id, result):
            return result["test"] > 6

        [trial] = tune.run(train, stop=stop).trials
        self.assertEqual(trial.last_result["training_iteration"], 8)

    def testStoppingMemberFunction(self):
        def train(config, reporter):
            for i in range(10):
                reporter(test=i)

        class Stopclass:
            def stop(self, trial_id, result):
                return result["test"] > 6

        [trial] = tune.run(train, stop=Stopclass().stop).trials
        self.assertEqual(trial.last_result["training_iteration"], 8)

    def testStopper(self):
        def train(config, reporter):
            for i in range(10):
                reporter(test=i)

        class CustomStopper(Stopper):
            def __init__(self):
                self._count = 0

            def __call__(self, trial_id, result):
                print("called")
                self._count += 1
                return result["test"] > 6

            def stop_all(self):
                return self._count > 5

        trials = tune.run(train, num_samples=5, stop=CustomStopper()).trials
        self.assertTrue(all(t.status == Trial.TERMINATED for t in trials))
        self.assertTrue(
            any(
                t.last_result.get("training_iteration") is None
                for t in trials))

    def testEarlyStopping(self):
        def train(config, reporter):
            reporter(test=0)

        top = 3

        with self.assertRaises(ValueError):
            EarlyStopping("test", top=0)
        with self.assertRaises(ValueError):
            EarlyStopping("test", top="0")
        with self.assertRaises(ValueError):
            EarlyStopping("test", std=0)
        with self.assertRaises(ValueError):
            EarlyStopping("test", patience=-1)
        with self.assertRaises(ValueError):
            EarlyStopping("test", std="0")
        with self.assertRaises(ValueError):
            EarlyStopping("test", mode="0")

        stopper = EarlyStopping("test", top=top, mode="min")

        analysis = tune.run(train, num_samples=10, stop=stopper)
        self.assertTrue(
            all(t.status == Trial.TERMINATED for t in analysis.trials))
        self.assertTrue(
            len(analysis.dataframe(metric="test", mode="max")) <= top)

        patience = 5
        stopper = EarlyStopping("test", top=top, mode="min", patience=patience)

        analysis = tune.run(train, num_samples=20, stop=stopper)
        self.assertTrue(
            all(t.status == Trial.TERMINATED for t in analysis.trials))
        self.assertTrue(
            len(analysis.dataframe(metric="test", mode="max")) <= patience)

        stopper = EarlyStopping("test", top=top, mode="min")

        analysis = tune.run(train, num_samples=10, stop=stopper)
        self.assertTrue(
            all(t.status == Trial.TERMINATED for t in analysis.trials))
        self.assertTrue(
            len(analysis.dataframe(metric="test", mode="max")) <= top)

    def testBadStoppingFunction(self):
        def train(config, reporter):
            for i in range(10):
                reporter(test=i)

        class CustomStopper:
            def stop(self, result):
                return result["test"] > 6

        def stop(result):
            return result["test"] > 6

        with self.assertRaises(TuneError):
            tune.run(train, stop=CustomStopper().stop)
        with self.assertRaises(TuneError):
            tune.run(train, stop=stop)

    def testMaximumIterationStopper(self):
        def train(config):
            for i in range(10):
                tune.report(it=i)

        stopper = MaximumIterationStopper(max_iter=6)

        out = tune.run(train, stop=stopper)
        self.assertEqual(out.trials[0].last_result[TRAINING_ITERATION], 6)

    def testTrialPlateauStopper(self):
        def train(config):
            tune.report(10.0)
            tune.report(11.0)
            tune.report(12.0)
            for i in range(10):
                tune.report(20.0)

        # num_results = 4, no other constraints --> early stop after 7
        stopper = TrialPlateauStopper(metric="_metric", num_results=4)

        out = tune.run(train, stop=stopper)
        self.assertEqual(out.trials[0].last_result[TRAINING_ITERATION], 7)

        # num_results = 4, grace period 9 --> early stop after 9
        stopper = TrialPlateauStopper(
            metric="_metric", num_results=4, grace_period=9)

        out = tune.run(train, stop=stopper)
        self.assertEqual(out.trials[0].last_result[TRAINING_ITERATION], 9)

        # num_results = 4, min_metric = 22 --> full 13 iterations
        stopper = TrialPlateauStopper(
            metric="_metric", num_results=4, metric_threshold=22.0, mode="max")

        out = tune.run(train, stop=stopper)
        self.assertEqual(out.trials[0].last_result[TRAINING_ITERATION], 13)

    def testCustomTrialDir(self):
        def train(config):
            for i in range(10):
                tune.report(test=i)

        custom_name = "TRAIL_TRIAL"

        def custom_trial_dir(trial):
            return custom_name

        trials = tune.run(
            train,
            config={
                "t1": tune.grid_search([1, 2, 3])
            },
            trial_dirname_creator=custom_trial_dir,
            local_dir=self.tmpdir).trials
        logdirs = {t.logdir for t in trials}
        assert len(logdirs) == 3
        assert all(custom_name in dirpath for dirpath in logdirs)

    def testTrialDirRegression(self):
        def train(config, reporter):
            for i in range(10):
                reporter(test=i)

        trials = tune.run(
            train,
            config={
                "t1": tune.grid_search([1, 2, 3])
            },
            local_dir=self.tmpdir).trials
        logdirs = {t.logdir for t in trials}
        for i in [1, 2, 3]:
            assert any(f"t1={i}" in dirpath for dirpath in logdirs)
        for t in trials:
            assert any(t.trainable_name in dirpath for dirpath in logdirs)

    def testEarlyReturn(self):
        def train(config, reporter):
            reporter(timesteps_total=100, done=True)
            time.sleep(99999)

        register_trainable("f1", train)
        [trial] = run_experiments({
            "foo": {
                "run": "f1",
            }
        })
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 100)

    def testReporterNoUsage(self):
        def run_task(config, reporter):
            print("hello")

        experiment = Experiment(run=run_task, name="ray_crash_repro")
        [trial] = ray.tune.run(experiment).trials
        print(trial.last_result)
        self.assertEqual(trial.last_result[DONE], True)

    def testRerun(self):
        tmpdir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(tmpdir))

        def test(config):
            tid = config["id"]
            fail = config["fail"]
            marker = os.path.join(tmpdir, f"t{tid}-{fail}.log")
            if not os.path.exists(marker) and fail:
                open(marker, "w").close()
                raise ValueError
            for i in range(10):
                time.sleep(0.1)
                tune.report(hello=123)

        config = dict(
            name="hi-2",
            config={
                "fail": tune.grid_search([True, False]),
                "id": tune.grid_search(list(range(5)))
            },
            verbose=1,
            local_dir=tmpdir,
            loggers=None)
        trials = tune.run(test, raise_on_failed_trial=False, **config).trials
        self.assertEqual(Counter(t.status for t in trials)["ERROR"], 5)
        new_trials = tune.run(test, resume="ERRORED_ONLY", **config).trials
        self.assertEqual(Counter(t.status for t in new_trials)["ERROR"], 0)
        self.assertTrue(
            all(t.last_result.get("hello") == 123 for t in new_trials))

    def testTrialInfoAccess(self):
        class TestTrainable(Trainable):
            def step(self):
                result = {"name": self.trial_name, "trial_id": self.trial_id}
                print(result)
                return result

        analysis = tune.run(TestTrainable, stop={TRAINING_ITERATION: 1})
        trial = analysis.trials[0]
        self.assertEqual(trial.last_result.get("name"), str(trial))
        self.assertEqual(trial.last_result.get("trial_id"), trial.trial_id)

    def testTrialInfoAccessFunction(self):
        def train(config, reporter):
            reporter(name=reporter.trial_name, trial_id=reporter.trial_id)

        analysis = tune.run(train, stop={TRAINING_ITERATION: 1})
        trial = analysis.trials[0]
        self.assertEqual(trial.last_result.get("name"), str(trial))
        self.assertEqual(trial.last_result.get("trial_id"), trial.trial_id)

        def track_train(config):
            tune.report(
                name=tune.get_trial_name(), trial_id=tune.get_trial_id())

        analysis = tune.run(track_train, stop={TRAINING_ITERATION: 1})
        trial = analysis.trials[0]
        self.assertEqual(trial.last_result.get("name"), str(trial))
        self.assertEqual(trial.last_result.get("trial_id"), trial.trial_id)

    @patch("ray.tune.ray_trial_executor.TRIAL_CLEANUP_THRESHOLD", 3)
    def testLotsOfStops(self):
        class TestTrainable(Trainable):
            def step(self):
                result = {"name": self.trial_name, "trial_id": self.trial_id}
                return result

            def cleanup(self):
                time.sleep(0.3)
                open(os.path.join(self.logdir, "marker"), "a").close()
                return 1

        analysis = tune.run(
            TestTrainable, num_samples=10, stop={TRAINING_ITERATION: 1})
        for trial in analysis.trials:
            path = os.path.join(trial.logdir, "marker")
            assert os.path.exists(path)

    def testReportTimeStep(self):
        # Test that no timestep count are logged if never the Trainable never
        # returns any.
        results1 = [dict(mean_accuracy=5, done=i == 99) for i in range(100)]
        logs1, _ = self.checkAndReturnConsistentLogs(results1)

        self.assertTrue(all(log[TIMESTEPS_TOTAL] is None for log in logs1))

        # Test that no timesteps_this_iter are logged if only timesteps_total
        # are returned.
        results2 = [dict(timesteps_total=5, done=i == 9) for i in range(10)]
        logs2, _ = self.checkAndReturnConsistentLogs(results2)

        # Re-run the same trials but with added delay. This is to catch some
        # inconsistent timestep counting that was present in the multi-threaded
        # FunctionRunner. This part of the test can be removed once the
        # multi-threaded FunctionRunner is removed from ray/tune.
        # TODO: remove once the multi-threaded function runner is gone.
        logs2, _ = self.checkAndReturnConsistentLogs(results2, 0.5)

        # check all timesteps_total report the same value
        self.assertTrue(all(log[TIMESTEPS_TOTAL] == 5 for log in logs2))
        # check that none of the logs report timesteps_this_iter
        self.assertFalse(
            any(hasattr(log, TIMESTEPS_THIS_ITER) for log in logs2))

        # Test that timesteps_total and episodes_total are reported when
        # timesteps_this_iter and episodes_this_iter despite only return zeros.
        results3 = [
            dict(timesteps_this_iter=0, episodes_this_iter=0)
            for i in range(10)
        ]
        logs3, _ = self.checkAndReturnConsistentLogs(results3)

        self.assertTrue(all(log[TIMESTEPS_TOTAL] == 0 for log in logs3))
        self.assertTrue(all(log[EPISODES_TOTAL] == 0 for log in logs3))

        # Test that timesteps_total and episodes_total are properly counted
        # when timesteps_this_iter and episodes_this_iter report non-zero
        # values.
        results4 = [
            dict(timesteps_this_iter=3, episodes_this_iter=i)
            for i in range(10)
        ]
        logs4, _ = self.checkAndReturnConsistentLogs(results4)

        # The last reported result should not be double-logged.
        self.assertEqual(logs4[-1][TIMESTEPS_TOTAL], 30)
        self.assertNotEqual(logs4[-2][TIMESTEPS_TOTAL],
                            logs4[-1][TIMESTEPS_TOTAL])
        self.assertEqual(logs4[-1][EPISODES_TOTAL], 45)
        self.assertNotEqual(logs4[-2][EPISODES_TOTAL],
                            logs4[-1][EPISODES_TOTAL])

    def testAllValuesReceived(self):
        results1 = [
            dict(timesteps_total=(i + 1), my_score=i**2, done=i == 4)
            for i in range(5)
        ]

        logs1, _ = self.checkAndReturnConsistentLogs(results1)

        # check if the correct number of results were reported
        self.assertEqual(len(logs1), len(results1))

        def check_no_missing(reported_result, result):
            common_results = [reported_result[k] == result[k] for k in result]
            return all(common_results)

        # check that no result was dropped or modified
        complete_results = [
            check_no_missing(log, result)
            for log, result in zip(logs1, results1)
        ]
        self.assertTrue(all(complete_results))

        # check if done was logged exactly once
        self.assertEqual(len([r for r in logs1 if r.get("done")]), 1)

    def testNoDoneReceived(self):
        # repeat same test but without explicitly reporting done=True
        results1 = [
            dict(timesteps_total=(i + 1), my_score=i**2) for i in range(5)
        ]

        logs1, trials = self.checkAndReturnConsistentLogs(results1)

        # check if the correct number of results were reported.
        self.assertEqual(len(logs1), len(results1))

        def check_no_missing(reported_result, result):
            common_results = [reported_result[k] == result[k] for k in result]
            return all(common_results)

        # check that no result was dropped or modified
        complete_results1 = [
            check_no_missing(log, result)
            for log, result in zip(logs1, results1)
        ]
        self.assertTrue(all(complete_results1))

    def _testDurableTrainable(self, trainable, function=False, cleanup=True):
        sync_client = mock_storage_client()
        mock_get_client = "ray.tune.durable_trainable.get_cloud_sync_client"
        with patch(mock_get_client) as mock_get_cloud_sync_client:
            mock_get_cloud_sync_client.return_value = sync_client
            test_trainable = trainable(remote_checkpoint_dir=MOCK_REMOTE_DIR)
            result = test_trainable.train()
            self.assertEqual(result["metric"], 1)
            checkpoint_path = test_trainable.save()
            result = test_trainable.train()
            self.assertEqual(result["metric"], 2)
            result = test_trainable.train()
            self.assertEqual(result["metric"], 3)
            result = test_trainable.train()
            self.assertEqual(result["metric"], 4)

            if not function:
                test_trainable.state["hi"] = 2
                test_trainable.restore(checkpoint_path)
                self.assertEqual(test_trainable.state["hi"], 1)
            else:
                # Cannot re-use function trainable, create new
                tune.session.shutdown()
                test_trainable = trainable(
                    remote_checkpoint_dir=MOCK_REMOTE_DIR)
                test_trainable.restore(checkpoint_path)

            result = test_trainable.train()
            self.assertEqual(result["metric"], 2)

        if cleanup:
            self.addCleanup(shutil.rmtree, MOCK_REMOTE_DIR)

    def testDurableTrainableClass(self):
        class TestTrain(DurableTrainable):
            def setup(self, config):
                self.state = {"hi": 1, "iter": 0}

            def step(self):
                self.state["iter"] += 1
                return {
                    "timesteps_this_iter": 1,
                    "metric": self.state["iter"],
                    "done": self.state["iter"] > 3
                }

            def save_checkpoint(self, path):
                return self.state

            def load_checkpoint(self, state):
                self.state = state

        self._testDurableTrainable(TestTrain)

    def testDurableTrainableWrapped(self):
        class TestTrain(Trainable):
            def setup(self, config):
                self.state = {"hi": 1, "iter": 0}

            def step(self):
                self.state["iter"] += 1
                return {
                    "timesteps_this_iter": 1,
                    "metric": self.state["iter"],
                    "done": self.state["iter"] > 3
                }

            def save_checkpoint(self, path):
                return self.state

            def load_checkpoint(self, state):
                self.state = state

        self._testDurableTrainable(durable(TestTrain), cleanup=False)

        tune.register_trainable("test_train", TestTrain)

        self._testDurableTrainable(durable("test_train"))

    def testDurableTrainableFunction(self):
        def test_train(config, checkpoint_dir=None):
            state = {"hi": 1, "iter": 0}
            if checkpoint_dir:
                with open(os.path.join(checkpoint_dir, "ckpt.pkl"),
                          "rb") as fp:
                    state = pickle.load(fp)

            for i in range(4):
                state["iter"] += 1
                with tune.checkpoint_dir(step=state["iter"]) as dir:
                    with open(os.path.join(dir, "ckpt.pkl"), "wb") as fp:
                        pickle.dump(state, fp)
                tune.report(
                    **{
                        "timesteps_this_iter": 1,
                        "metric": state["iter"],
                        "done": state["iter"] > 3
                    })

        self._testDurableTrainable(durable(test_train), function=True)

    def testCheckpointDict(self):
        class TestTrain(Trainable):
            def setup(self, config):
                self.state = {"hi": 1}

            def step(self):
                return {"timesteps_this_iter": 1, "done": True}

            def save_checkpoint(self, path):
                return self.state

            def load_checkpoint(self, state):
                self.state = state

        test_trainable = TestTrain()
        result = test_trainable.save()
        test_trainable.state["hi"] = 2
        test_trainable.restore(result)
        self.assertEqual(test_trainable.state["hi"], 1)

        trials = run_experiments({
            "foo": {
                "run": TestTrain,
                "checkpoint_at_end": True
            }
        })
        for trial in trials:
            self.assertEqual(trial.status, Trial.TERMINATED)
            self.assertTrue(trial.has_checkpoint())

    def testMultipleCheckpoints(self):
        class TestTrain(Trainable):
            def setup(self, config):
                self.state = {"hi": 1, "iter": 0}

            def step(self):
                self.state["iter"] += 1
                return {"timesteps_this_iter": 1, "done": True}

            def save_checkpoint(self, path):
                return self.state

            def load_checkpoint(self, state):
                self.state = state

        test_trainable = TestTrain()
        checkpoint_1 = test_trainable.save()
        test_trainable.train()
        checkpoint_2 = test_trainable.save()
        self.assertNotEqual(checkpoint_1, checkpoint_2)
        test_trainable.restore(checkpoint_2)
        self.assertEqual(test_trainable.state["iter"], 1)
        test_trainable.restore(checkpoint_1)
        self.assertEqual(test_trainable.state["iter"], 0)

        trials = run_experiments({
            "foo": {
                "run": TestTrain,
                "checkpoint_at_end": True
            }
        })
        for trial in trials:
            self.assertEqual(trial.status, Trial.TERMINATED)
            self.assertTrue(trial.has_checkpoint())

    def testBackwardsCompat(self):
        class TestTrain(Trainable):
            def _setup(self, config):
                self.state = {"hi": 1, "iter": 0}

            def _train(self):
                self.state["iter"] += 1
                return {"timesteps_this_iter": 1, "done": True}

            def _save(self, path):
                return self.state

            def _restore(self, state):
                self.state = state

        test_trainable = TestTrain()
        checkpoint_1 = test_trainable.save()
        test_trainable.train()
        checkpoint_2 = test_trainable.save()
        self.assertNotEqual(checkpoint_1, checkpoint_2)
        test_trainable.restore(checkpoint_2)
        self.assertEqual(test_trainable.state["iter"], 1)
        test_trainable.restore(checkpoint_1)
        self.assertEqual(test_trainable.state["iter"], 0)

        trials = run_experiments({
            "foo": {
                "run": TestTrain,
                "checkpoint_at_end": True
            }
        })
        for trial in trials:
            self.assertEqual(trial.status, Trial.TERMINATED)
            self.assertTrue(trial.has_checkpoint())

    def testLogToFile(self):
        def train(config, reporter):
            import sys
            from ray import logger
            for i in range(10):
                reporter(timesteps_total=i)
            print("PRINT_STDOUT")
            print("PRINT_STDERR", file=sys.stderr)
            logger.info("LOG_STDERR")

        register_trainable("f1", train)

        # Do not log to file
        [trial] = tune.run("f1", log_to_file=False).trials
        self.assertFalse(os.path.exists(os.path.join(trial.logdir, "stdout")))
        self.assertFalse(os.path.exists(os.path.join(trial.logdir, "stderr")))

        # Log to default files
        [trial] = tune.run("f1", log_to_file=True).trials
        self.assertTrue(os.path.exists(os.path.join(trial.logdir, "stdout")))
        self.assertTrue(os.path.exists(os.path.join(trial.logdir, "stderr")))
        with open(os.path.join(trial.logdir, "stdout"), "rt") as fp:
            content = fp.read()
            self.assertIn("PRINT_STDOUT", content)
        with open(os.path.join(trial.logdir, "stderr"), "rt") as fp:
            content = fp.read()
            self.assertIn("PRINT_STDERR", content)
            self.assertIn("LOG_STDERR", content)

        # Log to one file
        [trial] = tune.run("f1", log_to_file="combined").trials
        self.assertFalse(os.path.exists(os.path.join(trial.logdir, "stdout")))
        self.assertFalse(os.path.exists(os.path.join(trial.logdir, "stderr")))
        self.assertTrue(os.path.exists(os.path.join(trial.logdir, "combined")))
        with open(os.path.join(trial.logdir, "combined"), "rt") as fp:
            content = fp.read()
            self.assertIn("PRINT_STDOUT", content)
            self.assertIn("PRINT_STDERR", content)
            self.assertIn("LOG_STDERR", content)

        # Log to two files
        [trial] = tune.run(
            "f1", log_to_file=("alt.stdout", "alt.stderr")).trials
        self.assertFalse(os.path.exists(os.path.join(trial.logdir, "stdout")))
        self.assertFalse(os.path.exists(os.path.join(trial.logdir, "stderr")))
        self.assertTrue(
            os.path.exists(os.path.join(trial.logdir, "alt.stdout")))
        self.assertTrue(
            os.path.exists(os.path.join(trial.logdir, "alt.stderr")))

        with open(os.path.join(trial.logdir, "alt.stdout"), "rt") as fp:
            content = fp.read()
            self.assertIn("PRINT_STDOUT", content)
        with open(os.path.join(trial.logdir, "alt.stderr"), "rt") as fp:
            content = fp.read()
            self.assertIn("PRINT_STDERR", content)
            self.assertIn("LOG_STDERR", content)

    def testTimeout(self):
        from ray.tune.stopper import TimeoutStopper
        import datetime

        def train(config):
            for i in range(20):
                tune.report(metric=i)
                time.sleep(1)

        register_trainable("f1", train)

        start = time.time()
        tune.run("f1", time_budget_s=5)
        diff = time.time() - start
        self.assertLess(diff, 10)

        # Metric should fire first
        start = time.time()
        tune.run("f1", stop={"metric": 3}, time_budget_s=7)
        diff = time.time() - start
        self.assertLess(diff, 7)

        # Timeout should fire first
        start = time.time()
        tune.run("f1", stop={"metric": 10}, time_budget_s=5)
        diff = time.time() - start
        self.assertLess(diff, 10)

        # Combined stopper. Shorter timeout should win.
        start = time.time()
        tune.run(
            "f1",
            stop=TimeoutStopper(10),
            time_budget_s=datetime.timedelta(seconds=3))
        diff = time.time() - start
        self.assertLess(diff, 9)

    def testInfiniteTrials(self):
        def train(config):
            time.sleep(0.5)
            tune.report(np.random.uniform(-10., 10.))

        start = time.time()
        out = tune.run(train, num_samples=-1, time_budget_s=10)
        taken = time.time() - start

        # Allow for init time overhead
        self.assertLessEqual(taken, 20.)
        self.assertGreaterEqual(len(out.trials), 0)

        status = dict(Counter([trial.status for trial in out.trials]))
        self.assertGreaterEqual(status["TERMINATED"], 1)
        self.assertLessEqual(status.get("PENDING", 0), 1)

    def testMetricCheckingEndToEnd(self):
        def train(config):
            tune.report(val=4, second=8)

        def train2(config):
            return

        os.environ["TUNE_DISABLE_STRICT_METRIC_CHECKING"] = "0"
        # `acc` is not reported, should raise
        with self.assertRaises(TuneError):
            # The trial runner raises a ValueError, but the experiment fails
            # with a TuneError
            tune.run(train, metric="acc")

        # `val` is reported, should not raise
        tune.run(train, metric="val")

        # Run does not report anything, should not raise
        tune.run(train2, metric="val")

        # Only the scheduler requires a metric
        with self.assertRaises(TuneError):
            tune.run(
                train,
                scheduler=AsyncHyperBandScheduler(metric="acc", mode="max"))

        tune.run(
            train, scheduler=AsyncHyperBandScheduler(metric="val", mode="max"))

        # Only the search alg requires a metric
        with self.assertRaises(TuneError):
            tune.run(
                train,
                config={"a": tune.choice([1, 2])},
                search_alg=HyperOptSearch(metric="acc", mode="max"))

        # Metric is passed
        tune.run(
            train,
            config={"a": tune.choice([1, 2])},
            search_alg=HyperOptSearch(metric="val", mode="max"))

        os.environ["TUNE_DISABLE_STRICT_METRIC_CHECKING"] = "1"
        # With strict metric checking disabled, this should not raise
        tune.run(train, metric="acc")

    def testTrialDirCreation(self):
        def test_trial_dir(config):
            return 1.0

        # Per default, the directory should be named `test_trial_dir_{date}`
        with tempfile.TemporaryDirectory() as tmp_dir:
            tune.run(test_trial_dir, local_dir=tmp_dir)

            subdirs = list(os.listdir(tmp_dir))
            self.assertNotIn("test_trial_dir", subdirs)
            found = False
            for subdir in subdirs:
                if subdir.startswith("test_trial_dir_"):  # Date suffix
                    found = True
                    break
            self.assertTrue(found)

        # If we set an explicit name, no date should be appended
        with tempfile.TemporaryDirectory() as tmp_dir:
            tune.run(test_trial_dir, local_dir=tmp_dir, name="my_test_exp")

            subdirs = list(os.listdir(tmp_dir))
            self.assertIn("my_test_exp", subdirs)
            found = False
            for subdir in subdirs:
                if subdir.startswith("my_test_exp_"):  # Date suffix
                    found = True
                    break
            self.assertFalse(found)

        # Don't append date if we set the env variable
        os.environ["TUNE_DISABLE_DATED_SUBDIR"] = "1"
        with tempfile.TemporaryDirectory() as tmp_dir:
            tune.run(test_trial_dir, local_dir=tmp_dir)

            subdirs = list(os.listdir(tmp_dir))
            self.assertIn("test_trial_dir", subdirs)
            found = False
            for subdir in subdirs:
                if subdir.startswith("test_trial_dir_"):  # Date suffix
                    found = True
                    break
            self.assertFalse(found)

    def testWithParameters(self):
        class Data:
            def __init__(self):
                self.data = [0] * 500_000

        data = Data()
        data.data[100] = 1

        class TestTrainable(Trainable):
            def setup(self, config, data):
                self.data = data.data
                self.data[101] = 2  # Changes are local

            def step(self):
                return dict(
                    metric=len(self.data), hundred=self.data[100], done=True)

        trial_1, trial_2 = tune.run(
            tune.with_parameters(TestTrainable, data=data),
            num_samples=2).trials

        self.assertEqual(data.data[101], 0)
        self.assertEqual(trial_1.last_result["metric"], 500_000)
        self.assertEqual(trial_1.last_result["hundred"], 1)
        self.assertEqual(trial_2.last_result["metric"], 500_000)
        self.assertEqual(trial_2.last_result["hundred"], 1)
        self.assertTrue(str(trial_1).startswith("TestTrainable"))

    def testWithParameters2(self):
        class Data:
            def __init__(self):
                import numpy as np
                self.data = np.random.rand((2 * 1024 * 1024))

        class TestTrainable(Trainable):
            def setup(self, config, data):
                self.data = data.data

            def step(self):
                return dict(metric=len(self.data), done=True)

        trainable = tune.with_parameters(TestTrainable, data=Data())
        # ray.cloudpickle will crash for some reason
        import cloudpickle as cp
        dumped = cp.dumps(trainable)
        assert sys.getsizeof(dumped) < 100 * 1024


class SerializabilityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def tearDown(self):
        if "RAY_PICKLE_VERBOSE_DEBUG" in os.environ:
            del os.environ["RAY_PICKLE_VERBOSE_DEBUG"]

    def testNotRaisesNonserializable(self):
        import threading
        lock = threading.Lock()

        def train(config):
            print(lock)
            tune.report(val=4, second=8)

        with self.assertRaisesRegex(TypeError, "RAY_PICKLE_VERBOSE_DEBUG"):
            # The trial runner raises a ValueError, but the experiment fails
            # with a TuneError
            tune.run(train, metric="acc")

    def testRaisesNonserializable(self):
        os.environ["RAY_PICKLE_VERBOSE_DEBUG"] = "1"
        import threading
        lock = threading.Lock()

        def train(config):
            print(lock)
            tune.report(val=4, second=8)

        with self.assertRaises(TypeError) as cm:
            # The trial runner raises a ValueError, but the experiment fails
            # with a TuneError
            tune.run(train, metric="acc")
        msg = cm.exception.args[0]
        assert "RAY_PICKLE_VERBOSE_DEBUG" not in msg
        assert "thread.lock" in msg


class ShimCreationTest(unittest.TestCase):
    def testCreateScheduler(self):
        kwargs = {"metric": "metric_foo", "mode": "min"}

        scheduler = "async_hyperband"
        shim_scheduler = tune.create_scheduler(scheduler, **kwargs)
        real_scheduler = AsyncHyperBandScheduler(**kwargs)
        assert type(shim_scheduler) is type(real_scheduler)

    def testCreateSearcher(self):
        kwargs = {"metric": "metric_foo", "mode": "min"}

        searcher_ax = "ax"
        shim_searcher_ax = tune.create_searcher(searcher_ax, **kwargs)
        real_searcher_ax = AxSearch(space=[], **kwargs)
        assert type(shim_searcher_ax) is type(real_searcher_ax)

        searcher_hyperopt = "hyperopt"
        shim_searcher_hyperopt = tune.create_searcher(searcher_hyperopt,
                                                      **kwargs)
        real_searcher_hyperopt = HyperOptSearch({}, **kwargs)
        assert type(shim_searcher_hyperopt) is type(real_searcher_hyperopt)

    def testExtraParams(self):
        kwargs = {"metric": "metric_foo", "mode": "min", "extra_param": "test"}

        scheduler = "async_hyperband"
        tune.create_scheduler(scheduler, **kwargs)

        searcher_ax = "ax"
        tune.create_searcher(searcher_ax, **kwargs)


class ApiTestFast(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(
            num_cpus=4, num_gpus=0, local_mode=True, include_dashboard=False)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()
        _register_all()

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def testNestedResults(self):
        def create_result(i):
            return {"test": {"1": {"2": {"3": i, "4": False}}}}

        flattened_keys = list(flatten_dict(create_result(0)))

        class _MockScheduler(FIFOScheduler):
            results = []

            def on_trial_result(self, trial_runner, trial, result):
                self.results += [result]
                return TrialScheduler.CONTINUE

            def on_trial_complete(self, trial_runner, trial, result):
                self.complete_result = result

        def train(config, reporter):
            for i in range(100):
                reporter(**create_result(i))

        algo = _MockSuggestionAlgorithm()
        scheduler = _MockScheduler()
        [trial] = tune.run(
            train,
            scheduler=scheduler,
            search_alg=algo,
            stop={
                "test/1/2/3": 20
            }).trials
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result["test"]["1"]["2"]["3"], 20)
        self.assertEqual(trial.last_result["test"]["1"]["2"]["4"], False)
        self.assertEqual(trial.last_result[TRAINING_ITERATION], 21)
        self.assertEqual(len(scheduler.results), 20)
        self.assertTrue(
            all(
                set(result) >= set(flattened_keys)
                for result in scheduler.results))
        self.assertTrue(set(scheduler.complete_result) >= set(flattened_keys))
        self.assertEqual(len(algo.results), 20)
        self.assertTrue(
            all(set(result) >= set(flattened_keys) for result in algo.results))
        with self.assertRaises(TuneError):
            [trial] = tune.run(train, stop={"1/2/3": 20})
        with self.assertRaises(TuneError):
            [trial] = tune.run(train, stop={"test": 1}).trials

    def testIterationCounter(self):
        def train(config, reporter):
            for i in range(100):
                reporter(itr=i, timesteps_this_iter=1)

        register_trainable("exp", train)
        config = {
            "my_exp": {
                "run": "exp",
                "config": {
                    "iterations": 100,
                },
                "stop": {
                    "timesteps_total": 100
                },
            }
        }
        [trial] = run_experiments(config)
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TRAINING_ITERATION], 100)
        self.assertEqual(trial.last_result["itr"], 99)

    def testErrorReturn(self):
        def train(config, reporter):
            raise Exception("uh oh")

        register_trainable("f1", train)

        def f():
            run_experiments({
                "foo": {
                    "run": "f1",
                }
            })

        self.assertRaises(TuneError, f)

    def testSuccess(self):
        def train(config, reporter):
            for i in range(100):
                reporter(timesteps_total=i)

        register_trainable("f1", train)
        [trial] = run_experiments({
            "foo": {
                "run": "f1",
            }
        })
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 99)

    def testNoRaiseFlag(self):
        def train(config, reporter):
            raise Exception()

        register_trainable("f1", train)

        [trial] = run_experiments(
            {
                "foo": {
                    "run": "f1",
                }
            }, raise_on_failed_trial=False)
        self.assertEqual(trial.status, Trial.ERROR)

    def testReportInfinity(self):
        def train(config, reporter):
            for _ in range(100):
                reporter(mean_accuracy=float("inf"))

        register_trainable("f1", train)
        [trial] = run_experiments({
            "foo": {
                "run": "f1",
            }
        })
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result["mean_accuracy"], float("inf"))


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
