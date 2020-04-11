import shutil

import copy
import os
import time
import unittest
from unittest.mock import patch

import ray
from ray.rllib import _register_all

from ray import tune
from ray.tune import DurableTrainable, Trainable, TuneError, Stopper
from ray.tune import register_env, register_trainable, run_experiments
from ray.tune.schedulers import TrialScheduler, FIFOScheduler
from ray.tune.trial import Trial
from ray.tune.result import (TIMESTEPS_TOTAL, DONE, HOSTNAME, NODE_IP, PID,
                             EPISODES_TOTAL, TRAINING_ITERATION,
                             TIMESTEPS_THIS_ITER, TIME_THIS_ITER_S,
                             TIME_TOTAL_S, TRIAL_ID, EXPERIMENT_TAG)
from ray.tune.logger import Logger
from ray.tune.experiment import Experiment
from ray.tune.resources import Resources
from ray.tune.suggest import grid_search
from ray.tune.suggest.suggestion import _MockSuggestionAlgorithm
from ray.tune.utils import (flatten_dict, get_pinned_object,
                            pin_in_object_store)
from ray.tune.utils.mock import mock_storage_client, MOCK_REMOTE_DIR


class TrainableFunctionApiTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=4, num_gpus=0, object_store_memory=150 * 1024 * 1024)

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

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
            def _setup(self, config):
                del config
                self._result_iter = copy.deepcopy(class_results)

            def _train(self):
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

        trials = run_experiments(
            {
                "function_api": {
                    "run": _function_trainable,
                    "loggers": [FunctionAPILogger],
                },
                "class_api": {
                    "run": class_trainable_name,
                    "loggers": [ClassAPILogger],
                },
            },
            raise_on_failed_trial=False,
            scheduler=MockScheduler())

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
        class B(Trainable):
            @classmethod
            def default_resource_request(cls, config):
                return Resources(cpu=config["cpu"], gpu=config["gpu"])

            def _train(self):
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
            assert os.path.join(ray.utils.get_user_temp_dir(), "logdir",
                                "foo") in os.getcwd(), os.getcwd()
            reporter(timesteps_total=1)

        register_trainable("f1", train)
        run_experiments({
            "foo": {
                "run": "f1",
                "local_dir": os.path.join(ray.utils.get_user_temp_dir(),
                                          "logdir"),
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
            assert os.path.join(ray.utils.get_user_temp_dir(), "logdir",
                                "foo") in os.getcwd(), os.getcwd()
            reporter(timesteps_total=1)

        register_trainable("f1", train)
        run_experiments({
            "foo": {
                "run": "f1",
                "local_dir": os.path.join(ray.utils.get_user_temp_dir(),
                                          "logdir"),
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
            for i in range(100):
                reporter(mean_accuracy=float("inf"))

        register_trainable("f1", train)
        [trial] = run_experiments({
            "foo": {
                "run": "f1",
            }
        })
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result["mean_accuracy"], float("inf"))

    def testTrialInfoAccess(self):
        class TestTrainable(Trainable):
            def _train(self):
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
            tune.track.log(
                name=tune.track.trial_name(), trial_id=tune.track.trial_id())

        analysis = tune.run(track_train, stop={TRAINING_ITERATION: 1})
        trial = analysis.trials[0]
        self.assertEqual(trial.last_result.get("name"), str(trial))
        self.assertEqual(trial.last_result.get("trial_id"), trial.trial_id)

    @patch("ray.tune.ray_trial_executor.TRIAL_CLEANUP_THRESHOLD", 3)
    def testLotsOfStops(self):
        class TestTrainable(Trainable):
            def _train(self):
                result = {"name": self.trial_name, "trial_id": self.trial_id}
                return result

            def _stop(self):
                time.sleep(2)
                open(os.path.join(self.logdir, "marker"), "a").close()
                return 1

        analysis = tune.run(
            TestTrainable, num_samples=10, stop={TRAINING_ITERATION: 1})
        ray.shutdown()
        for trial in analysis.trials:
            path = os.path.join(trial.logdir, "marker")
            assert os.path.exists(path)

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

    def testDurableTrainable(self):
        class TestTrain(DurableTrainable):
            def _setup(self, config):
                self.state = {"hi": 1, "iter": 0}

            def _train(self):
                self.state["iter"] += 1
                return {"timesteps_this_iter": 1, "done": True}

            def _save(self, path):
                return self.state

            def _restore(self, state):
                self.state = state

        sync_client = mock_storage_client()
        mock_get_client = "ray.tune.durable_trainable.get_cloud_sync_client"
        with patch(mock_get_client) as mock_get_cloud_sync_client:
            mock_get_cloud_sync_client.return_value = sync_client
            test_trainable = TestTrain(remote_checkpoint_dir=MOCK_REMOTE_DIR)
            checkpoint_path = test_trainable.save()
            test_trainable.train()
            test_trainable.state["hi"] = 2
            test_trainable.restore(checkpoint_path)
            self.assertEqual(test_trainable.state["hi"], 1)

        self.addCleanup(shutil.rmtree, MOCK_REMOTE_DIR)

    def testCheckpointDict(self):
        class TestTrain(Trainable):
            def _setup(self, config):
                self.state = {"hi": 1}

            def _train(self):
                return {"timesteps_this_iter": 1, "done": True}

            def _save(self, path):
                return self.state

            def _restore(self, state):
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


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
