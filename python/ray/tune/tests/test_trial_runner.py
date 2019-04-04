from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import os
import shutil
import sys
import tempfile
import time
import unittest

import ray
from ray.rllib import _register_all

from ray import tune
from ray.tune import Trainable, TuneError
from ray.tune import register_env, register_trainable, run_experiments
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.schedulers import TrialScheduler, FIFOScheduler
from ray.tune.registry import _global_registry, TRAINABLE_CLASS
from ray.tune.result import (DEFAULT_RESULTS_DIR, TIMESTEPS_TOTAL, DONE,
                             HOSTNAME, NODE_IP, PID, EPISODES_TOTAL,
                             TRAINING_ITERATION, TIMESTEPS_THIS_ITER,
                             TIME_THIS_ITER_S, TIME_TOTAL_S)
from ray.tune.logger import Logger
from ray.tune.util import pin_in_object_store, get_pinned_object
from ray.tune.experiment import Experiment
from ray.tune.trial import (Trial, ExportFormat, Resources, resources_to_json,
                            json_to_resources)
from ray.tune.trial_runner import TrialRunner
from ray.tune.suggest import grid_search, BasicVariantGenerator
from ray.tune.suggest.suggestion import (_MockSuggestionAlgorithm,
                                         SuggestionAlgorithm)
from ray.tune.suggest.variant_generator import (RecursiveDependencyError,
                                                resolve_nested_dict)

if sys.version_info >= (3, 3):
    from unittest.mock import patch
else:
    from mock import patch


class TrainableFunctionApiTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=4, num_gpus=0)

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

        class A(object):
            pass

        class B(Trainable):
            pass

        register_trainable("foo", train)
        register_trainable("foo", B)
        self.assertRaises(TypeError, lambda: register_trainable("foo", B()))
        self.assertRaises(TypeError, lambda: register_trainable("foo", A))

    def testRegisterTrainableCallable(self):
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

        # Infeasible even with queueing enabled (no gpus)
        self.assertRaises(TuneError, lambda: f(1, 1, True))

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
            assert "/tmp/logdir/foo" in os.getcwd(), os.getcwd()
            reporter(timesteps_total=1)

        register_trainable("f1", train)
        run_experiments({
            "foo": {
                "run": "f1",
                "local_dir": "/tmp/logdir",
                "config": {
                    "a": "b"
                },
            }
        })

    def testUploadDirNone(self):
        def train(config, reporter):
            reporter(timesteps_total=1)

        [trial] = run_experiments({
            "foo": {
                "run": train,
                "upload_dir": None,
                "config": {
                    "a": "b"
                },
            }
        })
        self.assertFalse(trial.upload_dir)

    def testLogdirStartingWithTilde(self):
        local_dir = '~/ray_results/local_dir'

        def train(config, reporter):
            cwd = os.getcwd()
            assert cwd.startswith(os.path.expanduser(local_dir)), cwd
            assert not cwd.startswith('~'), cwd
            reporter(timesteps_total=1)

        register_trainable('f1', train)
        run_experiments({
            'foo': {
                'run': 'f1',
                'local_dir': local_dir,
                'config': {
                    'a': 'b'
                },
            }
        })

    def testLongFilename(self):
        def train(config, reporter):
            assert "/tmp/logdir/foo" in os.getcwd(), os.getcwd()
            reporter(timesteps_total=1)

        register_trainable("f1", train)
        run_experiments({
            "foo": {
                "run": "f1",
                "local_dir": "/tmp/logdir",
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
                reporter(mean_accuracy=float('inf'))

        register_trainable("f1", train)
        [trial] = run_experiments({
            "foo": {
                "run": "f1",
            }
        })
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result['mean_accuracy'], float('inf'))

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


class RunExperimentTest(unittest.TestCase):
    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def testDict(self):
        def train(config, reporter):
            for i in range(100):
                reporter(timesteps_total=i)

        register_trainable("f1", train)
        trials = run_experiments({
            "foo": {
                "run": "f1",
            },
            "bar": {
                "run": "f1",
            }
        })
        for trial in trials:
            self.assertEqual(trial.status, Trial.TERMINATED)
            self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 99)

    def testExperiment(self):
        def train(config, reporter):
            for i in range(100):
                reporter(timesteps_total=i)

        register_trainable("f1", train)
        exp1 = Experiment(**{
            "name": "foo",
            "run": "f1",
        })
        [trial] = run_experiments(exp1)
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 99)

    def testExperimentList(self):
        def train(config, reporter):
            for i in range(100):
                reporter(timesteps_total=i)

        register_trainable("f1", train)
        exp1 = Experiment(**{
            "name": "foo",
            "run": "f1",
        })
        exp2 = Experiment(**{
            "name": "bar",
            "run": "f1",
        })
        trials = run_experiments([exp1, exp2])
        for trial in trials:
            self.assertEqual(trial.status, Trial.TERMINATED)
            self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 99)

    def testAutoregisterTrainable(self):
        def train(config, reporter):
            for i in range(100):
                reporter(timesteps_total=i)

        class B(Trainable):
            def _train(self):
                return {"timesteps_this_iter": 1, "done": True}

        register_trainable("f1", train)
        trials = run_experiments({
            "foo": {
                "run": train,
            },
            "bar": {
                "run": B
            }
        })
        for trial in trials:
            self.assertEqual(trial.status, Trial.TERMINATED)

    def testCheckpointAtEnd(self):
        class train(Trainable):
            def _train(self):
                return {"timesteps_this_iter": 1, "done": True}

            def _save(self, path):
                checkpoint = path + "/checkpoint"
                with open(checkpoint, "w") as f:
                    f.write("OK")
                return checkpoint

        trials = run_experiments({
            "foo": {
                "run": train,
                "checkpoint_at_end": True
            }
        })
        for trial in trials:
            self.assertEqual(trial.status, Trial.TERMINATED)
            self.assertTrue(trial.has_checkpoint())

    def testExportFormats(self):
        class train(Trainable):
            def _train(self):
                return {"timesteps_this_iter": 1, "done": True}

            def _export_model(self, export_formats, export_dir):
                path = export_dir + "/exported"
                with open(path, "w") as f:
                    f.write("OK")
                return {export_formats[0]: path}

        trials = run_experiments({
            "foo": {
                "run": train,
                "export_formats": ["format"]
            }
        })
        for trial in trials:
            self.assertEqual(trial.status, Trial.TERMINATED)
            self.assertTrue(
                os.path.exists(os.path.join(trial.logdir, "exported")))

    def testInvalidExportFormats(self):
        class train(Trainable):
            def _train(self):
                return {"timesteps_this_iter": 1, "done": True}

            def _export_model(self, export_formats, export_dir):
                ExportFormat.validate(export_formats)
                return {}

        def fail_trial():
            run_experiments({
                "foo": {
                    "run": train,
                    "export_formats": ["format"]
                }
            })

        self.assertRaises(TuneError, fail_trial)

    def testCustomResources(self):
        ray.shutdown()
        ray.init(resources={"hi": 3})

        class train(Trainable):
            def _train(self):
                return {"timesteps_this_iter": 1, "done": True}

        trials = run_experiments({
            "foo": {
                "run": train,
                "resources_per_trial": {
                    "cpu": 1,
                    "custom_resources": {
                        "hi": 2
                    }
                }
            }
        })
        for trial in trials:
            self.assertEqual(trial.status, Trial.TERMINATED)

    def testCustomLogger(self):
        class CustomLogger(Logger):
            def on_result(self, result):
                with open(os.path.join(self.logdir, "test.log"), "w") as f:
                    f.write("hi")

        [trial] = run_experiments({
            "foo": {
                "run": "__fake",
                "stop": {
                    "training_iteration": 1
                },
                "loggers": [CustomLogger]
            }
        })
        self.assertTrue(os.path.exists(os.path.join(trial.logdir, "test.log")))
        self.assertFalse(
            os.path.exists(os.path.join(trial.logdir, "params.json")))

        [trial] = run_experiments({
            "foo": {
                "run": "__fake",
                "stop": {
                    "training_iteration": 1
                }
            }
        })
        self.assertTrue(
            os.path.exists(os.path.join(trial.logdir, "params.json")))

        [trial] = run_experiments({
            "foo": {
                "run": "__fake",
                "stop": {
                    "training_iteration": 1
                },
                "loggers": []
            }
        })
        self.assertFalse(
            os.path.exists(os.path.join(trial.logdir, "params.json")))

    def testCustomTrialString(self):
        [trial] = run_experiments({
            "foo": {
                "run": "__fake",
                "stop": {
                    "training_iteration": 1
                },
                "trial_name_creator": tune.function(
                    lambda t: "{}_{}_321".format(t.trainable_name, t.trial_id))
            }
        })
        self.assertEquals(
            str(trial), "{}_{}_321".format(trial.trainable_name,
                                           trial.trial_id))

    def testSyncFunction(self):
        def fail_sync_local():
            [trial] = run_experiments({
                "foo": {
                    "run": "__fake",
                    "stop": {
                        "training_iteration": 1
                    },
                    "upload_dir": "test",
                    "sync_function": "ls {remote_dir}"
                }
            })

        self.assertRaises(AssertionError, fail_sync_local)

        def fail_sync_remote():
            [trial] = run_experiments({
                "foo": {
                    "run": "__fake",
                    "stop": {
                        "training_iteration": 1
                    },
                    "upload_dir": "test",
                    "sync_function": "ls {local_dir}"
                }
            })

        self.assertRaises(AssertionError, fail_sync_remote)

        def sync_func(local, remote):
            with open(os.path.join(local, "test.log"), "w") as f:
                f.write(remote)

        [trial] = run_experiments({
            "foo": {
                "run": "__fake",
                "stop": {
                    "training_iteration": 1
                },
                "upload_dir": "test",
                "sync_function": tune.function(sync_func)
            }
        })
        self.assertTrue(os.path.exists(os.path.join(trial.logdir, "test.log")))


class VariantGeneratorTest(unittest.TestCase):
    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def generate_trials(self, spec, name):
        suggester = BasicVariantGenerator()
        suggester.add_configurations({name: spec})
        return suggester.next_trials()

    def testParseToTrials(self):
        trials = self.generate_trials({
            "run": "PPO",
            "num_samples": 2,
            "max_failures": 5,
            "config": {
                "env": "Pong-v0",
                "foo": "bar"
            },
        }, "tune-pong")
        trials = list(trials)
        self.assertEqual(len(trials), 2)
        self.assertEqual(str(trials[0]), "PPO_Pong-v0_0")
        self.assertEqual(trials[0].config, {"foo": "bar", "env": "Pong-v0"})
        self.assertEqual(trials[0].trainable_name, "PPO")
        self.assertEqual(trials[0].experiment_tag, "0")
        self.assertEqual(trials[0].max_failures, 5)
        self.assertEqual(trials[0].local_dir,
                         os.path.join(DEFAULT_RESULTS_DIR, "tune-pong"))
        self.assertEqual(trials[1].experiment_tag, "1")

    def testEval(self):
        trials = self.generate_trials({
            "run": "PPO",
            "config": {
                "foo": {
                    "eval": "2 + 2"
                },
            },
        }, "eval")
        trials = list(trials)
        self.assertEqual(len(trials), 1)
        self.assertEqual(trials[0].config, {"foo": 4})
        self.assertEqual(trials[0].experiment_tag, "0_foo=4")

    def testGridSearch(self):
        trials = self.generate_trials({
            "run": "PPO",
            "config": {
                "bar": {
                    "grid_search": [True, False]
                },
                "foo": {
                    "grid_search": [1, 2, 3]
                },
            },
        }, "grid_search")
        trials = list(trials)
        self.assertEqual(len(trials), 6)
        self.assertEqual(trials[0].config, {"bar": True, "foo": 1})
        self.assertEqual(trials[0].experiment_tag, "0_bar=True,foo=1")
        self.assertEqual(trials[1].config, {"bar": False, "foo": 1})
        self.assertEqual(trials[1].experiment_tag, "1_bar=False,foo=1")
        self.assertEqual(trials[2].config, {"bar": True, "foo": 2})
        self.assertEqual(trials[3].config, {"bar": False, "foo": 2})
        self.assertEqual(trials[4].config, {"bar": True, "foo": 3})
        self.assertEqual(trials[5].config, {"bar": False, "foo": 3})

    def testGridSearchAndEval(self):
        trials = self.generate_trials({
            "run": "PPO",
            "config": {
                "qux": tune.sample_from(lambda spec: 2 + 2),
                "bar": grid_search([True, False]),
                "foo": grid_search([1, 2, 3]),
            },
        }, "grid_eval")
        trials = list(trials)
        self.assertEqual(len(trials), 6)
        self.assertEqual(trials[0].config, {"bar": True, "foo": 1, "qux": 4})
        self.assertEqual(trials[0].experiment_tag, "0_bar=True,foo=1,qux=4")

    def testConditionResolution(self):
        trials = self.generate_trials({
            "run": "PPO",
            "config": {
                "x": 1,
                "y": tune.sample_from(lambda spec: spec.config.x + 1),
                "z": tune.sample_from(lambda spec: spec.config.y + 1),
            },
        }, "condition_resolution")
        trials = list(trials)
        self.assertEqual(len(trials), 1)
        self.assertEqual(trials[0].config, {"x": 1, "y": 2, "z": 3})

    def testDependentLambda(self):
        trials = self.generate_trials({
            "run": "PPO",
            "config": {
                "x": grid_search([1, 2]),
                "y": tune.sample_from(lambda spec: spec.config.x * 100),
            },
        }, "dependent_lambda")
        trials = list(trials)
        self.assertEqual(len(trials), 2)
        self.assertEqual(trials[0].config, {"x": 1, "y": 100})
        self.assertEqual(trials[1].config, {"x": 2, "y": 200})

    def testDependentGridSearch(self):
        trials = self.generate_trials({
            "run": "PPO",
            "config": {
                "x": grid_search([
                    tune.sample_from(lambda spec: spec.config.y * 100),
                    tune.sample_from(lambda spec: spec.config.y * 200)
                ]),
                "y": tune.sample_from(lambda spec: 1),
            },
        }, "dependent_grid_search")
        trials = list(trials)
        self.assertEqual(len(trials), 2)
        self.assertEqual(trials[0].config, {"x": 100, "y": 1})
        self.assertEqual(trials[1].config, {"x": 200, "y": 1})

    def test_resolve_dict(self):
        config = {
            "a": {
                "b": 1,
                "c": 2,
            },
            "b": {
                "a": 3
            }
        }
        resolved = resolve_nested_dict(config)
        for k, v in [(("a", "b"), 1), (("a", "c"), 2), (("b", "a"), 3)]:
            self.assertEqual(resolved.get(k), v)

    def testRecursiveDep(self):
        try:
            list(
                self.generate_trials({
                    "run": "PPO",
                    "config": {
                        "foo": tune.sample_from(lambda spec: spec.config.foo),
                    },
                }, "recursive_dep"))
        except RecursiveDependencyError as e:
            assert "`foo` recursively depends on" in str(e), e
        else:
            assert False

    def testMaxConcurrentSuggestions(self):
        """Checks that next_trials() supports throttling."""
        experiment_spec = {
            "run": "PPO",
            "num_samples": 6,
        }
        experiments = [Experiment.from_json("test", experiment_spec)]

        searcher = _MockSuggestionAlgorithm(max_concurrent=4)
        searcher.add_configurations(experiments)
        trials = searcher.next_trials()
        self.assertEqual(len(trials), 4)
        self.assertEqual(searcher.next_trials(), [])

        finished_trial = trials.pop()
        searcher.on_trial_complete(finished_trial.trial_id)
        self.assertEqual(len(searcher.next_trials()), 1)

        finished_trial = trials.pop()
        searcher.on_trial_complete(finished_trial.trial_id)

        finished_trial = trials.pop()
        searcher.on_trial_complete(finished_trial.trial_id)

        finished_trial = trials.pop()
        searcher.on_trial_complete(finished_trial.trial_id)
        self.assertEqual(len(searcher.next_trials()), 1)
        self.assertEqual(len(searcher.next_trials()), 0)


def create_mock_components():
    class _MockScheduler(FIFOScheduler):
        errored_trials = []

        def on_trial_error(self, trial_runner, trial):
            self.errored_trials += [trial]

    class _MockSearchAlg(BasicVariantGenerator):
        errored_trials = []

        def on_trial_complete(self, trial_id, error=False, **kwargs):
            if error:
                self.errored_trials += [trial_id]

    searchalg = _MockSearchAlg()
    scheduler = _MockScheduler()
    return searchalg, scheduler


class TrialRunnerTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def testTrialStatus(self):
        ray.init()
        trial = Trial("__fake")
        trial_executor = RayTrialExecutor()
        self.assertEqual(trial.status, Trial.PENDING)
        trial_executor.start_trial(trial)
        self.assertEqual(trial.status, Trial.RUNNING)
        trial_executor.stop_trial(trial)
        self.assertEqual(trial.status, Trial.TERMINATED)
        trial_executor.stop_trial(trial, error=True)
        self.assertEqual(trial.status, Trial.ERROR)

    def testExperimentTagTruncation(self):
        ray.init()

        def train(config, reporter):
            reporter(timesteps_total=1)

        trial_executor = RayTrialExecutor()
        register_trainable("f1", train)

        experiments = {
            "foo": {
                "run": "f1",
                "config": {
                    "a" * 50: tune.sample_from(lambda spec: 5.0 / 7),
                    "b" * 50: tune.sample_from(lambda spec: "long" * 40)
                },
            }
        }

        for name, spec in experiments.items():
            trial_generator = BasicVariantGenerator()
            trial_generator.add_configurations({name: spec})
            for trial in trial_generator.next_trials():
                trial_executor.start_trial(trial)
                self.assertLessEqual(len(trial.logdir), 200)
                trial_executor.stop_trial(trial)

    def testExtraResources(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 1
            },
            "resources": Resources(cpu=1, gpu=0, extra_cpu=3, extra_gpu=1),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

    def testCustomResources(self):
        ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 1
            },
            "resources": Resources(cpu=1, gpu=0, custom_resources={"a": 2}),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

    def testExtraCustomResources(self):
        ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 1
            },
            "resources": Resources(
                cpu=1, gpu=0, extra_custom_resources={"a": 2}),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertTrue(sum(t.status == Trial.RUNNING for t in trials) < 2)
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

    def testCustomResources2(self):
        ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
        runner = TrialRunner(BasicVariantGenerator())
        resource1 = Resources(cpu=1, gpu=0, extra_custom_resources={"a": 2})
        self.assertTrue(runner.has_resources(resource1))
        resource2 = Resources(cpu=1, gpu=0, custom_resources={"a": 2})
        self.assertTrue(runner.has_resources(resource2))
        resource3 = Resources(cpu=1, gpu=0, custom_resources={"a": 3})
        self.assertFalse(runner.has_resources(resource3))
        resource4 = Resources(cpu=1, gpu=0, extra_custom_resources={"a": 3})
        self.assertFalse(runner.has_resources(resource4))

    def testFractionalGpus(self):
        ray.init(num_cpus=4, num_gpus=1)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "resources": Resources(cpu=1, gpu=0.5),
        }
        trials = [
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs)
        ]
        for t in trials:
            runner.add_trial(t)

        for _ in range(10):
            runner.step()

        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(trials[2].status, Trial.PENDING)
        self.assertEqual(trials[3].status, Trial.PENDING)

    def testResourceScheduler(self):
        ray.init(num_cpus=4, num_gpus=1)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 1
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.TERMINATED)

    def testMultiStepRun(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 5
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.RUNNING)

    def testMultiStepRun2(self):
        """Checks that runner.step throws when overstepping."""
        ray.init(num_cpus=1)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 2
            },
            "resources": Resources(cpu=1, gpu=0),
        }
        trials = [Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertRaises(TuneError, runner.step)

    def testChangeResources(self):
        """Checks that resource requirements can be changed on fly."""
        ray.init(num_cpus=2)

        class ChangingScheduler(FIFOScheduler):
            def on_trial_result(self, trial_runner, trial, result):
                if result["training_iteration"] == 1:
                    executor = trial_runner.trial_executor
                    executor.stop_trial(trial, stop_logger=False)
                    trial.update_resources(2, 0)
                    executor.start_trial(trial)
                return TrialScheduler.CONTINUE

        runner = TrialRunner(
            BasicVariantGenerator(), scheduler=ChangingScheduler())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 2
            },
            "resources": Resources(cpu=1, gpu=0),
        }
        trials = [Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(runner.trial_executor._committed_resources.cpu, 1)
        self.assertRaises(ValueError, lambda: trials[0].update_resources(2, 0))

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(runner.trial_executor._committed_resources.cpu, 2)

    def testErrorHandling(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 1
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        _global_registry.register(TRAINABLE_CLASS, "asdf", None)
        trials = [Trial("asdf", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.ERROR)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.ERROR)
        self.assertEqual(trials[1].status, Trial.RUNNING)

    def testThrowOnOverstep(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner(BasicVariantGenerator())
        runner.step()
        self.assertRaises(TuneError, runner.step)

    def testFailureRecoveryDisabled(self):
        ray.init(num_cpus=1, num_gpus=1)
        searchalg, scheduler = create_mock_components()

        runner = TrialRunner(searchalg, scheduler=scheduler)
        kwargs = {
            "resources": Resources(cpu=1, gpu=1),
            "checkpoint_freq": 1,
            "max_failures": 0,
            "config": {
                "mock_error": True,
            },
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[0].status, Trial.ERROR)
        self.assertEqual(trials[0].num_failures, 1)
        self.assertEqual(len(searchalg.errored_trials), 1)
        self.assertEqual(len(scheduler.errored_trials), 1)

    def testFailureRecoveryEnabled(self):
        ray.init(num_cpus=1, num_gpus=1)
        searchalg, scheduler = create_mock_components()

        runner = TrialRunner(searchalg, scheduler=scheduler)

        kwargs = {
            "resources": Resources(cpu=1, gpu=1),
            "checkpoint_freq": 1,
            "max_failures": 1,
            "config": {
                "mock_error": True,
            },
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[0].num_failures, 1)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(len(searchalg.errored_trials), 0)
        self.assertEqual(len(scheduler.errored_trials), 0)

    def testFailureRecoveryNodeRemoval(self):
        ray.init(num_cpus=1, num_gpus=1)
        searchalg, scheduler = create_mock_components()

        runner = TrialRunner(searchalg, scheduler=scheduler)

        kwargs = {
            "resources": Resources(cpu=1, gpu=1),
            "checkpoint_freq": 1,
            "max_failures": 1,
            "config": {
                "mock_error": True,
            },
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        with patch('ray.global_state.cluster_resources') as resource_mock:
            resource_mock.return_value = {"CPU": 1, "GPU": 1}
            runner.step()
            self.assertEqual(trials[0].status, Trial.RUNNING)

            runner.step()
            self.assertEqual(trials[0].status, Trial.RUNNING)

            # Mimic a node failure
            resource_mock.return_value = {"CPU": 0, "GPU": 0}
            runner.step()
            self.assertEqual(trials[0].status, Trial.PENDING)
            self.assertEqual(trials[0].num_failures, 1)
            self.assertEqual(len(searchalg.errored_trials), 0)
            self.assertEqual(len(scheduler.errored_trials), 1)

    def testFailureRecoveryMaxFailures(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "resources": Resources(cpu=1, gpu=1),
            "checkpoint_freq": 1,
            "max_failures": 2,
            "config": {
                "mock_error": True,
                "persistent_error": True,
            },
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[0].num_failures, 1)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[0].num_failures, 2)
        runner.step()
        self.assertEqual(trials[0].status, Trial.ERROR)
        self.assertEqual(trials[0].num_failures, 3)

    def testCheckpointing(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 1
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.set_info.remote(1)), 1)
        path = runner.trial_executor.save(trials[0])
        kwargs["restore_path"] = path

        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[1].runner.get_info.remote()), 1)
        self.addCleanup(os.remove, path)

    def testRestoreMetricsAfterCheckpointing(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.set_info.remote(1)), 1)
        path = runner.trial_executor.save(trials[0])
        runner.trial_executor.stop_trial(trials[0])
        kwargs["restore_path"] = path

        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[1].last_result["timesteps_since_restore"], 10)
        self.assertEqual(trials[1].last_result["iterations_since_restore"], 1)
        self.assertGreater(trials[1].last_result["time_since_restore"], 0)
        runner.step()
        self.assertEqual(trials[1].last_result["timesteps_since_restore"], 20)
        self.assertEqual(trials[1].last_result["iterations_since_restore"], 2)
        self.assertGreater(trials[1].last_result["time_since_restore"], 0)
        self.addCleanup(os.remove, path)

    def testCheckpointingAtEnd(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 2
            },
            "checkpoint_at_end": True,
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        runner.step()
        self.assertEqual(trials[0].last_result[DONE], True)
        self.assertEqual(trials[0].has_checkpoint(), True)

    def testResultDone(self):
        """Tests that last_result is marked `done` after trial is complete."""
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 2
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertNotEqual(trials[0].last_result[DONE], True)
        runner.step()
        self.assertEqual(trials[0].last_result[DONE], True)

    def testPauseThenResume(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 2
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.get_info.remote()), None)

        self.assertEqual(ray.get(trials[0].runner.set_info.remote(1)), 1)

        runner.trial_executor.pause_trial(trials[0])
        self.assertEqual(trials[0].status, Trial.PAUSED)

        runner.trial_executor.resume_trial(trials[0])
        self.assertEqual(trials[0].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.get_info.remote()), 1)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)

    def testStepHook(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner(BasicVariantGenerator())

        def on_step_begin(self):
            self._update_avail_resources()
            cnt = self.pre_step if hasattr(self, 'pre_step') else 0
            setattr(self, 'pre_step', cnt + 1)

        def on_step_end(self):
            cnt = self.pre_step if hasattr(self, 'post_step') else 0
            setattr(self, 'post_step', 1 + cnt)

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
        runner = TrialRunner(BasicVariantGenerator())
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
        searcher = _MockSuggestionAlgorithm(max_concurrent=10)
        searcher.add_configurations(experiments)
        runner = TrialRunner(search_alg=searcher)
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
        searcher = _MockSuggestionAlgorithm(max_concurrent=10)
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
        searcher = _MockSuggestionAlgorithm(max_concurrent=10)
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
        searcher = _MockSuggestionAlgorithm(max_concurrent=1)
        searcher.add_configurations(experiments)
        runner = TrialRunner(search_alg=searcher)
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
        self.assertFalse(searcher.is_finished())
        self.assertFalse(runner.is_finished())

        searcher.stall = False

        runner.step()
        trials = runner.get_trials()
        self.assertEqual(trials[2].status, Trial.RUNNING)
        self.assertEqual(len(searcher.live_trials), 1)

        runner.step()
        self.assertEqual(trials[2].status, Trial.TERMINATED)
        self.assertEqual(len(searcher.live_trials), 0)
        self.assertTrue(searcher.is_finished())
        self.assertTrue(runner.is_finished())

    def testSearchAlgFinishes(self):
        """Empty SearchAlg changing state in `next_trials` does not crash."""

        class FinishFastAlg(SuggestionAlgorithm):
            _index = 0

            def next_trials(self):
                trials = []
                self._index += 1

                for trial in self._trial_generator:
                    trials += [trial]
                    break

                if self._index > 4:
                    self._finished = True
                return trials

            def _suggest(self, trial_id):
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

        runner = TrialRunner(
            BasicVariantGenerator(), metadata_checkpoint_dir=tmpdir)
        trials = [
            Trial(
                "__fake",
                trial_id="trial_terminate",
                stopping_criterion={"training_iteration": 1},
                checkpoint_freq=1)
        ]
        runner.add_trial(trials[0])
        runner.step()  # start
        runner.step()
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
        runner.step()
        runner.step()
        runner.step()
        self.assertEquals(trials[1].status, Trial.ERROR)

        trials += [
            Trial(
                "__fake",
                trial_id="trial_succ",
                stopping_criterion={"training_iteration": 2},
                checkpoint_freq=1)
        ]
        runner.add_trial(trials[2])
        runner.step()
        self.assertEquals(len(runner.trial_executor.get_checkpoints()), 3)
        self.assertEquals(trials[2].status, Trial.RUNNING)

        runner2 = TrialRunner.restore(tmpdir)
        for tid in ["trial_terminate", "trial_fail"]:
            original_trial = runner.get_trial(tid)
            restored_trial = runner2.get_trial(tid)
            self.assertEqual(original_trial.status, restored_trial.status)

        restored_trial = runner2.get_trial("trial_succ")
        self.assertEqual(Trial.PENDING, restored_trial.status)

        runner2.step()
        runner2.step()
        runner2.step()
        self.assertRaises(TuneError, runner2.step)
        shutil.rmtree(tmpdir)

    def testTrialNoSave(self):
        """Check that non-checkpointing trials are not saved."""
        ray.init(num_cpus=3)
        tmpdir = tempfile.mkdtemp()

        runner = TrialRunner(
            BasicVariantGenerator(), metadata_checkpoint_dir=tmpdir)

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

        runner2 = TrialRunner.restore(tmpdir)
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
            config={
                "callbacks": {
                    "on_episode_start": tune.function(lambda i: i),
                }
            },
            checkpoint_freq=1)
        tmpdir = tempfile.mkdtemp()
        runner = TrialRunner(
            BasicVariantGenerator(), metadata_checkpoint_dir=tmpdir)
        runner.add_trial(trial)
        for i in range(5):
            runner.step()
        # force checkpoint
        runner.checkpoint()
        runner2 = TrialRunner.restore(tmpdir)
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
        runner = TrialRunner(
            BasicVariantGenerator(), metadata_checkpoint_dir=tmpdir)
        runner.add_trial(trial)
        for i in range(5):
            runner.step()
        # force checkpoint
        runner.checkpoint()
        self.assertEquals(count_checkpoints(tmpdir), 1)

        runner2 = TrialRunner.restore(tmpdir)
        for i in range(5):
            runner2.step()
        self.assertEquals(count_checkpoints(tmpdir), 2)

        runner2.checkpoint()
        self.assertEquals(count_checkpoints(tmpdir), 2)
        shutil.rmtree(tmpdir)


class SearchAlgorithmTest(unittest.TestCase):
    def testNestedSuggestion(self):
        class TestSuggestion(SuggestionAlgorithm):
            def _suggest(self, trial_id):
                return {"a": {"b": {"c": {"d": 4, "e": 5}}}}

        alg = TestSuggestion()
        alg.add_configurations({"test": {"run": "__fake"}})
        trial = alg.next_trials()[0]
        self.assertTrue("e=5" in trial.experiment_tag)
        self.assertTrue("d=4" in trial.experiment_tag)


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
    unittest.main(verbosity=2)
