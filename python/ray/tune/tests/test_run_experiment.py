from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

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
                             EPISODES_TOTAL, TRAINING_ITERATION)
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
                "config": {
                    "script_min_iter_time_s": 0
                }
            },
            "bar": {
                "run": "f1",
                "config": {
                    "script_min_iter_time_s": 0
                }
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
            "config": {
                "script_min_iter_time_s": 0
            }
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
            "config": {
                "script_min_iter_time_s": 0
            }
        })
        exp2 = Experiment(**{
            "name": "bar",
            "run": "f1",
            "config": {
                "script_min_iter_time_s": 0
            }
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
                "config": {
                    "script_min_iter_time_s": 0
                }
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

    def testDeprecatedResources(self):
        class train(Trainable):
            def _train(self):
                return {"timesteps_this_iter": 1, "done": True}

        trials = run_experiments({
            "foo": {
                "run": train,
                "trial_resources": {
                    "cpu": 1
                }
            }
        })
        for trial in trials:
            self.assertEqual(trial.status, Trial.TERMINATED)

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

