import os
import unittest

import ray
from ray.rllib import _register_all

from ray.tune.result import TIMESTEPS_TOTAL
from ray.tune import Trainable, TuneError
from ray.tune import register_trainable, run_experiments
from ray.tune.logger import LegacyLoggerCallback, Logger
from ray.tune.experiment import Experiment
from ray.tune.trial import Trial, ExportFormat


class RunExperimentTest(unittest.TestCase):
    def setUp(self):
        os.environ["TUNE_STATE_REFRESH_PERIOD"] = "0.1"

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
            def step(self):
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
            def step(self):
                return {"timesteps_this_iter": 1, "done": True}

            def save_checkpoint(self, path):
                checkpoint = os.path.join(path, "checkpoint")
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
            def step(self):
                return {"timesteps_this_iter": 1, "done": True}

            def _export_model(self, export_formats, export_dir):
                path = os.path.join(export_dir, "exported")
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
            def step(self):
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
            def step(self):
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

    def testCustomLoggerNoAutoLogging(self):
        """Does not create CSV/JSON logger callbacks automatically"""
        os.environ["TUNE_DISABLE_AUTO_CALLBACK_LOGGERS"] = "1"

        class CustomLogger(Logger):
            def on_result(self, result):
                with open(os.path.join(self.logdir, "test.log"), "w") as f:
                    f.write("hi")

        [trial] = run_experiments(
            {
                "foo": {
                    "run": "__fake",
                    "stop": {
                        "training_iteration": 1
                    }
                }
            },
            callbacks=[LegacyLoggerCallback(logger_classes=[CustomLogger])])
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
        self.assertFalse(
            os.path.exists(os.path.join(trial.logdir, "params.json")))

        [trial] = run_experiments(
            {
                "foo": {
                    "run": "__fake",
                    "stop": {
                        "training_iteration": 1
                    }
                }
            },
            callbacks=[LegacyLoggerCallback(logger_classes=[])])
        self.assertFalse(
            os.path.exists(os.path.join(trial.logdir, "params.json")))

    def testCustomLoggerWithAutoLogging(self):
        """Creates CSV/JSON logger callbacks automatically"""
        if "TUNE_DISABLE_AUTO_CALLBACK_LOGGERS" in os.environ:
            del os.environ["TUNE_DISABLE_AUTO_CALLBACK_LOGGERS"]

        class CustomLogger(Logger):
            def on_result(self, result):
                with open(os.path.join(self.logdir, "test.log"), "w") as f:
                    f.write("hi")

        [trial] = run_experiments(
            {
                "foo": {
                    "run": "__fake",
                    "stop": {
                        "training_iteration": 1
                    }
                }
            },
            callbacks=[LegacyLoggerCallback(logger_classes=[CustomLogger])])
        self.assertTrue(os.path.exists(os.path.join(trial.logdir, "test.log")))
        self.assertTrue(
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

        [trial] = run_experiments(
            {
                "foo": {
                    "run": "__fake",
                    "stop": {
                        "training_iteration": 1
                    }
                }
            },
            callbacks=[LegacyLoggerCallback(logger_classes=[])])
        self.assertTrue(
            os.path.exists(os.path.join(trial.logdir, "params.json")))

    def testCustomTrialString(self):
        [trial] = run_experiments({
            "foo": {
                "run": "__fake",
                "stop": {
                    "training_iteration": 1
                },
                "trial_name_creator":
                    lambda t: "{}_{}_321".format(t.trainable_name, t.trial_id)
            }
        })
        self.assertEquals(
            str(trial), "{}_{}_321".format(trial.trainable_name,
                                           trial.trial_id))


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
