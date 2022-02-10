import os
import tempfile
from collections import namedtuple
from multiprocessing import Queue
import unittest

import numpy as np

from ray.tune import Trainable
from ray.tune.function_runner import wrap_function
from ray.tune.integration.wandb import (
    WandbLoggerCallback,
    _WandbLoggingProcess,
    _WANDB_QUEUE_END,
    WandbLogger,
    WANDB_ENV_VAR,
    WandbTrainableMixin,
    wandb_mixin,
)
from ray.tune.result import TRIAL_INFO
from ray.tune.trial import TrialInfo
from ray.tune.utils.placement_groups import PlacementGroupFactory


class Trial(
    namedtuple(
        "MockTrial",
        [
            "config",
            "trial_id",
            "trial_name",
            "trainable_name",
            "placement_group_factory",
        ],
    )
):
    def __hash__(self):
        return hash(self.trial_id)

    def __str__(self):
        return self.trial_name


class _MockWandbLoggingProcess(_WandbLoggingProcess):
    def __init__(self, queue, exclude, to_config, *args, **kwargs):
        super(_MockWandbLoggingProcess, self).__init__(
            queue, exclude, to_config, *args, **kwargs
        )

        self.logs = Queue()
        self.config_updates = Queue()

    def run(self):
        while True:
            result = self.queue.get()
            if result == _WANDB_QUEUE_END:
                break
            log, config_update = self._handle_result(result)
            self.config_updates.put(config_update)
            self.logs.put(log)


class WandbTestExperimentLogger(WandbLoggerCallback):
    _logger_process_cls = _MockWandbLoggingProcess

    @property
    def trial_processes(self):
        return self._trial_processes


class WandbTestLogger(WandbLogger):
    _experiment_logger_cls = WandbTestExperimentLogger

    @property
    def trial_process(self):
        return self._trial_experiment_logger.trial_processes[self.trial]


class _MockWandbAPI(object):
    def init(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        return self


class _MockWandbTrainableMixin(WandbTrainableMixin):
    _wandb = _MockWandbAPI()


class WandbTestTrainable(_MockWandbTrainableMixin, Trainable):
    pass


class WandbIntegrationTest(unittest.TestCase):
    def setUp(self):
        if WANDB_ENV_VAR in os.environ:
            del os.environ[WANDB_ENV_VAR]

    def tearDown(self):
        if WANDB_ENV_VAR in os.environ:
            del os.environ[WANDB_ENV_VAR]

    def testWandbLegacyLoggerConfig(self):
        trial_config = {"par1": 4, "par2": 9.12345678}
        trial = Trial(
            trial_config, 0, "trial_0", "trainable", PlacementGroupFactory([{"CPU": 1}])
        )

        if WANDB_ENV_VAR in os.environ:
            del os.environ[WANDB_ENV_VAR]

        # Needs at least a project
        with self.assertRaises(ValueError):
            logger = WandbTestLogger(trial_config, "/tmp", trial)

        # No API key
        trial_config["wandb"] = {"project": "test_project"}
        with self.assertRaises(ValueError):
            logger = WandbTestLogger(trial_config, "/tmp", trial)

        # API Key in config
        trial_config["wandb"] = {"project": "test_project", "api_key": "1234"}
        logger = WandbTestLogger(trial_config, "/tmp", trial)
        self.assertEqual(os.environ[WANDB_ENV_VAR], "1234")

        logger.close()
        del os.environ[WANDB_ENV_VAR]

        # API Key file
        with tempfile.NamedTemporaryFile("wt") as fp:
            fp.write("5678")
            fp.flush()

            trial_config["wandb"] = {"project": "test_project", "api_key_file": fp.name}

            logger = WandbTestLogger(trial_config, "/tmp", trial)
            self.assertEqual(os.environ[WANDB_ENV_VAR], "5678")

        logger.close()
        del os.environ[WANDB_ENV_VAR]

        # API Key in env
        os.environ[WANDB_ENV_VAR] = "9012"
        trial_config["wandb"] = {"project": "test_project"}
        logger = WandbTestLogger(trial_config, "/tmp", trial)
        logger.close()

        # From now on, the API key is in the env variable.

        # Default configuration
        trial_config["wandb"] = {"project": "test_project"}

        logger = WandbTestLogger(trial_config, "/tmp", trial)
        self.assertEqual(logger.trial_process.kwargs["project"], "test_project")
        self.assertEqual(logger.trial_process.kwargs["id"], trial.trial_id)
        self.assertEqual(logger.trial_process.kwargs["name"], trial.trial_name)
        self.assertEqual(logger.trial_process.kwargs["group"], trial.trainable_name)
        self.assertIn("config", logger.trial_process._exclude)

        logger.close()

        # log config.
        trial_config["wandb"] = {"project": "test_project", "log_config": True}

        logger = WandbTestLogger(trial_config, "/tmp", trial)
        self.assertNotIn("config", logger.trial_process._exclude)
        self.assertNotIn("metric", logger.trial_process._exclude)

        logger.close()

        # Exclude metric.
        trial_config["wandb"] = {"project": "test_project", "excludes": ["metric"]}

        logger = WandbTestLogger(trial_config, "/tmp", trial)
        self.assertIn("config", logger.trial_process._exclude)
        self.assertIn("metric", logger.trial_process._exclude)

        logger.close()

    def testWandbLegacyLoggerReporting(self):
        trial_config = {"par1": 4, "par2": 9.12345678}
        trial = Trial(
            trial_config, 0, "trial_0", "trainable", PlacementGroupFactory([{"CPU": 1}])
        )

        trial_config["wandb"] = {
            "project": "test_project",
            "api_key": "1234",
            "excludes": ["metric2"],
        }
        logger = WandbTestLogger(trial_config, "/tmp", trial)

        r1 = {
            "metric1": 0.8,
            "metric2": 1.4,
            "metric3": np.asarray(32.0),
            "metric4": np.float32(32.0),
            "const": "text",
            "config": trial_config,
        }

        logger.on_result(r1)

        logged = logger.trial_process.logs.get(timeout=10)
        self.assertIn("metric1", logged)
        self.assertNotIn("metric2", logged)
        self.assertIn("metric3", logged)
        self.assertIn("metric4", logged)
        self.assertNotIn("const", logged)
        self.assertNotIn("config", logged)

        logger.close()

    def testWandbLoggerConfig(self):
        trial_config = {"par1": 4, "par2": 9.12345678}
        trial = Trial(
            trial_config, 0, "trial_0", "trainable", PlacementGroupFactory([{"CPU": 1}])
        )

        if WANDB_ENV_VAR in os.environ:
            del os.environ[WANDB_ENV_VAR]

        # No API key
        with self.assertRaises(ValueError):
            logger = WandbTestExperimentLogger(project="test_project")
            logger.setup()

        # API Key in config
        logger = WandbTestExperimentLogger(project="test_project", api_key="1234")
        logger.setup()
        self.assertEqual(os.environ[WANDB_ENV_VAR], "1234")

        del logger
        del os.environ[WANDB_ENV_VAR]

        # API Key file
        with tempfile.NamedTemporaryFile("wt") as fp:
            fp.write("5678")
            fp.flush()

            logger = WandbTestExperimentLogger(
                project="test_project", api_key_file=fp.name
            )
            logger.setup()
            self.assertEqual(os.environ[WANDB_ENV_VAR], "5678")

        del logger
        del os.environ[WANDB_ENV_VAR]

        # API Key in env
        os.environ[WANDB_ENV_VAR] = "9012"
        logger = WandbTestExperimentLogger(project="test_project")
        logger.setup()
        del logger

        # From now on, the API key is in the env variable.

        logger = WandbTestExperimentLogger(project="test_project")
        logger.log_trial_start(trial)

        self.assertEqual(
            logger.trial_processes[trial].kwargs["project"], "test_project"
        )
        self.assertEqual(logger.trial_processes[trial].kwargs["id"], trial.trial_id)
        self.assertEqual(logger.trial_processes[trial].kwargs["name"], trial.trial_name)
        self.assertEqual(
            logger.trial_processes[trial].kwargs["group"], trial.trainable_name
        )
        self.assertIn("config", logger.trial_processes[trial]._exclude)

        del logger

        # log config.
        logger = WandbTestExperimentLogger(project="test_project", log_config=True)
        logger.log_trial_start(trial)
        self.assertNotIn("config", logger.trial_processes[trial]._exclude)
        self.assertNotIn("metric", logger.trial_processes[trial]._exclude)

        del logger

        # Exclude metric.
        logger = WandbTestExperimentLogger(project="test_project", excludes=["metric"])
        logger.log_trial_start(trial)
        self.assertIn("config", logger.trial_processes[trial]._exclude)
        self.assertIn("metric", logger.trial_processes[trial]._exclude)

        del logger

    def testWandbLoggerReporting(self):
        trial_config = {"par1": 4, "par2": 9.12345678}
        trial = Trial(
            trial_config, 0, "trial_0", "trainable", PlacementGroupFactory([{"CPU": 1}])
        )

        logger = WandbTestExperimentLogger(
            project="test_project", api_key="1234", excludes=["metric2"]
        )
        logger.on_trial_start(0, [], trial)

        r1 = {
            "metric1": 0.8,
            "metric2": 1.4,
            "metric3": np.asarray(32.0),
            "metric4": np.float32(32.0),
            "const": "text",
            "config": trial_config,
        }

        logger.on_trial_result(0, [], trial, r1)

        logged = logger.trial_processes[trial].logs.get(timeout=10)
        self.assertIn("metric1", logged)
        self.assertNotIn("metric2", logged)
        self.assertIn("metric3", logged)
        self.assertIn("metric4", logged)
        self.assertNotIn("const", logged)
        self.assertNotIn("config", logged)

        del logger

    def testWandbMixinConfig(self):
        config = {"par1": 4, "par2": 9.12345678}
        trial = Trial(
            config, 0, "trial_0", "trainable", PlacementGroupFactory([{"CPU": 1}])
        )
        trial_info = TrialInfo(trial)

        config[TRIAL_INFO] = trial_info

        if WANDB_ENV_VAR in os.environ:
            del os.environ[WANDB_ENV_VAR]

        # Needs at least a project
        with self.assertRaises(ValueError):
            trainable = WandbTestTrainable(config)

        # No API key
        config["wandb"] = {"project": "test_project"}
        with self.assertRaises(ValueError):
            trainable = WandbTestTrainable(config)

        # API Key in config
        config["wandb"] = {"project": "test_project", "api_key": "1234"}
        trainable = WandbTestTrainable(config)
        self.assertEqual(os.environ[WANDB_ENV_VAR], "1234")

        del os.environ[WANDB_ENV_VAR]

        # API Key file
        with tempfile.NamedTemporaryFile("wt") as fp:
            fp.write("5678")
            fp.flush()

            config["wandb"] = {"project": "test_project", "api_key_file": fp.name}

            trainable = WandbTestTrainable(config)
            self.assertEqual(os.environ[WANDB_ENV_VAR], "5678")

        del os.environ[WANDB_ENV_VAR]

        # API Key in env
        os.environ[WANDB_ENV_VAR] = "9012"
        config["wandb"] = {"project": "test_project"}
        trainable = WandbTestTrainable(config)

        # From now on, the API key is in the env variable.

        # Default configuration
        config["wandb"] = {"project": "test_project"}
        config[TRIAL_INFO] = trial_info

        trainable = WandbTestTrainable(config)
        self.assertEqual(trainable.wandb.kwargs["project"], "test_project")
        self.assertEqual(trainable.wandb.kwargs["id"], trial.trial_id)
        self.assertEqual(trainable.wandb.kwargs["name"], trial.trial_name)
        self.assertEqual(trainable.wandb.kwargs["group"], "WandbTestTrainable")

    def testWandbDecoratorConfig(self):
        config = {"par1": 4, "par2": 9.12345678}
        trial = Trial(
            config, 0, "trial_0", "trainable", PlacementGroupFactory([{"CPU": 1}])
        )
        trial_info = TrialInfo(trial)

        @wandb_mixin
        def train_fn(config):
            return 1

        train_fn.__mixins__ = (_MockWandbTrainableMixin,)

        config[TRIAL_INFO] = trial_info

        if WANDB_ENV_VAR in os.environ:
            del os.environ[WANDB_ENV_VAR]

        # Needs at least a project
        with self.assertRaises(ValueError):
            wrapped = wrap_function(train_fn)(config)

        # No API key
        config["wandb"] = {"project": "test_project"}
        with self.assertRaises(ValueError):
            wrapped = wrap_function(train_fn)(config)

        # API Key in config
        config["wandb"] = {"project": "test_project", "api_key": "1234"}
        wrapped = wrap_function(train_fn)(config)
        self.assertEqual(os.environ[WANDB_ENV_VAR], "1234")

        del os.environ[WANDB_ENV_VAR]

        # API Key file
        with tempfile.NamedTemporaryFile("wt") as fp:
            fp.write("5678")
            fp.flush()

            config["wandb"] = {"project": "test_project", "api_key_file": fp.name}

            wrapped = wrap_function(train_fn)(config)
            self.assertEqual(os.environ[WANDB_ENV_VAR], "5678")

        del os.environ[WANDB_ENV_VAR]

        # API Key in env
        os.environ[WANDB_ENV_VAR] = "9012"
        config["wandb"] = {"project": "test_project"}
        wrapped = wrap_function(train_fn)(config)

        # From now on, the API key is in the env variable.

        # Default configuration
        config["wandb"] = {"project": "test_project"}
        config[TRIAL_INFO] = trial_info

        wrapped = wrap_function(train_fn)(config)
        self.assertEqual(wrapped.wandb.kwargs["project"], "test_project")
        self.assertEqual(wrapped.wandb.kwargs["id"], trial.trial_id)
        self.assertEqual(wrapped.wandb.kwargs["name"], trial.trial_name)

    def testWandbMixinRllib(self):
        """Test compatibility with RLLib configuration dicts"""
        # Local import to avoid tune dependency on rllib
        try:
            from ray.rllib.agents.ppo import PPOTrainer
        except ImportError:
            self.skipTest("ray[rllib] not available")
            return

        class WandbPPOTrainer(_MockWandbTrainableMixin, PPOTrainer):
            pass

        config = {
            "env": "CartPole-v0",
            "wandb": {
                "project": "test_project",
                "api_key": "1234",
            },
        }

        # Test that trainer object can be initialized
        WandbPPOTrainer(config)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
