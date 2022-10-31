import os
import tempfile
from collections import namedtuple
from dataclasses import dataclass
from multiprocessing import Queue
from typing import Tuple, Dict
from unittest.mock import (
    Mock,
    patch,
)

import numpy as np
import pytest

import ray
from ray.tune import Trainable
from ray.tune.trainable import wrap_function
from ray.tune.integration.wandb import (
    WandbTrainableMixin,
    wandb_mixin,
)
from ray.air.callbacks.wandb import (
    WandbLoggerCallback,
    _WandbLoggingProcess,
    WANDB_ENV_VAR,
    WANDB_GROUP_ENV_VAR,
    WANDB_PROJECT_ENV_VAR,
    WANDB_SETUP_API_KEY_HOOK,
    _QueueItem,
)
from ray.tune.result import TRIAL_INFO
from ray.tune.experiment.trial import _TrialInfo
from ray.tune.execution.placement_groups import PlacementGroupFactory
from wandb.util import json_dumps_safer


class Trial(
    namedtuple(
        "MockTrial",
        [
            "config",
            "trial_id",
            "trial_name",
            "trainable_name",
            "placement_group_factory",
            "logdir",
        ],
    )
):
    def __hash__(self):
        return hash(self.trial_id)

    def __str__(self):
        return self.trial_name


@dataclass
class _MockWandbConfig:
    args: Tuple
    kwargs: Dict


class _MockWandbAPI:
    def __init__(self):
        self.logs = Queue()

    def init(self, *args, **kwargs):
        mock = Mock()
        mock.args = args
        mock.kwargs = kwargs
        return mock

    def log(self, data):
        try:
            json_dumps_safer(data)
        except Exception:
            self.logs.put("serialization error")
        else:
            self.logs.put(data)

    def finish(self):
        pass

    @property
    def config(self):
        return Mock()


class _MockWandbLoggingProcess(_WandbLoggingProcess):
    def __init__(self, logdir, queue, exclude, to_config, *args, **kwargs):
        super(_MockWandbLoggingProcess, self).__init__(
            logdir, queue, exclude, to_config, *args, **kwargs
        )
        self._wandb = _MockWandbAPI()


class WandbTestExperimentLogger(WandbLoggerCallback):
    _logger_process_cls = _MockWandbLoggingProcess

    @property
    def trial_processes(self):
        return self._trial_processes


class _MockWandbTrainableMixin(WandbTrainableMixin):
    _wandb = _MockWandbAPI()


class WandbTestTrainable(_MockWandbTrainableMixin, Trainable):
    pass


@pytest.fixture
def wandb_env():
    if WANDB_ENV_VAR in os.environ:
        del os.environ[WANDB_ENV_VAR]
    yield
    if WANDB_ENV_VAR in os.environ:
        del os.environ[WANDB_ENV_VAR]


def test_wandb_logger_config(wandb_env):
    trial_config = {"par1": 4, "par2": 9.12345678}
    trial = Trial(
        trial_config,
        0,
        "trial_0",
        "trainable",
        PlacementGroupFactory([{"CPU": 1}]),
        "/tmp",
    )

    # Read project and group name from environment variable
    os.environ[WANDB_PROJECT_ENV_VAR] = "test_project_from_env_var"
    os.environ[WANDB_GROUP_ENV_VAR] = "test_group_from_env_var"
    logger = WandbTestExperimentLogger(api_key="1234")
    logger.setup()
    assert logger.project == "test_project_from_env_var"
    assert logger.group == "test_group_from_env_var"

    del logger
    del os.environ[WANDB_ENV_VAR]

    # No API key
    with pytest.raises(ValueError):
        logger = WandbTestExperimentLogger(project="test_project")
        logger.setup()

    # API Key in config
    logger = WandbTestExperimentLogger(project="test_project", api_key="1234")
    logger.setup()
    assert os.environ[WANDB_ENV_VAR] == "1234"

    del logger
    del os.environ[WANDB_ENV_VAR]

    # API Key file
    with tempfile.NamedTemporaryFile("wt") as fp:
        fp.write("5678")
        fp.flush()

        logger = WandbTestExperimentLogger(project="test_project", api_key_file=fp.name)
        logger.setup()
        assert os.environ[WANDB_ENV_VAR] == "5678"

    del logger
    del os.environ[WANDB_ENV_VAR]

    # API Key from external hook
    os.environ[
        WANDB_SETUP_API_KEY_HOOK
    ] = "ray._private.test_utils.wandb_setup_api_key_hook"
    logger = WandbTestExperimentLogger(project="test_project")
    logger.setup()
    assert os.environ[WANDB_ENV_VAR] == "abcd"

    del logger
    del os.environ[WANDB_ENV_VAR]
    del os.environ[WANDB_SETUP_API_KEY_HOOK]

    # API Key in env
    os.environ[WANDB_ENV_VAR] = "9012"
    logger = WandbTestExperimentLogger(project="test_project")
    logger.setup()
    del logger

    # From now on, the API key is in the env variable.

    logger = WandbTestExperimentLogger(project="test_project")
    logger.log_trial_start(trial)

    assert logger.trial_processes[trial].kwargs["project"] == "test_project"
    assert logger.trial_processes[trial].kwargs["id"] == trial.trial_id
    assert logger.trial_processes[trial].kwargs["name"] == trial.trial_name
    assert logger.trial_processes[trial].kwargs["group"] == trial.trainable_name
    assert "config" in logger.trial_processes[trial]._exclude

    del logger

    # log config.
    logger = WandbTestExperimentLogger(project="test_project", log_config=True)
    logger.log_trial_start(trial)
    assert "config" not in logger.trial_processes[trial]._exclude
    assert "metric" not in logger.trial_processes[trial]._exclude

    del logger

    # Exclude metric.
    logger = WandbTestExperimentLogger(project="test_project", excludes=["metric"])
    logger.log_trial_start(trial)
    assert "config" in logger.trial_processes[trial]._exclude
    assert "metric" in logger.trial_processes[trial]._exclude

    del logger


def test_wandb_logger_reporting(wandb_env):
    trial_config = {"par1": 4, "par2": 9.12345678}
    trial = Trial(
        trial_config,
        0,
        "trial_0",
        "trainable",
        PlacementGroupFactory([{"CPU": 1}]),
        "/tmp",
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

    logged = logger.trial_processes[trial]._wandb.logs.get(timeout=10)
    assert "metric1" in logged
    assert "metric2" not in logged
    assert "metric3" in logged
    assert "metric4" in logged
    assert "const" not in logged
    assert "config" not in logged

    del logger


def test_set_serializability_result(wandb_env):
    """Tests that objects that contain sets can be serialized by wandb."""
    trial_config = {"par1": 4, "par2": 9.12345678}
    trial = Trial(
        trial_config,
        0,
        "trial_0",
        "trainable",
        PlacementGroupFactory([{"CPU": 1}]),
        "/tmp",
    )

    logger = WandbTestExperimentLogger(
        project="test_project", api_key="1234", excludes=["metric2"]
    )
    logger.on_trial_start(0, [], trial)

    # Testing for https://github.com/ray-project/ray/issues/28541
    rllib_result = {
        "env": "simple_spread",
        "framework": "torch",
        "num_gpus": 1,
        "num_workers": 20,
        "num_envs_per_worker": 1,
        "compress_observations": True,
        "lambda": 0.99,
        "train_batch_size": 512,
        "sgd_minibatch_size": 32,
        "num_sgd_iter": 5,
        "batch_mode": "truncate_episodes",
        "entropy_coeff": 0.01,
        "lr": 2e-05,
        "multiagent": {
            "policies": {"shared_policy"},
            "policy_mapping_fn": lambda x: x,
        },
    }
    logger.on_trial_result(0, [], trial, rllib_result)
    logged = logger.trial_processes[trial]._wandb.logs.get(timeout=10)
    assert logged != "serialization error"

    del logger


def test_wandb_mixin_config(wandb_env):
    config = {"par1": 4, "par2": 9.12345678}
    trial = Trial(
        config,
        0,
        "trial_0",
        "trainable",
        PlacementGroupFactory([{"CPU": 1}]),
        "/tmp",
    )
    trial_info = _TrialInfo(trial)

    config[TRIAL_INFO] = trial_info

    if WANDB_ENV_VAR in os.environ:
        del os.environ[WANDB_ENV_VAR]

    # Needs at least a project
    with pytest.raises(ValueError):
        trainable = WandbTestTrainable(config)

    # No API key
    config["wandb"] = {"project": "test_project"}
    with pytest.raises(ValueError):
        trainable = WandbTestTrainable(config)

    # API Key in config
    config["wandb"] = {"project": "test_project", "api_key": "1234"}
    trainable = WandbTestTrainable(config)
    assert os.environ[WANDB_ENV_VAR] == "1234"

    del os.environ[WANDB_ENV_VAR]

    # API Key file
    with tempfile.NamedTemporaryFile("wt") as fp:
        fp.write("5678")
        fp.flush()

        config["wandb"] = {"project": "test_project", "api_key_file": fp.name}

        trainable = WandbTestTrainable(config)
        assert os.environ[WANDB_ENV_VAR] == "5678"

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
    assert trainable.run.kwargs["project"] == "test_project"
    assert trainable.run.kwargs["id"] == trial.trial_id
    assert trainable.run.kwargs["name"] == trial.trial_name
    assert trainable.run.kwargs["group"] == "WandbTestTrainable"


def test_wandb_decorator_config(wandb_env):
    config = {"par1": 4, "par2": 9.12345678}
    trial = Trial(
        config,
        0,
        "trial_0",
        "trainable",
        PlacementGroupFactory([{"CPU": 1}]),
        "/tmp",
    )
    trial_info = _TrialInfo(trial)

    @wandb_mixin
    def train_fn(config):
        return 1

    train_fn.__mixins__ = (_MockWandbTrainableMixin,)

    config[TRIAL_INFO] = trial_info

    if WANDB_ENV_VAR in os.environ:
        del os.environ[WANDB_ENV_VAR]

    # Needs at least a project
    with pytest.raises(ValueError):
        wrapped = wrap_function(train_fn)(config)

    # No API key
    config["wandb"] = {"project": "test_project"}
    with pytest.raises(ValueError):
        wrapped = wrap_function(train_fn)(config)

    # API Key in config
    config["wandb"] = {"project": "test_project", "api_key": "1234"}
    wrapped = wrap_function(train_fn)(config)
    assert os.environ[WANDB_ENV_VAR] == "1234"

    del os.environ[WANDB_ENV_VAR]

    # API Key file
    with tempfile.NamedTemporaryFile("wt") as fp:
        fp.write("5678")
        fp.flush()

        config["wandb"] = {"project": "test_project", "api_key_file": fp.name}

        wrapped = wrap_function(train_fn)(config)
        assert os.environ[WANDB_ENV_VAR] == "5678"

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
    assert wrapped.run.kwargs["project"] == "test_project"
    assert wrapped.run.kwargs["id"] == trial.trial_id
    assert wrapped.run.kwargs["name"] == trial.trial_name


def test_wandb_mixin_rllib(wandb_env):
    """Test compatibility with RLlib configuration dicts"""
    # Local import to avoid tune dependency on rllib
    try:
        from ray.rllib.algorithms.ppo import PPO
    except ImportError:
        pytest.skip("ray[rllib] not available")

    class WandbPPOTrainer(_MockWandbTrainableMixin, PPO):
        pass

    config = {
        "env": "CartPole-v0",
        "wandb": {"project": "test_project", "api_key": "1234"},
    }

    # Test that trainer object can be initialized
    WandbPPOTrainer(config)


def test_wandb_logging_process_run_info_hook(wandb_env, monkeypatch):
    """
    Test WANDB_PROCESS_RUN_INFO_HOOK in _WandbLoggingProcess is
    correctly called by calling _WandbLoggingProcess.run() mocking
    out calls to wandb.
    """
    mock_queue = Mock(get=Mock(return_value=(_QueueItem.END, None)))
    monkeypatch.setenv(
        "WANDB_PROCESS_RUN_INFO_HOOK", "mock_wandb_process_run_info_hook"
    )

    with patch.object(ray.air.callbacks.wandb, "_load_class") as mock_load_class:
        logging_process = _WandbLoggingProcess(
            logdir="/tmp", queue=mock_queue, exclude=[], to_config=[]
        )
        logging_process._wandb = Mock()
        logging_process.run()

    logging_process._wandb.init.assert_called_once()
    run = logging_process._wandb.init.return_value
    mock_load_class.assert_called_once_with("mock_wandb_process_run_info_hook")
    external_hook = mock_load_class.return_value
    external_hook.assert_called_once_with(run)
    logging_process._wandb.finish.assert_called_once()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
