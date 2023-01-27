import os
import tempfile

from unittest.mock import (
    Mock,
    patch,
)

import numpy as np
import pytest

import ray

from ray.tune import Trainable
from ray.tune.integration.wandb import WandbTrainableMixin

from ray.tune.trainable import wrap_function
from ray.tune.integration.wandb import wandb_mixin
from ray.air.integrations.wandb import (
    WandbLoggerCallback,
    _QueueItem,
    _WandbLoggingActor,
)
from ray.air.integrations.wandb import (
    WANDB_ENV_VAR,
    WANDB_GROUP_ENV_VAR,
    WANDB_POPULATE_RUN_LOCATION_HOOK,
    WANDB_PROJECT_ENV_VAR,
    WANDB_SETUP_API_KEY_HOOK,
)
from ray.tune.result import TRIAL_INFO
from ray.tune.experiment.trial import _TrialInfo
from ray.tune.execution.placement_groups import PlacementGroupFactory

from ray.air.tests.mocked_wandb_integration import (
    _MockWandbAPI,
    _MockWandbLoggingActor,
    Trial,
    WandbTestExperimentLogger,
)


class _MockWandbTrainableMixin(WandbTrainableMixin):
    _wandb = _MockWandbAPI()


class WandbTestTrainable(_MockWandbTrainableMixin, Trainable):
    pass


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    ray.shutdown()


@pytest.fixture
def trial():
    trial_config = {"par1": 4, "par2": 9.12345678}
    trial = Trial(
        trial_config,
        0,
        "trial_0",
        "trainable",
        PlacementGroupFactory([{"CPU": 1}]),
        "/tmp",
    )
    yield trial


@pytest.fixture
def train_fn():
    @wandb_mixin
    def train_fn(config):
        return 1

    train_fn.__mixins__ = (_MockWandbTrainableMixin,)

    yield train_fn


@pytest.fixture(autouse=True)
def wandb_env():
    """Clean up W&B env var before and after each test.

    Even if we use monkeypatch in the test, this is useful to remove environment
    variables that are set on the laptop when running tests locally.
    """
    if WANDB_ENV_VAR in os.environ:
        del os.environ[WANDB_ENV_VAR]
    yield
    if WANDB_ENV_VAR in os.environ:
        del os.environ[WANDB_ENV_VAR]


class TestWandbLogger:
    def test_wandb_logger_project_group(self, monkeypatch):
        monkeypatch.setenv(WANDB_PROJECT_ENV_VAR, "test_project_from_env_var")
        monkeypatch.setenv(WANDB_GROUP_ENV_VAR, "test_group_from_env_var")
        # Read project and group name from environment variable
        logger = WandbTestExperimentLogger(api_key="1234")
        logger.setup()
        assert logger.project == "test_project_from_env_var"
        assert logger.group == "test_group_from_env_var"

    def test_wandb_logger_api_key_config(self, monkeypatch):
        # No API key
        with pytest.raises(ValueError):
            logger = WandbTestExperimentLogger(project="test_project")
            logger.setup()

        # Fetch API key from argument even if external hook and WANDB_ENV_VAR set
        monkeypatch.setenv(
            WANDB_SETUP_API_KEY_HOOK, "ray._private.test_utils.wandb_setup_api_key_hook"
        )
        monkeypatch.setenv(
            WANDB_ENV_VAR,
            "abcde",
        )
        # API Key in config
        logger = WandbTestExperimentLogger(project="test_project", api_key="1234")
        logger.setup()
        assert os.environ[WANDB_ENV_VAR] == "1234"

    def test_wandb_logger_api_key_file(self, monkeypatch):
        # Fetch API key from file even if external hook and WANDB_ENV_VAR set
        monkeypatch.setenv(
            WANDB_SETUP_API_KEY_HOOK, "ray._private.test_utils.wandb_setup_api_key_hook"
        )
        monkeypatch.setenv(
            WANDB_ENV_VAR,
            "abcde",
        )
        # API Key file
        with tempfile.NamedTemporaryFile("wt") as fp:
            fp.write("5678")
            fp.flush()

            logger = WandbTestExperimentLogger(
                project="test_project", api_key_file=fp.name
            )
            logger.setup()
            assert os.environ[WANDB_ENV_VAR] == "5678"

    def test_wandb_logger_api_key_env_var(self, monkeypatch):
        # API Key from env var takes precedence over external hook and
        # logged in W&B API key
        monkeypatch.setenv(
            WANDB_SETUP_API_KEY_HOOK, "ray._private.test_utils.wandb_setup_api_key_hook"
        )
        monkeypatch.setenv(
            WANDB_ENV_VAR,
            "1234",
        )
        mock_wandb = Mock(api=Mock(api_key="efgh"))
        with patch.multiple("ray.air.integrations.wandb", wandb=mock_wandb):
            logger = WandbTestExperimentLogger(project="test_project")
            logger.setup()
        assert os.environ[WANDB_ENV_VAR] == "1234"
        mock_wandb.ensure_configured.assert_not_called()

    def test_wandb_logger_api_key_external_hook(self, monkeypatch):
        # API Key from external hook if API key not provided through
        # argument or WANDB_ENV_VAR and user not already logged in to W&B
        monkeypatch.setenv(
            WANDB_SETUP_API_KEY_HOOK, "ray._private.test_utils.wandb_setup_api_key_hook"
        )

        mock_wandb = Mock(api=Mock(api_key=None))
        with patch.multiple("ray.air.integrations.wandb", wandb=mock_wandb):
            logger = WandbTestExperimentLogger(project="test_project")
            logger.setup()
        assert os.environ[WANDB_ENV_VAR] == "abcd"
        mock_wandb.ensure_configured.assert_called_once()

        mock_wandb = Mock(ensure_configured=Mock(side_effect=AttributeError()))
        with patch.multiple("ray.air.integrations.wandb", wandb=mock_wandb):
            logger = WandbTestExperimentLogger(project="test_project")
            logger.setup()
        assert os.environ[WANDB_ENV_VAR] == "abcd"

    def test_wandb_logger_api_key_from_wandb_login(self, monkeypatch):
        # No API key should get set if user is already logged in to W&B
        # and they didn't pass API key through argument or env var.
        # External hook should not be called because user already logged
        # in takes precedence.
        monkeypatch.setenv(
            WANDB_SETUP_API_KEY_HOOK, "ray._private.test_utils.wandb_setup_api_key_hook"
        )
        mock_wandb = Mock()
        with patch.multiple("ray.air.integrations.wandb", wandb=mock_wandb):
            logger = WandbTestExperimentLogger(project="test_project")
            logger.setup()
        assert os.environ.get(WANDB_ENV_VAR) is None
        mock_wandb.ensure_configured.assert_called_once()

    def test_wandb_logger_run_location_external_hook(self, monkeypatch):
        # No project
        with pytest.raises(ValueError):
            logger = WandbTestExperimentLogger(api_key="1234")
            logger.setup()

        # Project and group env vars from external hook
        monkeypatch.setenv(
            WANDB_POPULATE_RUN_LOCATION_HOOK,
            "ray._private.test_utils.wandb_populate_run_location_hook",
        )
        logger = WandbTestExperimentLogger(api_key="1234")
        logger.setup()
        assert os.environ[WANDB_PROJECT_ENV_VAR] == "test_project"
        assert os.environ[WANDB_GROUP_ENV_VAR] == "test_group"

    def test_wandb_logger_start(self, monkeypatch, trial):
        monkeypatch.setenv(WANDB_ENV_VAR, "9012")
        # API Key in env
        logger = WandbTestExperimentLogger(project="test_project")
        logger.setup()

        # From now on, the API key is in the env variable.
        logger = WandbTestExperimentLogger(project="test_project")
        logger.log_trial_start(trial)

        assert logger.trial_processes[trial].kwargs["project"] == "test_project"
        assert logger.trial_processes[trial].kwargs["id"] == trial.trial_id
        assert logger.trial_processes[trial].kwargs["name"] == trial.trial_name
        assert (
            logger.trial_processes[trial].kwargs["group"] == trial.experiment_dir_name
        )
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

    def test_wandb_logger_reporting(self, trial):
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
            "config": trial.config,
        }

        logger.on_trial_result(0, [], trial, r1)

        logged = logger.trial_processes[trial]._wandb.logs.get(timeout=10)
        assert "metric1" in logged
        assert "metric2" not in logged
        assert "metric3" in logged
        assert "metric4" in logged
        assert "const" not in logged
        assert "config" not in logged

    def test_wandb_logger_auto_config_keys(self, trial):
        logger = WandbTestExperimentLogger(project="test_project", api_key="1234")
        logger.on_trial_start(iteration=0, trials=[], trial=trial)
        config = logger.trial_processes[trial]._wandb.config.queue.get(timeout=10)

        result = {key: 0 for key in WandbLoggerCallback.AUTO_CONFIG_KEYS}
        logger.on_trial_result(0, [], trial, result)
        config_increment = logger.trial_processes[trial]._wandb.config.queue.get(
            timeout=10
        )
        config.update(config_increment)

        logger.on_trial_complete(0, [], trial)
        # The results in `AUTO_CONFIG_KEYS` should be saved as training configuration
        # instead of output metrics.
        assert set(WandbLoggerCallback.AUTO_CONFIG_KEYS) < set(config)

    def test_wandb_logger_exclude_config(self):
        trial = Trial(
            config={"param1": 0, "param2": 0},
            trial_id=0,
            trial_name="trial_0",
            experiment_dir_name="trainable",
            placement_group_factory=PlacementGroupFactory([{"CPU": 1}]),
            logdir=tempfile.gettempdir(),
        )
        logger = WandbTestExperimentLogger(
            project="test_project",
            api_key="1234",
            excludes=(["param2"] + WandbLoggerCallback.AUTO_CONFIG_KEYS),
        )
        logger.on_trial_start(iteration=0, trials=[], trial=trial)
        config = logger.trial_processes[trial]._wandb.config.queue.get(timeout=10)

        # We need to test that `excludes` also applies to `AUTO_CONFIG_KEYS`.
        result = {key: 0 for key in WandbLoggerCallback.AUTO_CONFIG_KEYS}
        logger.on_trial_result(0, [], trial, result)
        config_increment = logger.trial_processes[trial]._wandb.config.queue.get(
            timeout=10
        )
        config.update(config_increment)

        logger.on_trial_complete(0, [], trial)
        assert set(config) == {"param1"}

    def test_set_serializability_result(self, trial):
        """Tests that objects that contain sets can be serialized by wandb."""
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

    def test_wandb_logging_actor_api_key(self, ray_start_2_cpus, trial, monkeypatch):
        """Tests that the wandb API key get propagated as an environment variable to
        the remote logging actors."""

        def mock_run(actor_cls):
            return os.environ.get(WANDB_ENV_VAR)

        monkeypatch.setattr(
            WandbLoggerCallback, "_logger_actor_cls", _MockWandbLoggingActor
        )
        monkeypatch.setattr(_MockWandbLoggingActor, "run", mock_run)

        logger = WandbLoggerCallback(
            project="test_project", api_key="1234", excludes=["metric2"]
        )
        logger.setup()
        logger.log_trial_start(trial)
        actor_env_var = ray.get(logger._trial_logging_futures[trial])
        assert actor_env_var == "1234"


class TestWandbClassMixin:
    def test_wandb_mixin_config(self):
        # Needs at least a project
        config = {}
        with pytest.raises(ValueError):
            WandbTestTrainable(config)

        # No API key
        config = {"wandb": {"project": "test_project"}}
        with pytest.raises(ValueError):
            WandbTestTrainable(config)

        # API Key in config
        config = {"wandb": {"project": "test_project", "api_key": "1234"}}
        WandbTestTrainable(config)
        assert os.environ[WANDB_ENV_VAR] == "1234"

        del os.environ[WANDB_ENV_VAR]

    def test_wandb_mixin_api_key_file(self):
        # API Key file
        with tempfile.NamedTemporaryFile("wt") as fp:
            fp.write("5678")
            fp.flush()

            config = {"wandb": {"project": "test_project", "api_key_file": fp.name}}

            WandbTestTrainable(config)
            assert os.environ[WANDB_ENV_VAR] == "5678"

    def test_wandb_mixin_init(self, trial, monkeypatch):
        # API Key in env
        monkeypatch.setenv(WANDB_ENV_VAR, "9012")
        config = {"wandb": {"project": "test_project"}}
        trainable = WandbTestTrainable(config)

        # From now on, the API key is in the env variable.

        # Default configuration
        config = {"wandb": {"project": "test_project"}}
        trial_info = _TrialInfo(trial)
        config[TRIAL_INFO] = trial_info

        trainable = WandbTestTrainable(config)
        assert trainable.wandb.kwargs["project"] == "test_project"
        assert trainable.wandb.kwargs["id"] == trial.trial_id
        assert trainable.wandb.kwargs["name"] == trial.trial_name
        assert trainable.wandb.kwargs["group"] == "WandbTestTrainable"

    def test_wandb_mixin_rllib(self):
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


class TestWandbMixinDecorator:
    def test_wandb_decorator_config(self, train_fn):
        # Needs at least a project
        config = {}
        with pytest.raises(ValueError):
            wrap_function(train_fn)(config)

        # No API key
        config = {"wandb": {"project": "test_project"}}
        with pytest.raises(ValueError):
            wrap_function(train_fn)(config)

        # API Key in config
        config = {"wandb": {"project": "test_project", "api_key": "1234"}}
        wrap_function(train_fn)(config)
        assert os.environ[WANDB_ENV_VAR] == "1234"

    def test_wandb_decorator_api_key_file(self, train_fn):
        # API Key file
        with tempfile.NamedTemporaryFile("wt") as fp:
            fp.write("5678")
            fp.flush()

            config = {"wandb": {"project": "test_project", "api_key_file": fp.name}}

            wrap_function(train_fn)(config)
            assert os.environ[WANDB_ENV_VAR] == "5678"

    def test_wandb_decorator_init(self, trial, train_fn, monkeypatch):
        trial_info = _TrialInfo(trial)

        # API Key in env
        monkeypatch.setenv(WANDB_ENV_VAR, "9012")
        config = {"wandb": {"project": "test_project"}}
        wrapped = wrap_function(train_fn)(config)

        # From now on, the API key is in the env variable.

        # Default configuration
        config = {"wandb": {"project": "test_project"}}
        config[TRIAL_INFO] = trial_info

        wrapped = wrap_function(train_fn)(config)
        assert wrapped.wandb.kwargs["project"] == "test_project"
        assert wrapped.wandb.kwargs["id"] == trial.trial_id
        assert wrapped.wandb.kwargs["name"] == trial.trial_name


def test_wandb_logging_process_run_info_hook(monkeypatch):
    """
    Test WANDB_PROCESS_RUN_INFO_HOOK in _WandbLoggingActor is
    correctly called by calling _WandbLoggingActor.run() mocking
    out calls to wandb.
    """
    mock_queue = Mock(get=Mock(return_value=(_QueueItem.END, None)))
    monkeypatch.setenv(
        "WANDB_PROCESS_RUN_INFO_HOOK", "mock_wandb_process_run_info_hook"
    )

    with patch.object(ray.air.integrations.wandb, "_load_class") as mock_load_class:
        logging_process = _WandbLoggingActor(
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
