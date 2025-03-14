"""Tests for wandb integration.

Note: These tests use a set of mocked APIs:
- _MockWandbAPI: Mocks wandb API calls (ex: wandb.init).
- _MockWandbLoggingActor: The same as the regular _WandbLoggingActor,
    except using the mocked wandb API
- WandbTestExperimentLogger: Thin subclass of `WandbLoggerCallback` to use for testing.
    Provides a helper `trial_logging_actors` property that can be used to
    access attributes of the remote actors for assertions.
- Use the `get_mock_wandb_logger` helper method to create a logger with
    a custom mock wandb API class. (Ex: If you want to override some wandb API methods.)

Template for testing with these mocks:

    wandb_logger_kwargs = {}
    logger = get_mock_wandb_logger(mock_api_cls=_MockWandbAPI, **wandb_logger_kwargs)
    logger.setup()

    # From now on, the API key is in the env variable.
    # Start the remote logging actor
    logger.on_trial_start(0, [], trial)
    # Log some results
    result = {}
    logger.on_trial_result(0, [], trial, result)
    # Send a STOP signal to the logging actor
    logger.on_trial_complete(0, [], trial)
    # This will wait for the logging actor to finish + cleanup
    logger.on_experiment_end(trials=[trial])

    # Now, we can access properties of the logging actors
    # (must happen after `on_trial_end` and `on_experiment_end`)
    logger_state = logger.trial_logging_actor_states[trial]
    # logger_state.logs, logger_state.config, logger_state.kwargs, ...
"""

import gc
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import pytest

import ray
from ray.air.integrations.wandb import (
    WANDB_ENV_VAR,
    WANDB_GROUP_ENV_VAR,
    WANDB_POPULATE_RUN_LOCATION_HOOK,
    WANDB_PROJECT_ENV_VAR,
    WANDB_SETUP_API_KEY_HOOK,
    WandbLoggerCallback,
    _QueueItem,
    _WandbLoggingActor,
)
from ray.air.tests.mocked_wandb_integration import (
    Trial,
    WandbTestExperimentLogger,
    _MockWandbAPI,
    _MockWandbLoggingActor,
    get_mock_wandb_logger,
)
from ray.exceptions import RayActorError
from ray.tune.execution.placement_groups import PlacementGroupFactory


@pytest.fixture(autouse=True, scope="module")
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


def fake_wandb_populate_run_location_hook():
    """Fake user-provided hook to populate W&B environment variables."""
    os.environ[WANDB_PROJECT_ENV_VAR] = "test_project"
    os.environ[WANDB_GROUP_ENV_VAR] = "test_group"


FAKE_WANDB_POPULATE_RUN_LOCATION_HOOK_IMPORT_PATH = (
    "ray.air.tests.test_integration_wandb.fake_wandb_populate_run_location_hook"
)


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
        with patch.dict(os.environ):
            # No project
            with pytest.raises(ValueError):
                logger = WandbTestExperimentLogger(api_key="1234")
                logger.setup()

            # Project and group env vars from external hook
            monkeypatch.setenv(
                WANDB_POPULATE_RUN_LOCATION_HOOK,
                FAKE_WANDB_POPULATE_RUN_LOCATION_HOOK_IMPORT_PATH,
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
        logger.log_trial_start(trial)
        logger.log_trial_end(trial)
        logger.on_experiment_end(trials=[trial])

        logger_state = logger.trial_logging_actor_states[trial]
        assert logger_state.kwargs["project"] == "test_project"
        assert logger_state.kwargs["id"] == trial.trial_id
        assert logger_state.kwargs["name"] == trial.trial_name
        assert logger_state.kwargs["group"] == trial.experiment_dir_name
        assert "config" in logger_state.exclude

        del logger

        # log config.
        logger = WandbTestExperimentLogger(project="test_project", log_config=True)
        logger.log_trial_start(trial)
        logger.log_trial_end(trial)
        logger.on_experiment_end(trials=[trial])

        logger_state = logger.trial_logging_actor_states[trial]
        assert "config" not in logger_state.exclude
        assert "metric" not in logger_state.exclude

        del logger

        # Exclude metric.
        logger = WandbTestExperimentLogger(project="test_project", excludes=["metric"])
        logger.log_trial_start(trial)
        logger.log_trial_end(trial)
        logger.on_experiment_end(trials=[trial])

        logger_state = logger.trial_logging_actor_states[trial]
        assert "config" in logger_state.exclude
        assert "metric" in logger_state.exclude

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
        logger.on_trial_complete(0, [], trial)
        logger.on_experiment_end(trials=[trial])
        logged = logger.trial_logging_actor_states[trial].logs[0]
        assert "metric1" in logged
        assert "metric2" not in logged
        assert "metric3" in logged
        assert "metric4" in logged
        assert "const" not in logged
        assert "config" not in logged

    def test_wandb_logger_auto_config_keys(self, trial):
        logger = WandbTestExperimentLogger(project="test_project", api_key="1234")
        logger.on_trial_start(iteration=0, trials=[], trial=trial)
        result = {key: 0 for key in WandbLoggerCallback.AUTO_CONFIG_KEYS}
        logger.on_trial_result(0, [], trial, result)
        logger.on_trial_complete(0, [], trial)
        logger.on_experiment_end(trials=[trial])
        config = logger.trial_logging_actor_states[trial].config
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
            local_path=tempfile.gettempdir(),
        )
        logger = WandbTestExperimentLogger(
            project="test_project",
            api_key="1234",
            excludes=(["param2"] + WandbLoggerCallback.AUTO_CONFIG_KEYS),
        )
        logger.on_trial_start(iteration=0, trials=[], trial=trial)

        # We need to test that `excludes` also applies to `AUTO_CONFIG_KEYS`.
        result = {key: 0 for key in WandbLoggerCallback.AUTO_CONFIG_KEYS}
        logger.on_trial_result(0, [], trial, result)
        logger.on_trial_complete(0, [], trial)
        logger.on_experiment_end(trials=[trial])

        config = logger.trial_logging_actor_states[trial].config
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
            "num_envs_per_env_runner": 1,
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
        logger.on_trial_complete(0, [], trial)
        logger.on_experiment_end(trials=[trial])
        logged = logger.trial_logging_actor_states[trial].logs[0]
        assert logged != "serialization error"

    def test_wandb_logging_actor_api_key(self, trial, monkeypatch):
        """Tests that the wandb API key get propagated as an environment variable to
        the remote logging actors."""

        def mock_run(actor_cls):
            return os.environ.get(WANDB_ENV_VAR)

        monkeypatch.setattr(_MockWandbLoggingActor, "run", mock_run)

        logger = WandbLoggerCallback(
            project="test_project", api_key="1234", excludes=["metric2"]
        )
        logger._logger_actor_cls = _MockWandbLoggingActor
        logger.setup()
        logger.log_trial_start(trial)
        actor_env_var = ray.get(logger._trial_logging_futures[trial])
        assert actor_env_var == "1234"

    def test_wandb_finish(self, trial, tmp_path):
        """Test that logging actors are cleaned up upon experiment completion."""
        marker = tmp_path / "hang_marker"
        marker.write_text("")

        class HangingFinishMockWandbAPI(_MockWandbAPI):
            def finish(self):
                while marker.exists():
                    time.sleep(0.1)

        logger = get_mock_wandb_logger(
            mock_api_cls=HangingFinishMockWandbAPI,
            upload_timeout=1.0,
        )
        logger.setup()
        logger.on_trial_start(0, [], trial)
        logger.on_trial_complete(0, [], trial)
        # Signalling stop will not cleanup fully due to the hanging finish
        assert logger._trial_logging_actors
        marker.unlink()
        # wandb.finish has ended -> experiment end hook should cleanup actors fully
        logger.on_experiment_end(trials=[trial])
        assert not logger._trial_logging_actors

    def test_wandb_kill_hanging_actor(self, trial):
        """Test that logging actors are killed if exceeding the upload timeout
        upon experiment completion."""

        class HangingFinishMockWandbAPI(_MockWandbAPI):
            def finish(self):
                time.sleep(5)

        logger = get_mock_wandb_logger(
            mock_api_cls=HangingFinishMockWandbAPI,
            upload_timeout=0.1,
        )
        logger.setup()
        logger.on_trial_start(0, [], trial)
        logger.on_trial_complete(0, [], trial)
        # Signalling stop will not cleanup fully due to the hanging finish
        assert logger._trial_logging_actors
        actor = logger._trial_logging_actors[trial]
        # Experiment end hook should kill actors since upload_timeout < 5
        logger.on_experiment_end(trials=[trial])
        assert not logger._trial_logging_actors
        gc.collect()
        with pytest.raises(RayActorError):
            ray.get(actor.get_state.remote())

    def test_wandb_destructor(self, trial):
        """Test that the WandbLoggerCallback destructor forcefully cleans up
        logging actors."""

        class SlowFinishMockWandbAPI(_MockWandbAPI):
            def finish(self):
                time.sleep(5)

        logger = get_mock_wandb_logger(
            mock_api_cls=SlowFinishMockWandbAPI,
            upload_timeout=1.0,
        )

        logger.setup()
        # Triggers logging actor run loop
        logger.on_trial_start(0, [], trial)
        actor = logger._trial_logging_actors[trial]
        del logger
        gc.collect()
        with pytest.raises(RayActorError):
            ray.get(actor.get_state.remote())

    def test_wandb_logging_actor_fault_tolerance(self, trial):
        """Tests that failing wandb logging actors are restarted"""

        with tempfile.TemporaryDirectory() as tempdir:
            fail_marker = Path(tempdir) / "fail_marker"

            class _FailingWandbLoggingActor(_MockWandbLoggingActor):
                def _handle_result(self, result):
                    if (
                        result.get("training_iteration") == 3
                        and not fail_marker.exists()
                    ):
                        fail_marker.write_text("Ok")
                        raise SystemExit

                    return super()._handle_result(result)

            logger = WandbLoggerCallback(
                project="test_project", api_key="1234", excludes=["metric2"]
            )
            logger._logger_actor_cls = _FailingWandbLoggingActor
            logger.setup()
            logger.log_trial_start(trial)

            actor = logger._trial_logging_actors[trial]
            queue = logger._trial_queues[trial]

            logger.log_trial_result(1, trial, result={"training_iteration": 1})
            logger.log_trial_result(2, trial, result={"training_iteration": 2})
            logger.log_trial_result(3, trial, result={"training_iteration": 3})

            logger.log_trial_result(4, trial, result={"training_iteration": 4})
            logger.log_trial_result(5, trial, result={"training_iteration": 5})

            queue.put(_QueueItem.END)

            state = ray.get(actor.get_state.remote())
            assert [metrics["training_iteration"] for metrics in state.logs] == [4, 5]

    def test_wandb_restart(self, trial):
        """Test that the WandbLoggerCallback reuses actors for trial restarts."""

        logger = WandbLoggerCallback(project="test_project", api_key="1234")
        logger._logger_actor_cls = _MockWandbLoggingActor
        logger.setup()

        assert len(logger._trial_logging_futures) == 0
        assert len(logger._logging_future_to_trial) == 0

        logger.log_trial_start(trial)

        assert len(logger._trial_logging_futures) == 1
        assert len(logger._logging_future_to_trial) == 1

        logger.log_trial_start(trial)

        assert len(logger._trial_logging_futures) == 1
        assert len(logger._logging_future_to_trial) == 1


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

    with patch.object(ray.air.integrations.wandb, "load_class") as mock_load_class:
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
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
