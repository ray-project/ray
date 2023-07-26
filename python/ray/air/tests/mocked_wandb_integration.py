from collections import namedtuple
from dataclasses import dataclass
from typing import Dict
from unittest.mock import Mock
from wandb.util import json_dumps_safer

import ray
from ray.air.integrations.wandb import (
    _WandbLoggingActor,
    WandbLoggerCallback,
)


class Trial(
    namedtuple(
        "MockTrial",
        [
            "config",
            "trial_id",
            "trial_name",
            "experiment_dir_name",
            "placement_group_factory",
            "local_path",
        ],
    )
):
    def __hash__(self):
        return hash(self.trial_id)

    def __str__(self):
        return self.trial_name


@dataclass
class LoggingActorState:
    args: list
    kwargs: dict
    exclude: list
    logs: list
    config: dict


class _FakeConfig:
    def __init__(self):
        self.config = {}

    def update(self, config, *args, **kwargs):
        self.config.update(config)


class _MockWandbAPI:
    """Thread-safe.

    Note: Not implemented to mock re-init behavior properly. Proceed with caution."""

    def __init__(self):
        self.logs = []
        self.config = _FakeConfig()

    def init(self, *args, **kwargs):
        mock = Mock()
        mock.args = args
        mock.kwargs = kwargs

        if "config" in kwargs:
            self.config.update(kwargs["config"])

        return mock

    def log(self, data):
        try:
            json_dumps_safer(data)
        except Exception:
            self.logs.append("serialization error")
        else:
            self.logs.append(data)

    def finish(self):
        pass

    def get_logs(self):
        return self.logs

    def get_config(self):
        return self.config.config


class _MockWandbLoggingActor(_WandbLoggingActor):
    _mock_wandb_api_cls = _MockWandbAPI

    def __init__(self, logdir, queue, exclude, to_config, *args, **kwargs):
        super(_MockWandbLoggingActor, self).__init__(
            logdir, queue, exclude, to_config, *args, **kwargs
        )
        self._wandb = self._mock_wandb_api_cls()

    def get_state(self):
        return LoggingActorState(
            args=self.args,
            kwargs=self.kwargs,
            exclude=self._exclude,
            logs=self._wandb.get_logs(),
            config=self._wandb.get_config(),
        )


class WandbTestExperimentLogger(WandbLoggerCallback):
    """Wandb logger with mocked Wandb API gateway (one per trial)."""

    _logger_actor_cls = _MockWandbLoggingActor

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._saved_actor_states: Dict["Trial", LoggingActorState] = {}

    def _cleanup_logging_actor(self, trial: "Trial", **kwargs):
        logging_actor_state: LoggingActorState = ray.get(
            self._trial_logging_actors[trial].get_state.remote()
        )
        self._saved_actor_states[trial] = logging_actor_state
        super()._cleanup_logging_actor(trial, **kwargs)

    @property
    def trial_logging_actor_states(self) -> Dict["Trial", LoggingActorState]:
        return self._saved_actor_states


def get_mock_wandb_logger(mock_api_cls=_MockWandbAPI, **kwargs):
    class MockWandbLoggingActor(_MockWandbLoggingActor):
        _mock_wandb_api_cls = mock_api_cls

    logger = WandbTestExperimentLogger(
        project="test_project",
        api_key="1234",
        **kwargs,
    )
    logger._logger_actor_cls = MockWandbLoggingActor
    return logger
