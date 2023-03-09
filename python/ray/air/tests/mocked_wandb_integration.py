from collections import namedtuple
from queue import Queue
import threading
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
            "logdir",
        ],
    )
):
    def __hash__(self):
        return hash(self.trial_id)

    def __str__(self):
        return self.trial_name


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


class _MockWandbLoggingActor(_WandbLoggingActor):
    def __init__(self, logdir, queue, exclude, to_config, *args, **kwargs):
        super(_MockWandbLoggingActor, self).__init__(
            logdir, queue, exclude, to_config, *args, **kwargs
        )
        self._wandb = _MockWandbAPI()

    def getattr(self, attr):
        # Helper for the test to get attributes from the actor
        return getattr(self, attr)


class WandbTestExperimentLogger(WandbLoggerCallback):
    """Wandb logger with mocked Wandb API gateway (one per trial)."""

    _logger_actor_cls = _MockWandbLoggingActor

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _cleanup_logging_actor(self, trial: "Trial"):
        # Unique for the mocked instance is the delayed delete of
        # `self._trial_logging_actors[trial]`.
        # This is because we want to access them in unit test after `.fit()`
        # to assert certain config and log is called with wandb.
        # Call `__del__` on the instance to kill/delete the actors
        del self._trial_queues[trial]
        del self._trial_logging_futures[trial]

    @property
    def trial_logging_actors(self):
        class PassThroughActor:
            def __init__(self, actor):
                self._actor = actor

            def __getattr__(self, attr):
                return ray.get(self._actor.getattr.remote(attr))

        return {
            trial: PassThroughActor(actor)
            for trial, actor in self._trial_logging_actors.items()
        }
