from collections import namedtuple
from queue import Queue
import threading
from unittest.mock import Mock
from wandb.util import json_dumps_safer

from ray.air.integrations.wandb import (
    _WandbLoggingActor,
    _QueueItem,
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
    """Thread-safe."""

    def __init__(self):
        self.queue = Queue()

    # This is called during both on_trial_start and on_trial_result.
    def update(self, config, *args, **kwargs):
        self.queue.put(config)


class _MockWandbAPI:
    """Thread-safe.

    Note: Not implemented to mock re-init behavior properly. Proceed with caution."""

    def __init__(self):
        self.logs = Queue()
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
            self.logs.put("serialization error")
        else:
            self.logs.put(data)

    def finish(self):
        pass


class _MockWandbLoggingActor(_WandbLoggingActor):
    def __init__(self, logdir, queue, exclude, to_config, *args, **kwargs):
        super(_MockWandbLoggingActor, self).__init__(
            logdir, queue, exclude, to_config, *args, **kwargs
        )
        self._wandb = _MockWandbAPI()


class WandbTestExperimentLogger(WandbLoggerCallback):

    """Wandb logger with mocked Wandb API gateway (one per trial)."""

    @property
    def trial_processes(self):
        return self._trial_logging_actors

    def _start_logging_actor(self, trial, exclude_results, **wandb_init_kwargs):
        self._trial_queues[trial] = Queue()
        local_actor = _MockWandbLoggingActor(
            logdir=trial.logdir,
            queue=self._trial_queues[trial],
            exclude=exclude_results,
            to_config=self.AUTO_CONFIG_KEYS,
            **wandb_init_kwargs,
        )
        self._trial_logging_actors[trial] = local_actor

        thread = threading.Thread(target=local_actor.run)
        self._trial_logging_futures[trial] = thread
        thread.start()

    def _stop_logging_actor(self, trial: "Trial", timeout: int = 10):
        # Unique for the mocked instance is the delayed delete of
        # `self._trial_logging_actors`.
        # This is because we want to access them in unit test after `.fit()`
        # to assert certain config and log is called with wandb.
        if trial in self._trial_queues:
            self._trial_queues[trial].put((_QueueItem.END, None))
            del self._trial_queues[trial]
        if trial in self._trial_logging_futures:
            self._trial_logging_futures[trial].join(timeout=2)
            del self._trial_logging_futures[trial]
