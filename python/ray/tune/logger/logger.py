import abc
import json
import logging

from typing import TYPE_CHECKING, Dict, List, Optional, Type, Iterable

import yaml
from ray.tune.callback import Callback
from ray.util.annotations import PublicAPI, DeveloperAPI
from ray.util.ml_utils.json import SafeFallbackEncoder

if TYPE_CHECKING:
    from ray.tune.experiment.trial import Trial  # noqa: F401

logger = logging.getLogger(__name__)


@DeveloperAPI
class Logger(abc.ABC):
    """Logging interface for ray.tune.

    By default, the UnifiedLogger implementation is used which logs results in
    multiple formats (TensorBoard, rllab/viskit, plain json, custom loggers)
    at once.

    Arguments:
        config: Configuration passed to all logger creators.
        logdir: Directory for all logger creators to log to.
        trial: Trial object for the logger to access.
    """

    def __init__(self, config: Dict, logdir: str, trial: Optional["Trial"] = None):
        self.config = config
        self.logdir = logdir
        self.trial = trial
        self._init()

    def _init(self):
        pass

    def on_result(self, result):
        """Given a result, appends it to the existing log."""

        raise NotImplementedError

    def update_config(self, config):
        """Updates the config for logger."""

        pass

    def close(self):
        """Releases all resources used by this logger."""

        pass

    def flush(self):
        """Flushes all disk writes to storage."""

        pass


@PublicAPI
class LoggerCallback(Callback):
    """Base class for experiment-level logger callbacks

    This base class defines a general interface for logging events,
    like trial starts, restores, ends, checkpoint saves, and receiving
    trial results.

    Callbacks implementing this interface should make sure that logging
    utilities are cleaned up properly on trial termination, i.e. when
    ``log_trial_end`` is received. This includes e.g. closing files.
    """

    def log_trial_start(self, trial: "Trial"):
        """Handle logging when a trial starts.

        Args:
            trial: Trial object.
        """
        pass

    def log_trial_restore(self, trial: "Trial"):
        """Handle logging when a trial restores.

        Args:
            trial: Trial object.
        """
        pass

    def log_trial_save(self, trial: "Trial"):
        """Handle logging when a trial saves a checkpoint.

        Args:
            trial: Trial object.
        """
        pass

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        """Handle logging when a trial reports a result.

        Args:
            trial: Trial object.
            result: Result dictionary.
        """
        pass

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        """Handle logging when a trial ends.

        Args:
            trial: Trial object.
            failed: True if the Trial finished gracefully, False if
                it failed (e.g. when it raised an exception).
        """
        pass

    def on_trial_result(
        self,
        iteration: int,
        trials: List["Trial"],
        trial: "Trial",
        result: Dict,
        **info,
    ):
        self.log_trial_result(iteration, trial, result)

    def on_trial_start(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self.log_trial_start(trial)

    def on_trial_restore(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self.log_trial_restore(trial)

    def on_trial_save(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self.log_trial_save(trial)

    def on_trial_complete(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self.log_trial_end(trial, failed=False)

    def on_trial_error(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self.log_trial_end(trial, failed=True)


@DeveloperAPI
class LegacyLoggerCallback(LoggerCallback):
    """Supports logging to trial-specific `Logger` classes.

    Previously, Ray Tune logging was handled via `Logger` classes that have
    been instantiated per-trial. This callback is a fallback to these
    `Logger`-classes, instantiating each `Logger` class for each trial
    and logging to them.

    Args:
        logger_classes: Logger classes that should
            be instantiated for each trial.

    """

    def __init__(self, logger_classes: Iterable[Type[Logger]]):
        self.logger_classes = list(logger_classes)
        self._class_trial_loggers: Dict[Type[Logger], Dict["Trial", Logger]] = {}

    def log_trial_start(self, trial: "Trial"):
        trial.init_logdir()

        for logger_class in self.logger_classes:
            trial_loggers = self._class_trial_loggers.get(logger_class, {})
            if trial not in trial_loggers:
                logger = logger_class(trial.config, trial.logdir, trial)
                trial_loggers[trial] = logger
            self._class_trial_loggers[logger_class] = trial_loggers

    def log_trial_restore(self, trial: "Trial"):
        for logger_class, trial_loggers in self._class_trial_loggers.items():
            if trial in trial_loggers:
                trial_loggers[trial].flush()

    def log_trial_save(self, trial: "Trial"):
        for logger_class, trial_loggers in self._class_trial_loggers.items():
            if trial in trial_loggers:
                trial_loggers[trial].flush()

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        for logger_class, trial_loggers in self._class_trial_loggers.items():
            if trial in trial_loggers:
                trial_loggers[trial].on_result(result)

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        for logger_class, trial_loggers in self._class_trial_loggers.items():
            if trial in trial_loggers:
                trial_loggers[trial].close()


def pretty_print(result):
    result = result.copy()
    result.update(config=None)  # drop config from pretty print
    result.update(hist_stats=None)  # drop hist_stats from pretty print
    out = {}
    for k, v in result.items():
        if v is not None:
            out[k] = v

    cleaned = json.dumps(out, cls=SafeFallbackEncoder)
    return yaml.safe_dump(json.loads(cleaned), default_flow_style=False)
