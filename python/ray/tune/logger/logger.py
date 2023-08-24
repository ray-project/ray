import abc
import json
import logging
import os
import pyarrow

from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Set, Type

import yaml
from ray.air._internal.json import SafeFallbackEncoder
from ray.train._internal.storage import _use_storage_context
from ray.tune.callback import Callback
from ray.util.annotations import Deprecated, DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray.tune.experiment.trial import Trial  # noqa: F401

logger = logging.getLogger(__name__)


# Apply flow style for sequences of this length
_SEQUENCE_LEN_FLOW_STYLE = 3

_LOGGER_DEPRECATION_WARNING = (
    "The `{old} interface is deprecated in favor of the "
    "`{new}` interface and will be removed in Ray 2.7."
)


@Deprecated(
    message=_LOGGER_DEPRECATION_WARNING.format(
        old="Logger", new="ray.tune.logger.LoggerCallback"
    ),
)
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

    def _restore_from_remote(self, file_name: str, trial: "Trial") -> None:
        if not _use_storage_context():
            return  # Legacy code path.

        if not trial.checkpoint:
            return

        local_file = os.path.join(trial.local_path, file_name)
        remote_file = os.path.join(trial.storage.trial_fs_path, file_name)

        try:
            pyarrow.fs.copy_files(
                remote_file,
                local_file,
                source_filesystem=trial.storage.storage_filesystem,
            )
            logger.debug(f"Copied {remote_file} to {local_file}")
        except FileNotFoundError:
            logger.warning(f"Remote file not found: {remote_file}")
        except Exception:
            logger.exception(f"Error downloading {remote_file}")


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
        trial.init_local_path()

        for logger_class in self.logger_classes:
            trial_loggers = self._class_trial_loggers.get(logger_class, {})
            if trial not in trial_loggers:
                logger = logger_class(trial.config, trial.local_path, trial)
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


class _RayDumper(yaml.SafeDumper):
    def represent_sequence(self, tag, sequence, flow_style=None):
        if len(sequence) > _SEQUENCE_LEN_FLOW_STYLE:
            return super().represent_sequence(tag, sequence, flow_style=True)
        return super().represent_sequence(tag, sequence, flow_style=flow_style)


@DeveloperAPI
def pretty_print(result, exclude: Optional[Set[str]] = None):
    result = result.copy()
    result.update(config=None)  # drop config from pretty print
    result.update(hist_stats=None)  # drop hist_stats from pretty print
    out = {}
    for k, v in result.items():
        if v is not None and (exclude is None or k not in exclude):
            out[k] = v

    cleaned = json.dumps(out, cls=SafeFallbackEncoder)
    return yaml.dump(json.loads(cleaned), Dumper=_RayDumper, default_flow_style=False)
