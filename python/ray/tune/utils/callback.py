import importlib
import logging
import os
from typing import TYPE_CHECKING, Collection, List, Optional, Type, Union

from ray.tune.callback import Callback, CallbackList
from ray.tune.constants import RAY_TUNE_CALLBACKS_ENV_VAR
from ray.tune.logger import (
    CSVLogger,
    CSVLoggerCallback,
    JsonLogger,
    JsonLoggerCallback,
    LegacyLoggerCallback,
    TBXLogger,
    TBXLoggerCallback,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ray.tune.experimental.output import AirVerbosity

DEFAULT_CALLBACK_CLASSES = (
    CSVLoggerCallback,
    JsonLoggerCallback,
    TBXLoggerCallback,
)


def _get_artifact_templates_for_callbacks(
    callbacks: Union[List[Callback], List[Type[Callback]], CallbackList]
) -> List[str]:
    templates = []
    for callback in callbacks:
        templates += list(callback._SAVED_FILE_TEMPLATES)
    return templates


def _create_default_callbacks(
    callbacks: Optional[List[Callback]],
    *,
    air_verbosity: Optional["AirVerbosity"] = None,
    entrypoint: Optional[str] = None,
    metric: Optional[str] = None,
    mode: Optional[str] = None,
    config: Optional[dict] = None,
    progress_metrics: Optional[Collection[str]] = None,
) -> List[Callback]:
    """Create default callbacks for `Tuner.fit()`.

    This function takes a list of existing callbacks and adds default
    callbacks to it.

    Specifically, three kinds of callbacks will be added:

    1. Loggers. Ray Tune's experiment analysis relies on CSV and JSON logging.
    2. Syncer. Ray Tune synchronizes logs and checkpoint between workers and
       the head node.
    2. Trial progress reporter. For reporting intermediate progress, like trial
       results, Ray Tune uses a callback.

    These callbacks will only be added if they don't already exist, i.e. if
    they haven't been passed (and configured) by the user. A notable case
    is when a Logger is passed, which is not a CSV or JSON logger - then
    a CSV and JSON logger will still be created.

    Lastly, this function will ensure that the Syncer callback comes after all
    Logger callbacks, to ensure that the most up-to-date logs and checkpoints
    are synced across nodes.

    """
    callbacks = callbacks or []

    # Initialize callbacks from environment variable
    env_callbacks = _initialize_env_callbacks()
    callbacks.extend(env_callbacks)

    has_csv_logger = False
    has_json_logger = False
    has_tbx_logger = False

    from ray.tune.progress_reporter import TrialProgressCallback

    has_trial_progress_callback = any(
        isinstance(c, TrialProgressCallback) for c in callbacks
    )

    if has_trial_progress_callback and air_verbosity is not None:
        logger.warning(
            "AIR_VERBOSITY is set, ignoring passed-in TrialProgressCallback."
        )
        new_callbacks = [
            c for c in callbacks if not isinstance(c, TrialProgressCallback)
        ]
        callbacks = new_callbacks
    if air_verbosity is not None:  # new flow
        from ray.tune.experimental.output import (
            _detect_reporter as _detect_air_reporter,
        )

        air_progress_reporter = _detect_air_reporter(
            air_verbosity,
            num_samples=1,  # Update later with setup()
            entrypoint=entrypoint,
            metric=metric,
            mode=mode,
            config=config,
            progress_metrics=progress_metrics,
        )
        callbacks.append(air_progress_reporter)
    elif not has_trial_progress_callback:  # old flow
        trial_progress_callback = TrialProgressCallback(
            metric=metric, progress_metrics=progress_metrics
        )
        callbacks.append(trial_progress_callback)

    # Check if we have a CSV, JSON and TensorboardX logger
    for i, callback in enumerate(callbacks):
        if isinstance(callback, LegacyLoggerCallback):
            if CSVLogger in callback.logger_classes:
                has_csv_logger = True
            if JsonLogger in callback.logger_classes:
                has_json_logger = True
            if TBXLogger in callback.logger_classes:
                has_tbx_logger = True
        elif isinstance(callback, CSVLoggerCallback):
            has_csv_logger = True
        elif isinstance(callback, JsonLoggerCallback):
            has_json_logger = True
        elif isinstance(callback, TBXLoggerCallback):
            has_tbx_logger = True

    # If CSV, JSON or TensorboardX loggers are missing, add
    if os.environ.get("TUNE_DISABLE_AUTO_CALLBACK_LOGGERS", "0") != "1":
        if not has_csv_logger:
            callbacks.append(CSVLoggerCallback())
        if not has_json_logger:
            callbacks.append(JsonLoggerCallback())
        if not has_tbx_logger:
            try:
                callbacks.append(TBXLoggerCallback())
            except ImportError:
                logger.warning(
                    "The TensorboardX logger cannot be instantiated because "
                    "either TensorboardX or one of it's dependencies is not "
                    "installed. Please make sure you have the latest version "
                    "of TensorboardX installed: `pip install -U tensorboardx`"
                )

    return callbacks


def _initialize_env_callbacks() -> List[Callback]:
    """Initialize callbacks from environment variable.

    Returns:
        List of callbacks initialized from environment variable.
    """
    callbacks = []
    callbacks_str = os.environ.get(RAY_TUNE_CALLBACKS_ENV_VAR, "")
    if not callbacks_str:
        return callbacks

    for callback_path in callbacks_str.split(","):
        callback_path = callback_path.strip()
        if not callback_path:
            continue

        try:
            module_path, class_name = callback_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            callback_cls = getattr(module, class_name)
            if not issubclass(callback_cls, Callback):
                raise TypeError(
                    f"Callback class '{callback_path}' must be a subclass of "
                    f"Callback, got {type(callback_cls).__name__}"
                )
            callback = callback_cls()
            callbacks.append(callback)
        except (ImportError, AttributeError, ValueError, TypeError) as e:
            raise ValueError(f"Failed to import callback from '{callback_path}'") from e

    return callbacks
