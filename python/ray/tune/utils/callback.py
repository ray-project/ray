from typing import List, Optional

import logging
import os

from ray.tune.callback import Callback
from ray.tune.progress_reporter import TrialProgressCallback
from ray.tune.syncer import SyncConfig
from ray.tune.logger import (
    CSVLoggerCallback,
    CSVLogger,
    JsonLoggerCallback,
    JsonLogger,
    LegacyLoggerCallback,
    LoggerCallback,
    TBXLoggerCallback,
    TBXLogger,
)
from ray.tune.syncer import SyncerCallback

logger = logging.getLogger(__name__)


def _create_default_callbacks(
    callbacks: Optional[List[Callback]],
    sync_config: SyncConfig,
    metric: Optional[str] = None,
    progress_metrics: Optional[List[str]] = None,
):
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
    has_syncer_callback = False
    has_csv_logger = False
    has_json_logger = False
    has_tbx_logger = False

    has_trial_progress_callback = any(
        isinstance(c, TrialProgressCallback) for c in callbacks
    )

    if not has_trial_progress_callback:
        trial_progress_callback = TrialProgressCallback(
            metric=metric, progress_metrics=progress_metrics
        )
        callbacks.append(trial_progress_callback)

    # Track syncer obj/index to move callback after loggers
    last_logger_index = None
    syncer_index = None

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
        elif isinstance(callback, SyncerCallback):
            syncer_index = i
            has_syncer_callback = True

        if isinstance(callback, LoggerCallback):
            last_logger_index = i

    # If CSV, JSON or TensorboardX loggers are missing, add
    if os.environ.get("TUNE_DISABLE_AUTO_CALLBACK_LOGGERS", "0") != "1":
        if not has_csv_logger:
            callbacks.append(CSVLoggerCallback())
            last_logger_index = len(callbacks) - 1
        if not has_json_logger:
            callbacks.append(JsonLoggerCallback())
            last_logger_index = len(callbacks) - 1
        if not has_tbx_logger:
            try:
                callbacks.append(TBXLoggerCallback())
                last_logger_index = len(callbacks) - 1
            except ImportError:
                logger.warning(
                    "The TensorboardX logger cannot be instantiated because "
                    "either TensorboardX or one of it's dependencies is not "
                    "installed. Please make sure you have the latest version "
                    "of TensorboardX installed: `pip install -U tensorboardx`"
                )

    # If no SyncerCallback was found, add
    if (
        not has_syncer_callback
        and os.environ.get("TUNE_DISABLE_AUTO_CALLBACK_SYNCER", "0") != "1"
    ):
        syncer_callback = SyncerCallback(
            enabled=bool(sync_config.syncer), sync_period=sync_config.sync_period
        )
        callbacks.append(syncer_callback)
        syncer_index = len(callbacks) - 1

    if (
        syncer_index is not None
        and last_logger_index is not None
        and syncer_index < last_logger_index
    ):
        # Re-order callbacks
        syncer_obj = callbacks[syncer_index]
        callbacks.pop(syncer_index)
        callbacks.insert(last_logger_index, syncer_obj)

    return callbacks
