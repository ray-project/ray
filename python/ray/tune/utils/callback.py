import logging
import os
from typing import List, Optional

from ray.tune.callback import Callback
from ray.tune.syncer import SyncConfig
from ray.tune.logger import CSVExperimentLogger, CSVLogger, \
    ExperimentLogger, \
    JsonExperimentLogger, JsonLogger, LegacyExperimentLogger, Logger, \
    TBXExperimentLogger, TBXLogger
from ray.tune.syncer import SyncerCallback

logger = logging.getLogger(__name__)


def create_default_callbacks(callbacks: Optional[List[Callback]],
                             sync_config: SyncConfig,
                             loggers: Optional[List[Logger]]):

    callbacks = callbacks or []
    has_syncer_callback = False
    has_csv_logger = False
    has_json_logger = False
    has_tbx_logger = False

    # Track syncer obj/index to move callback after loggers
    last_logger_index = None
    syncer_index = None

    # Create LegacyExperimentLogger for passed Logger classes
    if loggers:
        # Todo(krfricke): Deprecate `loggers` argument, print warning here.
        # Add warning as soon as we ported all loggers to ExperimentLogger
        # classes.
        add_loggers = []
        for trial_logger in loggers:
            if isinstance(trial_logger, ExperimentLogger):
                callbacks.append(trial_logger)
            elif isinstance(trial_logger, type) and issubclass(
                    trial_logger, Logger):
                add_loggers.append(trial_logger)
            else:
                raise ValueError(
                    f"Invalid value passed to `loggers` argument of "
                    f"`tune.run()`: {trial_logger}")
        if add_loggers:
            callbacks.append(LegacyExperimentLogger(add_loggers))

    # Check if we have a CSV, JSON and TensorboardX logger
    for i, callback in enumerate(callbacks):
        if isinstance(callback, LegacyExperimentLogger):
            last_logger_index = i
            if CSVLogger in callback.logger_classes:
                has_csv_logger = True
            if JsonLogger in callback.logger_classes:
                has_json_logger = True
            if TBXLogger in callback.logger_classes:
                has_tbx_logger = True
        elif isinstance(callback, CSVExperimentLogger):
            has_csv_logger = True
            last_logger_index = i
        elif isinstance(callback, JsonExperimentLogger):
            has_json_logger = True
            last_logger_index = i
        elif isinstance(callback, TBXExperimentLogger):
            has_tbx_logger = True
            last_logger_index = i
        elif isinstance(callback, SyncerCallback):
            syncer_index = i
            has_syncer_callback = True

    # If CSV, JSON or TensorboardX loggers are missing, add
    if os.environ.get("TUNE_DISABLE_AUTO_CALLBACK_LOGGERS", "0") != "1":
        if not has_csv_logger:
            callbacks.append(CSVExperimentLogger())
            last_logger_index = len(callbacks) - 1
        if not has_json_logger:
            callbacks.append(JsonExperimentLogger())
            last_logger_index = len(callbacks) - 1
        if not has_tbx_logger:
            callbacks.append(TBXExperimentLogger())
            last_logger_index = len(callbacks) - 1

    # If no SyncerCallback was found, add
    if not has_syncer_callback and os.environ.get(
            "TUNE_DISABLE_AUTO_CALLBACK_SYNCER", "0") != "1":
        syncer_callback = SyncerCallback(
            sync_function=sync_config.sync_to_driver)
        callbacks.append(syncer_callback)
        syncer_index = len(callbacks) - 1

    if syncer_index is not None and last_logger_index is not None and \
       syncer_index < last_logger_index:
        if (not has_csv_logger or not has_json_logger or not has_tbx_logger) \
           and not loggers:
            # Only raise the warning if the loggers were passed by the user.
            # (I.e. don't warn if this was automatic behavior and they only
            # passed a customer SyncerCallback).
            raise ValueError(
                "The `SyncerCallback` you passed to `tune.run()` came before "
                "at least one `ExperimentLogger`. Syncing should be done "
                "after writing logs. Please re-order the callbacks so that "
                "the `SyncerCallback` comes after any `ExperimentLogger`.")
        else:
            # If these loggers were automatically created. just re-order
            # the callbacks
            syncer_obj = callbacks[syncer_index]
            callbacks.pop(syncer_index)
            callbacks.insert(last_logger_index, syncer_obj)

    return callbacks
