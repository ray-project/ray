import os
from typing import List, Optional

from ray.tune.callback import Callback
from ray.tune.progress_reporter import TrialProgressCallback
from ray.tune.syncer import SyncConfig
from ray.tune.logger import CSVLogger, DEFAULT_LOGGERS, ExperimentLogger, \
    JsonLogger, LegacyExperimentLogger, Logger
from ray.tune.syncer import SyncerCallback


def create_default_callbacks(callbacks: Optional[List[Callback]],
                             sync_config: SyncConfig,
                             loggers: Optional[List[Logger]],
                             metric: Optional[str] = None):
    callbacks = callbacks or []
    has_syncer_callback = False
    has_csv_logger = False
    has_json_logger = False

    has_trial_progress_callback = any(
        isinstance(c, TrialProgressCallback) for c in callbacks)

    if not has_trial_progress_callback:
        trial_progress_callback = TrialProgressCallback(metric=metric)
    callbacks.append(trial_progress_callback)

    # Track syncer obj/index to move callback after loggers
    last_logger_index = None
    syncer_index = None

    if not loggers:
        # If no logger callback and no `loggers` have been provided,
        # add DEFAULT_LOGGERS.
        if not any(
                isinstance(callback, ExperimentLogger)
                for callback in callbacks):
            loggers = DEFAULT_LOGGERS

    # Create LegacyExperimentLogger for passed Logger classes
    if loggers:
        # Todo(krfricke): Deprecate `loggers` argument, print warning here.
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

    # Check if we have a CSV and JSON logger
    for i, callback in enumerate(callbacks):
        if isinstance(callback, LegacyExperimentLogger):
            last_logger_index = i
            if CSVLogger in callback.logger_classes:
                has_csv_logger = True
            if JsonLogger in callback.logger_classes:
                has_json_logger = True
        # Todo(krfricke): add checks for new ExperimentLogger classes
        elif isinstance(callback, SyncerCallback):
            syncer_index = i
            has_syncer_callback = True

    # If CSV or JSON logger is missing, add
    if os.environ.get("TUNE_DISABLE_AUTO_CALLBACK_LOGGERS", "0") != "1":
        # Todo(krfricke): Switch to new ExperimentLogger classes
        add_loggers = []
        if not has_csv_logger:
            add_loggers.append(CSVLogger)
        if not has_json_logger:
            add_loggers.append(JsonLogger)
        if add_loggers:
            callbacks.append(LegacyExperimentLogger(add_loggers))
            last_logger_index = len(callbacks) - 1
>>>>>>> master

    # If no SyncerCallback was found, add
    if not has_syncer_callback and os.environ.get(
            "TUNE_DISABLE_AUTO_CALLBACK_SYNCER", "0") != "1":
        syncer_callback = SyncerCallback(
            sync_function=sync_config.sync_to_driver)
        callbacks.append(syncer_callback)
        syncer_index = len(callbacks) - 1

    # Todo(krfricke): Maybe check if syncer comes after all loggers
    if syncer_index is not None and last_logger_index is not None and \
       syncer_index < last_logger_index:
        if (not has_csv_logger or not has_json_logger) and not loggers:
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
