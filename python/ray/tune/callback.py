import os
from typing import Dict, List, Optional

from ray.tune import SyncConfig
from ray.tune.checkpoint_manager import Checkpoint
from ray.tune.logger import CSVLogger, DEFAULT_LOGGERS, ExperimentLogger, \
    JsonLogger, LegacyExperimentLogger, Logger
from ray.tune.syncer import SyncerCallback
from ray.tune.trial import Trial


class Callback:
    """Tune base callback that can be extended and passed to a ``TrialRunner``

    Tune callbacks are called from within the ``TrialRunner`` class. There are
    several hooks that can be used, all of which are found in the submethod
    definitions of this base class.

    The parameters passed to the ``**info`` dict vary between hooks. The
    parameters passed are described in the docstrings of the methods.

    This example will print a metric each time a result is received:

    .. code-block:: python

        from ray import tune
        from ray.tune import Callback


        class MyCallback(Callback):
            def on_trial_result(self, iteration, trials, trial, result,
                                **info):
                print(f"Got result: {result['metric']}")


        def train(config):
            for i in range(10):
                tune.report(metric=i)


        tune.run(
            train,
            callbacks=[MyCallback()])

    """

    def on_step_begin(self, iteration: int, trials: List[Trial], **info):
        """Called at the start of each tuning loop step.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_step_end(self, iteration: int, trials: List[Trial], **info):
        """Called at the end of each tuning loop step.

        The iteration counter is increased before this hook is called.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_trial_start(self, iteration: int, trials: List[Trial], trial: Trial,
                       **info):
        """Called after starting a trial instance.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            trial (Trial): Trial that just has been started.
            **info: Kwargs dict for forward compatibility.

        """
        pass

    def on_trial_restore(self, iteration: int, trials: List[Trial],
                         trial: Trial, **info):
        """Called after restoring a trial instance.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            trial (Trial): Trial that just has been restored.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_trial_save(self, iteration: int, trials: List[Trial], trial: Trial,
                      **info):
        """Called after receiving a checkpoint from a trial.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            trial (Trial): Trial that just saved a checkpoint.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_trial_result(self, iteration: int, trials: List[Trial],
                        trial: Trial, result: Dict, **info):
        """Called after receiving a result from a trial.

        The search algorithm and scheduler are notified before this
        hook is called.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            trial (Trial): Trial that just sent a result.
            result (Dict): Result that the trial sent.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_trial_complete(self, iteration: int, trials: List[Trial],
                          trial: Trial, **info):
        """Called after a trial instance completed.

        The search algorithm and scheduler are notified before this
        hook is called.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            trial (Trial): Trial that just has been completed.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_trial_error(self, iteration: int, trials: List[Trial], trial: Trial,
                       **info):
        """Called after a trial instance failed (errored).

        The search algorithm and scheduler are notified before this
        hook is called.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            trial (Trial): Trial that just has errored.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_checkpoint(self, iteration: int, trials: List[Trial], trial: Trial,
                      checkpoint: Checkpoint, **info):
        """Called after a trial saved a checkpoint with Tune.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            trial (Trial): Trial that just has errored.
            checkpoint (Checkpoint): Checkpoint object that has been saved
                by the trial.
            **info: Kwargs dict for forward compatibility.
        """
        pass


class CallbackList:
    """Call multiple callbacks at once."""

    def __init__(self, callbacks: List[Callback]):
        self._callbacks = callbacks

    def on_step_begin(self, **info):
        for callback in self._callbacks:
            callback.on_step_begin(**info)

    def on_step_end(self, **info):
        for callback in self._callbacks:
            callback.on_step_end(**info)

    def on_trial_start(self, **info):
        for callback in self._callbacks:
            callback.on_trial_start(**info)

    def on_trial_restore(self, **info):
        for callback in self._callbacks:
            callback.on_trial_restore(**info)

    def on_trial_save(self, **info):
        for callback in self._callbacks:
            callback.on_trial_save(**info)

    def on_trial_result(self, **info):
        for callback in self._callbacks:
            callback.on_trial_result(**info)

    def on_trial_complete(self, **info):
        for callback in self._callbacks:
            callback.on_trial_complete(**info)

    def on_trial_error(self, **info):
        for callback in self._callbacks:
            callback.on_trial_error(**info)

    def on_checkpoint(self, **info):
        for callback in self._callbacks:
            callback.on_checkpoint(**info)


def create_default_callbacks(callbacks: Optional[List[Callback]],
                             sync_config: SyncConfig,
                             loggers: Optional[List[Logger]]):

    callbacks = callbacks or []
    has_syncer_callback = False
    has_csv_logger = False
    has_json_logger = False

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
