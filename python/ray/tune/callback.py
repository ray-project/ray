from typing import TYPE_CHECKING, Dict, List

from ray.tune.checkpoint_manager import Checkpoint

if TYPE_CHECKING:
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

    def on_step_begin(self, iteration: int, trials: List["Trial"], **info):
        """Called at the start of each tuning loop step.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_step_end(self, iteration: int, trials: List["Trial"], **info):
        """Called at the end of each tuning loop step.

        The iteration counter is increased before this hook is called.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_trial_start(self, iteration: int, trials: List["Trial"],
                       trial: "Trial", **info):
        """Called after starting a trial instance.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            trial (Trial): Trial that just has been started.
            **info: Kwargs dict for forward compatibility.

        """
        pass

    def on_trial_restore(self, iteration: int, trials: List["Trial"],
                         trial: "Trial", **info):
        """Called after restoring a trial instance.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            trial (Trial): Trial that just has been restored.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_trial_save(self, iteration: int, trials: List["Trial"],
                      trial: "Trial", **info):
        """Called after receiving a checkpoint from a trial.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            trial (Trial): Trial that just saved a checkpoint.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_trial_result(self, iteration: int, trials: List["Trial"],
                        trial: "Trial", result: Dict, **info):
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

    def on_trial_complete(self, iteration: int, trials: List["Trial"],
                          trial: "Trial", **info):
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

    def on_trial_error(self, iteration: int, trials: List["Trial"],
                       trial: "Trial", **info):
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

    def on_checkpoint(self, iteration: int, trials: List["Trial"],
                      trial: "Trial", checkpoint: Checkpoint, **info):
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
