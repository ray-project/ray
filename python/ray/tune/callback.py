from abc import ABCMeta
import glob
import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple
import warnings

from ray.util.annotations import PublicAPI, DeveloperAPI
from ray.tune.utils.util import _atomic_save, _load_newest_checkpoint

if TYPE_CHECKING:
    from ray.train import Checkpoint
    from ray.tune.experiment import Trial
    from ray.tune.stopper import Stopper


class _CallbackMeta(ABCMeta):
    """A helper metaclass to ensure container classes (e.g. CallbackList) have
    implemented all the callback methods (e.g. `on_*`).
    """

    def __new__(mcs, name: str, bases: Tuple[type], attrs: Dict[str, Any]) -> type:
        cls = super().__new__(mcs, name, bases, attrs)

        if mcs.need_check(cls, name, bases, attrs):
            mcs.check(cls, name, bases, attrs)

        return cls

    @classmethod
    def need_check(
        mcs, cls: type, name: str, bases: Tuple[type], attrs: Dict[str, Any]
    ) -> bool:

        return attrs.get("IS_CALLBACK_CONTAINER", False)

    @classmethod
    def check(
        mcs, cls: type, name: str, bases: Tuple[type], attrs: Dict[str, Any]
    ) -> None:

        methods = set()
        for base in bases:
            methods.update(
                attr_name
                for attr_name, attr in vars(base).items()
                if mcs.need_override_by_subclass(attr_name, attr)
            )
        overridden = {
            attr_name
            for attr_name, attr in attrs.items()
            if mcs.need_override_by_subclass(attr_name, attr)
        }
        missing = methods.difference(overridden)
        if missing:
            raise TypeError(
                f"Found missing callback method: {missing} "
                f"in class {cls.__module__}.{cls.__qualname__}."
            )

    @classmethod
    def need_override_by_subclass(mcs, attr_name: str, attr: Any) -> bool:
        return (
            (
                attr_name.startswith("on_")
                and not attr_name.startswith("on_trainer_init")
            )
            or attr_name == "setup"
        ) and callable(attr)


@PublicAPI(stability="beta")
class Callback(metaclass=_CallbackMeta):
    """Tune base callback that can be extended and passed to a ``TrialRunner``

    Tune callbacks are called from within the ``TrialRunner`` class. There are
    several hooks that can be used, all of which are found in the submethod
    definitions of this base class.

    The parameters passed to the ``**info`` dict vary between hooks. The
    parameters passed are described in the docstrings of the methods.

    This example will print a metric each time a result is received:

    .. testcode::

        from ray import train, tune
        from ray.tune import Callback


        class MyCallback(Callback):
            def on_trial_result(self, iteration, trials, trial, result,
                                **info):
                print(f"Got result: {result['metric']}")


        def train_func(config):
            for i in range(10):
                tune.report(metric=i)

        tuner = tune.Tuner(
            train_func,
            run_config=train.RunConfig(
                callbacks=[MyCallback()]
            )
        )
        tuner.fit()

    .. testoutput::
        :hide:

        ...
    """

    # File templates for any artifacts written by this callback
    # These files should live in the `trial.local_path` for each trial.
    # TODO(ml-team): Make this more visible to users to override. Internal use for now.
    _SAVED_FILE_TEMPLATES = []

    # arguments here match Experiment.public_spec
    def setup(
        self,
        stop: Optional["Stopper"] = None,
        num_samples: Optional[int] = None,
        total_num_samples: Optional[int] = None,
        **info,
    ):
        """Called once at the very beginning of training.

        Any Callback setup should be added here (setting environment
        variables, etc.)

        Arguments:
            stop: Stopping criteria.
                If ``time_budget_s`` was passed to ``train.RunConfig``, a
                ``TimeoutStopper`` will be passed here, either by itself
                or as a part of a ``CombinedStopper``.
            num_samples: Number of times to sample from the
                hyperparameter space. Defaults to 1. If `grid_search` is
                provided as an argument, the grid will be repeated
                `num_samples` of times. If this is -1, (virtually) infinite
                samples are generated until a stopping condition is met.
            total_num_samples: Total number of samples factoring
                in grid search samplers.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_step_begin(self, iteration: int, trials: List["Trial"], **info):
        """Called at the start of each tuning loop step.

        Arguments:
            iteration: Number of iterations of the tuning loop.
            trials: List of trials.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_step_end(self, iteration: int, trials: List["Trial"], **info):
        """Called at the end of each tuning loop step.

        The iteration counter is increased before this hook is called.

        Arguments:
            iteration: Number of iterations of the tuning loop.
            trials: List of trials.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_trial_start(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        """Called after starting a trial instance.

        Arguments:
            iteration: Number of iterations of the tuning loop.
            trials: List of trials.
            trial: Trial that just has been started.
            **info: Kwargs dict for forward compatibility.

        """
        pass

    def on_trial_restore(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        """Called after restoring a trial instance.

        Arguments:
            iteration: Number of iterations of the tuning loop.
            trials: List of trials.
            trial: Trial that just has been restored.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_trial_save(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        """Called after receiving a checkpoint from a trial.

        Arguments:
            iteration: Number of iterations of the tuning loop.
            trials: List of trials.
            trial: Trial that just saved a checkpoint.
            **info: Kwargs dict for forward compatibility.
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
        """Called after receiving a result from a trial.

        The search algorithm and scheduler are notified before this
        hook is called.

        Arguments:
            iteration: Number of iterations of the tuning loop.
            trials: List of trials.
            trial: Trial that just sent a result.
            result: Result that the trial sent.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_trial_complete(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        """Called after a trial instance completed.

        The search algorithm and scheduler are notified before this
        hook is called.

        Arguments:
            iteration: Number of iterations of the tuning loop.
            trials: List of trials.
            trial: Trial that just has been completed.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_trial_recover(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        """Called after a trial instance failed (errored) but the trial is scheduled
        for retry.

        The search algorithm and scheduler are not notified.

        Arguments:
            iteration: Number of iterations of the tuning loop.
            trials: List of trials.
            trial: Trial that just has errored.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_trial_error(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        """Called after a trial instance failed (errored).

        The search algorithm and scheduler are notified before this
        hook is called.

        Arguments:
            iteration: Number of iterations of the tuning loop.
            trials: List of trials.
            trial: Trial that just has errored.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_checkpoint(
        self,
        iteration: int,
        trials: List["Trial"],
        trial: "Trial",
        checkpoint: "Checkpoint",
        **info,
    ):
        """Called after a trial saved a checkpoint with Tune.

        Arguments:
            iteration: Number of iterations of the tuning loop.
            trials: List of trials.
            trial: Trial that just has errored.
            checkpoint: Checkpoint object that has been saved
                by the trial.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_experiment_end(self, trials: List["Trial"], **info):
        """Called after experiment is over and all trials have concluded.

        Arguments:
            trials: List of trials.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def get_state(self) -> Optional[Dict]:
        """Get the state of the callback.

        This method should be implemented by subclasses to return a dictionary
        representation of the object's current state.

        This is called automatically by Tune to periodically checkpoint callback state.
        Upon :ref:`Tune experiment restoration <tune-experiment-level-fault-tolerance>`,
        callback state will be restored via :meth:`~ray.tune.Callback.set_state`.

        .. testcode::

            from typing import Dict, List, Optional

            from ray.tune import Callback
            from ray.tune.experiment import Trial

            class MyCallback(Callback):
                def __init__(self):
                    self._trial_ids = set()

                def on_trial_start(
                    self, iteration: int, trials: List["Trial"], trial: "Trial", **info
                ):
                    self._trial_ids.add(trial.trial_id)

                def get_state(self) -> Optional[Dict]:
                    return {"trial_ids": self._trial_ids.copy()}

                def set_state(self, state: Dict) -> Optional[Dict]:
                    self._trial_ids = state["trial_ids"]

        Returns:
            dict: State of the callback. Should be `None` if the callback does not
            have any state to save (this is the default).
        """
        return None

    def set_state(self, state: Dict):
        """Set the state of the callback.

        This method should be implemented by subclasses to restore the callback's
        state based on the given dict state.

        This is used automatically by Tune to restore checkpoint callback state
        on :ref:`Tune experiment restoration <tune-experiment-level-fault-tolerance>`.

        See :meth:`~ray.tune.Callback.get_state` for an example implementation.

        Args:
            state: State of the callback.
        """
        pass


@DeveloperAPI
class CallbackList(Callback):
    """Call multiple callbacks at once."""

    IS_CALLBACK_CONTAINER = True
    CKPT_FILE_TMPL = "callback-states-{}.pkl"

    def __init__(self, callbacks: List[Callback]):
        self._callbacks = callbacks

    def setup(self, **info):
        for callback in self._callbacks:
            try:
                callback.setup(**info)
            except TypeError as e:
                if "argument" in str(e):
                    warnings.warn(
                        "Please update `setup` method in callback "
                        f"`{callback.__class__}` to match the method signature"
                        " in `ray.tune.callback.Callback`.",
                        FutureWarning,
                    )
                    callback.setup()
                else:
                    raise e

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

    def on_trial_recover(self, **info):
        for callback in self._callbacks:
            callback.on_trial_recover(**info)

    def on_trial_error(self, **info):
        for callback in self._callbacks:
            callback.on_trial_error(**info)

    def on_checkpoint(self, **info):
        for callback in self._callbacks:
            callback.on_checkpoint(**info)

    def on_experiment_end(self, **info):
        for callback in self._callbacks:
            callback.on_experiment_end(**info)

    def get_state(self) -> Optional[Dict]:
        """Gets the state of all callbacks contained within this list.
        If there are no stateful callbacks, then None will be returned in order
        to avoid saving an unnecessary callback checkpoint file."""
        state = {}
        any_stateful_callbacks = False
        for i, callback in enumerate(self._callbacks):
            callback_state = callback.get_state()
            if callback_state:
                any_stateful_callbacks = True
            state[i] = callback_state
        if not any_stateful_callbacks:
            return None
        return state

    def set_state(self, state: Dict):
        """Sets the state for all callbacks contained within this list.
        Skips setting state for all stateless callbacks where `get_state`
        returned None."""
        for i, callback in enumerate(self._callbacks):
            callback_state = state.get(i, None)
            if callback_state:
                callback.set_state(callback_state)

    def save_to_dir(self, checkpoint_dir: str, session_str: str = "default"):
        """Save the state of the callback list to the checkpoint_dir.

        Args:
            checkpoint_dir: directory where the checkpoint is stored.
            session_str: Unique identifier of the current run session (ex: timestamp).
        """
        state_dict = self.get_state()

        if state_dict:
            file_name = self.CKPT_FILE_TMPL.format(session_str)
            tmp_file_name = f".tmp-{file_name}"
            _atomic_save(
                state=state_dict,
                checkpoint_dir=checkpoint_dir,
                file_name=file_name,
                tmp_file_name=tmp_file_name,
            )

    def restore_from_dir(self, checkpoint_dir: str):
        """Restore the state of the list of callbacks from the checkpoint_dir.

        You should check if it's possible to restore with `can_restore`
        before calling this method.

        Args:
            checkpoint_dir: directory where the checkpoint is stored.

        Raises:
            RuntimeError: if unable to find checkpoint.
            NotImplementedError: if the `set_state` method is not implemented.
        """
        state_dict = _load_newest_checkpoint(
            checkpoint_dir, self.CKPT_FILE_TMPL.format("*")
        )
        if not state_dict:
            raise RuntimeError(
                "Unable to find checkpoint in {}.".format(checkpoint_dir)
            )
        self.set_state(state_dict)

    def can_restore(self, checkpoint_dir: str) -> bool:
        """Check if the checkpoint_dir contains the saved state for this callback list.

        Returns:
            can_restore: True if the checkpoint_dir contains a file of the
                format `CKPT_FILE_TMPL`. False otherwise.
        """
        return bool(
            glob.glob(os.path.join(checkpoint_dir, self.CKPT_FILE_TMPL.format("*")))
        )

    def __len__(self) -> int:
        return len(self._callbacks)

    def __getitem__(self, i: int) -> "Callback":
        return self._callbacks[i]
