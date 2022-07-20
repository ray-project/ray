import copy
from functools import partial
import grpc
import inspect
import logging
import os
from pathlib import Path
from pickle import PicklingError
import traceback
from typing import Any, Dict, Optional, Sequence, Union, Callable, Type, List

from ray.tune.error import TuneError
from ray.tune.registry import register_trainable
from ray.tune.result import DEFAULT_RESULTS_DIR
from ray.tune.stopper import CombinedStopper, FunctionStopper, Stopper, TimeoutStopper
from ray.tune.syncer import SyncConfig
from ray.tune.utils import date_str, detect_checkpoint_function

from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


def _validate_log_to_file(log_to_file):
    """Validate ``tune.run``'s ``log_to_file`` parameter. Return
    validated relative stdout and stderr filenames."""
    if not log_to_file:
        stdout_file = stderr_file = None
    elif isinstance(log_to_file, bool) and log_to_file:
        stdout_file = "stdout"
        stderr_file = "stderr"
    elif isinstance(log_to_file, str):
        stdout_file = stderr_file = log_to_file
    elif isinstance(log_to_file, Sequence):
        if len(log_to_file) != 2:
            raise ValueError(
                "If you pass a Sequence to `log_to_file` it has to have "
                "a length of 2 (for stdout and stderr, respectively). The "
                "Sequence you passed has length {}.".format(len(log_to_file))
            )
        stdout_file, stderr_file = log_to_file
    else:
        raise ValueError(
            "You can pass a boolean, a string, or a Sequence of length 2 to "
            "`log_to_file`, but you passed something else ({}).".format(
                type(log_to_file)
            )
        )
    return stdout_file, stderr_file


def _get_local_dir_with_expand_user(local_dir: Optional[str]) -> str:
    return os.path.abspath(os.path.expanduser(local_dir or DEFAULT_RESULTS_DIR))


def _get_dir_name(run, explicit_name: Optional[str], combined_name: str) -> str:
    # If the name has been set explicitly, we don't want to create
    # dated directories. The same is true for string run identifiers.
    if (
        int(os.environ.get("TUNE_DISABLE_DATED_SUBDIR", 0)) == 1
        or explicit_name
        or isinstance(run, str)
    ):
        dir_name = combined_name
    else:
        dir_name = "{}_{}".format(combined_name, date_str())
    return dir_name


@DeveloperAPI
class Experiment:
    """Tracks experiment specifications.

    Implicitly registers the Trainable if needed. The args here take
    the same meaning as the arguments defined `tune.py:run`.

    .. code-block:: python

        experiment_spec = Experiment(
            "my_experiment_name",
            my_func,
            stop={"mean_accuracy": 100},
            config={
                "alpha": tune.grid_search([0.2, 0.4, 0.6]),
                "beta": tune.grid_search([1, 2]),
            },
            resources_per_trial={
                "cpu": 1,
                "gpu": 0
            },
            num_samples=10,
            local_dir="~/ray_results",
            checkpoint_freq=10,
            max_failures=2)

    Args:
        TODO(xwjiang): Add the whole list.
        _experiment_checkpoint_dir: Internal use only. If present, use this
            as the root directory for experiment checkpoint. If not present,
            the directory path will be deduced from trainable name instead.
    """

    # Keys that will be present in `public_spec` dict.
    PUBLIC_KEYS = {"stop", "num_samples", "time_budget_s"}

    def __init__(
        self,
        name,
        run,
        stop=None,
        time_budget_s=None,
        config=None,
        resources_per_trial=None,
        num_samples=1,
        local_dir=None,
        _experiment_checkpoint_dir: Optional[str] = None,
        sync_config=None,
        trial_name_creator=None,
        trial_dirname_creator=None,
        log_to_file=False,
        checkpoint_freq=0,
        checkpoint_at_end=False,
        keep_checkpoints_num=None,
        checkpoint_score_attr=None,
        export_formats=None,
        max_failures=0,
        restore=None,
    ):

        local_dir = _get_local_dir_with_expand_user(local_dir)
        # `_experiment_checkpoint_dir` is for internal use only for better
        # support of Tuner API.
        # If set, it should be a subpath under `local_dir`. Also deduce `dir_name`.
        self._experiment_checkpoint_dir = _experiment_checkpoint_dir
        if _experiment_checkpoint_dir:
            experiment_checkpoint_dir_path = Path(_experiment_checkpoint_dir)
            local_dir_path = Path(local_dir)
            assert local_dir_path in experiment_checkpoint_dir_path.parents
            # `dir_name` is set by `_experiment_checkpoint_dir` indirectly.
            self.dir_name = os.path.relpath(_experiment_checkpoint_dir, local_dir)

        config = config or {}
        sync_config = sync_config or SyncConfig()
        if (
            callable(run)
            and not inspect.isclass(run)
            and detect_checkpoint_function(run)
        ):
            if checkpoint_at_end:
                raise ValueError(
                    "'checkpoint_at_end' cannot be used with a "
                    "checkpointable function. You can specify "
                    "and register checkpoints within "
                    "your trainable function."
                )
            if checkpoint_freq:
                raise ValueError(
                    "'checkpoint_freq' cannot be used with a "
                    "checkpointable function. You can specify checkpoints "
                    "within your trainable function."
                )
        try:
            self._run_identifier = Experiment.register_if_needed(run)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                raise TuneError(
                    f"The Trainable/training function is too large for grpc resource "
                    f"limit. Check that its definition is not implicitly capturing a "
                    f"large array or other object in scope. "
                    f"Tip: use tune.with_parameters() to put large objects "
                    f"in the Ray object store. \n"
                    f"Original exception: {traceback.format_exc()}"
                )
            else:
                raise e

        self.name = name or self._run_identifier

        if not _experiment_checkpoint_dir:
            self.dir_name = _get_dir_name(run, name, self.name)

        assert self.dir_name

        if sync_config.upload_dir:
            self.remote_checkpoint_dir = os.path.join(
                sync_config.upload_dir, self.dir_name
            )
        else:
            self.remote_checkpoint_dir = None

        self._stopper = None
        stopping_criteria = {}
        if not stop:
            pass
        elif isinstance(stop, list):
            bad_stoppers = [s for s in stop if not isinstance(s, Stopper)]
            if bad_stoppers:
                stopper_types = [type(s) for s in stop]
                raise ValueError(
                    "If you pass a list as the `stop` argument to "
                    "`tune.run()`, each element must be an instance of "
                    f"`tune.stopper.Stopper`. Got {stopper_types}."
                )
            self._stopper = CombinedStopper(*stop)
        elif isinstance(stop, dict):
            stopping_criteria = stop
        elif callable(stop):
            if FunctionStopper.is_valid_function(stop):
                self._stopper = FunctionStopper(stop)
            elif isinstance(stop, Stopper):
                self._stopper = stop
            else:
                raise ValueError(
                    "Provided stop object must be either a dict, "
                    "a function, or a subclass of "
                    f"`ray.tune.Stopper`. Got {type(stop)}."
                )
        else:
            raise ValueError(
                f"Invalid stop criteria: {stop}. Must be a "
                f"callable or dict. Got {type(stop)}."
            )

        if time_budget_s:
            if self._stopper:
                self._stopper = CombinedStopper(
                    self._stopper, TimeoutStopper(time_budget_s)
                )
            else:
                self._stopper = TimeoutStopper(time_budget_s)

        stdout_file, stderr_file = _validate_log_to_file(log_to_file)

        spec = {
            "run": self._run_identifier,
            "stop": stopping_criteria,
            "time_budget_s": time_budget_s,
            "config": config,
            "resources_per_trial": resources_per_trial,
            "num_samples": num_samples,
            "local_dir": local_dir,
            "sync_config": sync_config,
            "remote_checkpoint_dir": self.remote_checkpoint_dir,
            "trial_name_creator": trial_name_creator,
            "trial_dirname_creator": trial_dirname_creator,
            "log_to_file": (stdout_file, stderr_file),
            "checkpoint_freq": checkpoint_freq,
            "checkpoint_at_end": checkpoint_at_end,
            "keep_checkpoints_num": keep_checkpoints_num,
            "checkpoint_score_attr": checkpoint_score_attr,
            "export_formats": export_formats or [],
            "max_failures": max_failures,
            "restore": os.path.abspath(os.path.expanduser(restore))
            if restore
            else None,
        }
        self.spec = spec

    @classmethod
    def from_json(cls, name: str, spec: dict):
        """Generates an Experiment object from JSON.

        Args:
            name: Name of Experiment.
            spec: JSON configuration of experiment.
        """
        if "run" not in spec:
            raise TuneError("No trainable specified!")

        # Special case the `env` param for RLlib by automatically
        # moving it into the `config` section.
        if "env" in spec:
            spec["config"] = spec.get("config", {})
            spec["config"]["env"] = spec["env"]
            del spec["env"]

        if "sync_config" in spec and isinstance(spec["sync_config"], dict):
            spec["sync_config"] = SyncConfig(**spec["sync_config"])

        spec = copy.deepcopy(spec)

        run_value = spec.pop("run")
        try:
            exp = cls(name, run_value, **spec)
        except TypeError:
            raise TuneError("Improper argument from JSON: {}.".format(spec))
        return exp

    @classmethod
    def get_trainable_name(cls, run_object: Union[str, Callable, Type]):
        """Get Trainable name.

        Args:
            run_object: Trainable to run. If string,
                assumes it is an ID and does not modify it. Otherwise,
                returns a string corresponding to the run_object name.

        Returns:
            A string representing the trainable identifier.

        Raises:
            TuneError: if ``run_object`` passed in is invalid.
        """
        from ray.tune.search.sample import Domain

        if isinstance(run_object, str) or isinstance(run_object, Domain):
            return run_object
        elif isinstance(run_object, type) or callable(run_object):
            name = "DEFAULT"
            if hasattr(run_object, "_name"):
                name = run_object._name
            elif hasattr(run_object, "__name__"):
                fn_name = run_object.__name__
                if fn_name == "<lambda>":
                    name = "lambda"
                elif fn_name.startswith("<"):
                    name = "DEFAULT"
                else:
                    name = fn_name
            elif (
                isinstance(run_object, partial)
                and hasattr(run_object, "func")
                and hasattr(run_object.func, "__name__")
            ):
                name = run_object.func.__name__
            else:
                logger.warning("No name detected on trainable. Using {}.".format(name))
            return name
        else:
            raise TuneError("Improper 'run' - not string nor trainable.")

    @classmethod
    def register_if_needed(cls, run_object: Union[str, Callable, Type]):
        """Registers Trainable or Function at runtime.

        Assumes already registered if run_object is a string.
        Also, does not inspect interface of given run_object.

        Args:
            run_object: Trainable to run. If string,
                assumes it is an ID and does not modify it. Otherwise,
                returns a string corresponding to the run_object name.

        Returns:
            A string representing the trainable identifier.
        """
        from ray.tune.search.sample import Domain

        if isinstance(run_object, str):
            return run_object
        elif isinstance(run_object, Domain):
            logger.warning("Not registering trainable. Resolving as variant.")
            return run_object
        name = cls.get_trainable_name(run_object)
        try:
            register_trainable(name, run_object)
        except (TypeError, PicklingError) as e:
            extra_msg = (
                "Other options: "
                "\n-Try reproducing the issue by calling "
                "`pickle.dumps(trainable)`. "
                "\n-If the error is typing-related, try removing "
                "the type annotations and try again."
            )
            raise type(e)(str(e) + " " + extra_msg) from None
        return name

    @classmethod
    def get_experiment_checkpoint_dir(
        cls,
        run_obj: Union[str, Callable, Type],
        local_dir: Optional[str] = None,
        name: Optional[str] = None,
    ):
        """Get experiment checkpoint dir without setting up an experiment.

        This is only used internally for better support of Tuner API.

        Args:
            run_obj: Trainable to run.
            local_dir: The local_dir path.
            name: The name of the experiment specified by user.

        Returns:
            Checkpoint directory for experiment.
        """
        assert run_obj
        local_dir = _get_local_dir_with_expand_user(local_dir)
        run_identifier = cls.get_trainable_name(run_obj)
        combined_name = name or run_identifier

        dir_name = _get_dir_name(run_obj, name, combined_name)

        return os.path.join(local_dir, dir_name)

    @property
    def stopper(self):
        return self._stopper

    @property
    def local_dir(self):
        return self.spec.get("local_dir")

    @property
    def checkpoint_dir(self):
        # Provided when initializing Experiment, if so, return directly.
        if self._experiment_checkpoint_dir:
            return self._experiment_checkpoint_dir
        assert self.local_dir
        return os.path.join(self.local_dir, self.dir_name)

    @property
    def run_identifier(self):
        """Returns a string representing the trainable identifier."""
        return self._run_identifier

    @property
    def public_spec(self) -> Dict[str, Any]:
        """Returns the spec dict with only the public-facing keys.

        Intended to be used for passing information to callbacks,
        Searchers and Schedulers.
        """
        return {k: v for k, v in self.spec.items() if k in self.PUBLIC_KEYS}


def convert_to_experiment_list(experiments: Union[Experiment, List[Experiment], Dict]):
    """Produces a list of Experiment objects.

    Converts input from dict, single experiment, or list of
    experiments to list of experiments. If input is None,
    will return an empty list.

    Arguments:
        experiments: Experiments to run.

    Returns:
        List of experiments.
    """
    exp_list = experiments

    # Transform list if necessary
    if experiments is None:
        exp_list = []
    elif isinstance(experiments, Experiment):
        exp_list = [experiments]
    elif type(experiments) is dict:
        exp_list = [
            Experiment.from_json(name, spec) for name, spec in experiments.items()
        ]

    # Validate exp_list
    if type(exp_list) is list and all(isinstance(exp, Experiment) for exp in exp_list):
        if len(exp_list) > 1:
            logger.info(
                "Running with multiple concurrent experiments. "
                "All experiments will be using the same SearchAlgorithm."
            )
    else:
        raise TuneError("Invalid argument: {}".format(experiments))

    return exp_list
