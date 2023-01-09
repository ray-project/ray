import enum
import os
import pickle
import urllib
import warnings

import numpy as np
from numbers import Number

from types import ModuleType
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import ray
from ray import logger
from ray.air import session

from ray.tune.logger import LoggerCallback
from ray.tune.utils import flatten_dict
from ray.tune.experiment import Trial

from ray._private.storage import _load_class
from ray.tune.utils.node import _force_on_current_node
from ray.util import PublicAPI
from ray.util.queue import Queue

try:
    import wandb
    from wandb.util import json_dumps_safer
    from wandb.wandb_run import Run
    from wandb.sdk.lib.disabled import RunDisabled
    from wandb.sdk.data_types.base_types.wb_value import WBValue
except ImportError:
    logger.error("pip install 'wandb' to use WandbLoggerCallback/WandbTrainableMixin.")
    wandb = json_dumps_safer = Run = RunDisabled = WBValue = None


WANDB_ENV_VAR = "WANDB_API_KEY"
WANDB_PROJECT_ENV_VAR = "WANDB_PROJECT_NAME"
WANDB_GROUP_ENV_VAR = "WANDB_GROUP_NAME"
# Hook that is invoked before wandb.init in the setup method of WandbLoggerCallback
# to populate the API key if it isn't already set when initializing the callback.
# It doesn't take in any arguments and returns the W&B API key.
# Example: "your.module.wandb_setup_api_key_hook".
WANDB_SETUP_API_KEY_HOOK = "WANDB_SETUP_API_KEY_HOOK"
# Hook that is invoked before wandb.init in the setup method of WandbLoggerCallback
# to populate environment variables to specify the location
# (project and group) of the W&B run.
# It doesn't take in any arguments and doesn't return anything, but it does populate
# WANDB_PROJECT_NAME and WANDB_GROUP_NAME.
# Example: "your.module.wandb_populate_run_location_hook".
WANDB_POPULATE_RUN_LOCATION_HOOK = "WANDB_POPULATE_RUN_LOCATION_HOOK"
# Hook that is invoked after running wandb.init in WandbLoggerCallback
# to process information about the W&B run.
# It takes in a W&B run object and doesn't return anything.
# Example: "your.module.wandb_process_run_info_hook".
WANDB_PROCESS_RUN_INFO_HOOK = "WANDB_PROCESS_RUN_INFO_HOOK"


@PublicAPI(stability="alpha")
def setup_wandb(
    config: Optional[Dict] = None,
    api_key: Optional[str] = None,
    api_key_file: Optional[str] = None,
    rank_zero_only: bool = True,
    **kwargs,
) -> Union[Run, RunDisabled]:
    """Set up a Weights & Biases session.

    This function can be used to initialize a Weights & Biases session in a
    (distributed) training or tuning run.

    By default, the run ID is the trial ID, the run name is the trial name, and
    the run group is the experiment name. These settings can be overwritten by
    passing the respective arguments as ``kwargs``, which will be passed to
    ``wandb.init()``.

    In distributed training with Ray Train, only the zero-rank worker will initialize
    wandb. All other workers will return a disabled run object, so that logging is not
    duplicated in a distributed run. This can be disabled by passing
    ``rank_zero_only=False``, which will then initialize wandb in every training
    worker.

    The ``config`` argument will be passed to Weights and Biases and will be logged
    as the run configuration.

    If no API key or key file are passed, wandb will try to authenticate
    using locally stored credentials, created for instance by running ``wandb login``.

    Keyword arguments passed to ``setup_wandb()`` will be passed to
    ``wandb.init()`` and take precedence over any potential default settings.

    Args:
        config: Configuration dict to be logged to Weights and Biases. Can contain
            arguments for ``wandb.init()`` as well as authentication information.
        api_key: API key to use for authentication with Weights and Biases.
        api_key_file: File pointing to API key for with Weights and Biases.
        rank_zero_only: If True, will return an initialized session only for the
            rank 0 worker in distributed training. If False, will initialize a
            session for all workers.
        kwargs: Passed to ``wandb.init()``.

    Example:

        .. code-block: python

            from ray.air.integrations.wandb import wandb_setup

            def training_loop(config):
                wandb = wandb_setup(config)
                # ...
                wandb.log({"loss": 0.123})

    """
    if not wandb:
        raise RuntimeError(
            "Wandb was not found - please install with `pip install wandb`"
        )

    try:
        # Do a try-catch here if we are not in a train session
        _session = session._get_session(warn=False)
        if _session and rank_zero_only and session.get_world_rank() != 0:
            return RunDisabled()

        default_trial_id = session.get_trial_id()
        default_trial_name = session.get_trial_name()
        default_experiment_name = session.get_experiment_name()

    except RuntimeError:
        default_trial_id = None
        default_trial_name = None
        default_experiment_name = None

    # Default init kwargs
    wandb_init_kwargs = {
        "trial_id": kwargs.get("trial_id") or default_trial_id,
        "trial_name": kwargs.get("trial_name") or default_trial_name,
        "group": kwargs.get("group") or default_experiment_name,
    }
    # Passed kwargs take precedence over default kwargs
    wandb_init_kwargs.update(kwargs)

    return _setup_wandb(
        config=config, api_key=api_key, api_key_file=api_key_file, **wandb_init_kwargs
    )


def _setup_wandb(
    trial_id: str,
    trial_name: str,
    config: Optional[Dict] = None,
    api_key: Optional[str] = None,
    api_key_file: Optional[str] = None,
    _wandb: Optional[ModuleType] = None,
    **kwargs,
) -> Union[Run, RunDisabled]:
    _config = config.copy() if config else {}

    wandb_config = _config.pop("wandb", {}).copy()

    # Deprecate: 2.4
    if wandb_config:
        warnings.warn(
            "Passing a `wandb` key in the config dict is deprecated and will raise an "
            "error in the future. Please pass the actual arguments to `setup_wandb()` "
            "instead.",
            DeprecationWarning,
        )

    # If key file is specified, set
    api_key_file = api_key_file or wandb_config.pop("api_key_file", None)
    if api_key_file:
        api_key_file = os.path.expanduser(api_key_file)

    _set_api_key(api_key_file, api_key or wandb_config.pop("api_key", None))
    wandb_config["project"] = _get_wandb_project(wandb_config.get("project"))
    wandb_config["group"] = (
        os.environ.get(WANDB_GROUP_ENV_VAR)
        if (not wandb_config.get("group") and os.environ.get(WANDB_GROUP_ENV_VAR))
        else wandb_config.get("group")
    )

    # remove unpickleable items
    _config = _clean_log(_config)

    wandb_init_kwargs = dict(
        id=trial_id,
        name=trial_name,
        resume=True,
        reinit=True,
        allow_val_change=True,
        config=_config,
    )

    # Update config (e.g.g set group, project, override other settings)
    wandb_init_kwargs.update(wandb_config)
    wandb_init_kwargs.update(**kwargs)

    # On windows, we can't fork
    if os.name == "nt":
        os.environ["WANDB_START_METHOD"] = "thread"
    else:
        os.environ["WANDB_START_METHOD"] = "fork"

    _wandb = _wandb or wandb

    run = _wandb.init(**wandb_init_kwargs)
    _run_wandb_process_run_info_hook(run)
    return run


def _is_allowed_type(obj):
    """Return True if type is allowed for logging to wandb"""
    if isinstance(obj, np.ndarray) and obj.size == 1:
        return isinstance(obj.item(), Number)
    if isinstance(obj, Sequence) and len(obj) > 0:
        return isinstance(obj[0], WBValue)
    return isinstance(obj, (Number, WBValue))


def _clean_log(obj: Any):
    # Fixes https://github.com/ray-project/ray/issues/10631
    if isinstance(obj, dict):
        return {k: _clean_log(v) for k, v in obj.items()}
    elif isinstance(obj, (list, set)):
        return [_clean_log(v) for v in obj]
    elif isinstance(obj, tuple):
        return tuple(_clean_log(v) for v in obj)
    elif _is_allowed_type(obj):
        return obj

    # Else

    try:
        # This is what wandb uses internally. If we cannot dump
        # an object using this method, wandb will raise an exception.
        json_dumps_safer(obj)

        # This is probably unnecessary, but left here to be extra sure.
        pickle.dumps(obj)

        return obj
    except Exception:
        # give up, similar to _SafeFallBackEncoder
        fallback = str(obj)

        # Try to convert to int
        try:
            fallback = int(fallback)
            return fallback
        except ValueError:
            pass

        # Try to convert to float
        try:
            fallback = float(fallback)
            return fallback
        except ValueError:
            pass

        # Else, return string
        return fallback


def _get_wandb_project(project: Optional[str] = None) -> Optional[str]:
    """Get W&B project from environment variable or external hook if not passed
    as and argument."""
    if (
        not project
        and not os.environ.get(WANDB_PROJECT_ENV_VAR)
        and os.environ.get(WANDB_POPULATE_RUN_LOCATION_HOOK)
    ):
        # Try to populate WANDB_PROJECT_ENV_VAR and WANDB_GROUP_ENV_VAR
        # from external hook
        try:
            _load_class(os.environ[WANDB_POPULATE_RUN_LOCATION_HOOK])()
        except Exception as e:
            logger.exception(
                f"Error executing {WANDB_POPULATE_RUN_LOCATION_HOOK} to "
                f"populate {WANDB_PROJECT_ENV_VAR} and {WANDB_GROUP_ENV_VAR}: {e}",
                exc_info=e,
            )
    if not project and os.environ.get(WANDB_PROJECT_ENV_VAR):
        # Try to get project and group from environment variables if not
        # passed through WandbLoggerCallback.
        project = os.environ.get(WANDB_PROJECT_ENV_VAR)
    return project


def _set_api_key(api_key_file: Optional[str] = None, api_key: Optional[str] = None):
    """Set WandB API key from `wandb_config`. Will pop the
    `api_key_file` and `api_key` keys from `wandb_config` parameter"""
    if api_key_file:
        if api_key:
            raise ValueError("Both WandB `api_key_file` and `api_key` set.")
        with open(api_key_file, "rt") as fp:
            api_key = fp.readline().strip()
    # Try to get API key from external hook
    if not api_key and WANDB_SETUP_API_KEY_HOOK in os.environ:
        try:
            api_key = _load_class(os.environ[WANDB_SETUP_API_KEY_HOOK])()
        except Exception as e:
            logger.exception(
                f"Error executing {WANDB_SETUP_API_KEY_HOOK} to setup API key: {e}",
                exc_info=e,
            )
    if api_key:
        os.environ[WANDB_ENV_VAR] = api_key
    elif not os.environ.get(WANDB_ENV_VAR):
        try:
            # Check if user is already logged into wandb.
            wandb.ensure_configured()
            if wandb.api.api_key:
                logger.info("Already logged into W&B.")
                return
        except AttributeError:
            pass
        raise ValueError(
            "No WandB API key found. Either set the {} environment "
            "variable, pass `api_key` or `api_key_file` to the"
            "`WandbLoggerCallback` class as arguments, "
            "or run `wandb login` from the command line".format(WANDB_ENV_VAR)
        )


def _run_wandb_process_run_info_hook(run: Any) -> None:
    """Run external hook to process information about wandb run"""
    if WANDB_PROCESS_RUN_INFO_HOOK in os.environ:
        try:
            _load_class(os.environ[WANDB_PROCESS_RUN_INFO_HOOK])(run)
        except Exception as e:
            logger.exception(
                f"Error calling {WANDB_PROCESS_RUN_INFO_HOOK}: {e}", exc_info=e
            )


class _QueueItem(enum.Enum):
    END = enum.auto()
    RESULT = enum.auto()
    CHECKPOINT = enum.auto()


class _WandbLoggingActor:
    """
    We need a separate process to allow multiple concurrent
    wandb logging instances locally. We use Ray actors as forking multiprocessing
    processes is not supported by Ray and spawn processes run into pickling
    problems.

    We use a queue for the driver to communicate with the logging process.
    The queue accepts the following items:

    - If it's a dict, it is assumed to be a result and will be logged using
      ``wandb.log()``
    - If it's a checkpoint object, it will be saved using ``wandb.log_artifact()``.
    """

    def __init__(
        self,
        logdir: str,
        queue: Queue,
        exclude: List[str],
        to_config: List[str],
        *args,
        **kwargs,
    ):
        import wandb

        self._wandb = wandb

        os.chdir(logdir)
        self.queue = queue
        self._exclude = set(exclude)
        self._to_config = set(to_config)
        self.args = args
        self.kwargs = kwargs

        self._trial_name = self.kwargs.get("name", "unknown")
        self._logdir = logdir

    def run(self):
        # Since we're running in a separate process already, use threads.
        os.environ["WANDB_START_METHOD"] = "thread"
        run = self._wandb.init(*self.args, **self.kwargs)
        run.config.trial_log_path = self._logdir

        _run_wandb_process_run_info_hook(run)

        while True:
            item_type, item_content = self.queue.get()
            if item_type == _QueueItem.END:
                break

            if item_type == _QueueItem.CHECKPOINT:
                self._handle_checkpoint(item_content)
                continue

            assert item_type == _QueueItem.RESULT
            log, config_update = self._handle_result(item_content)
            try:
                self._wandb.config.update(config_update, allow_val_change=True)
                self._wandb.log(log)
            except urllib.error.HTTPError as e:
                # Ignore HTTPError. Missing a few data points is not a
                # big issue, as long as things eventually recover.
                logger.warn("Failed to log result to w&b: {}".format(str(e)))
        self._wandb.finish()

    def _handle_checkpoint(self, checkpoint_path: str):
        artifact = self._wandb.Artifact(
            name=f"checkpoint_{self._trial_name}", type="model"
        )
        artifact.add_dir(checkpoint_path)
        self._wandb.log_artifact(artifact)

    def _handle_result(self, result: Dict) -> Tuple[Dict, Dict]:
        config_update = result.get("config", {}).copy()
        log = {}
        flat_result = flatten_dict(result, delimiter="/")

        for k, v in flat_result.items():
            if any(k.startswith(item + "/") or k == item for item in self._to_config):
                config_update[k] = v
            elif any(k.startswith(item + "/") or k == item for item in self._exclude):
                continue
            elif not _is_allowed_type(v):
                continue
            else:
                log[k] = v

        config_update.pop("callbacks", None)  # Remove callbacks
        return log, config_update


class WandbLoggerCallback(LoggerCallback):
    """WandbLoggerCallback

    Weights and biases (https://www.wandb.ai/) is a tool for experiment
    tracking, model optimization, and dataset versioning. This Ray Tune
    ``LoggerCallback`` sends metrics to Wandb for automatic tracking and
    visualization.

    Args:
        project: Name of the Wandb project. Mandatory.
        group: Name of the Wandb group. Defaults to the trainable
            name.
        api_key_file: Path to file containing the Wandb API KEY. This
            file only needs to be present on the node running the Tune script
            if using the WandbLogger.
        api_key: Wandb API Key. Alternative to setting ``api_key_file``.
        excludes: List of metrics that should be excluded from
            the log.
        log_config: Boolean indicating if the ``config`` parameter of
            the ``results`` dict should be logged. This makes sense if
            parameters will change during training, e.g. with
            PopulationBasedTraining. Defaults to False.
        save_checkpoints: If ``True``, model checkpoints will be saved to
            Wandb as artifacts. Defaults to ``False``.
        **kwargs: The keyword arguments will be pased to ``wandb.init()``.

    Wandb's ``group``, ``run_id`` and ``run_name`` are automatically selected
    by Tune, but can be overwritten by filling out the respective configuration
    values.

    Please see here for all other valid configuration settings:
    https://docs.wandb.ai/library/init

    Example:

    .. code-block:: python

        from ray.tune.logger import DEFAULT_LOGGERS
        from ray.air.integrations.wandb import WandbLoggerCallback
        tune.run(
            train_fn,
            config={
                # define search space here
                "parameter_1": tune.choice([1, 2, 3]),
                "parameter_2": tune.choice([4, 5, 6]),
            },
            callbacks=[WandbLoggerCallback(
                project="Optimization_Project",
                api_key_file="/path/to/file",
                log_config=True)])

    """

    # Do not log these result keys
    _exclude_results = ["done", "should_checkpoint"]

    # Use these result keys to update `wandb.config`
    _config_results = [
        "trial_id",
        "experiment_tag",
        "node_ip",
        "experiment_id",
        "hostname",
        "pid",
        "date",
    ]

    _logger_actor_cls = _WandbLoggingActor

    def __init__(
        self,
        project: Optional[str] = None,
        group: Optional[str] = None,
        api_key_file: Optional[str] = None,
        api_key: Optional[str] = None,
        excludes: Optional[List[str]] = None,
        log_config: bool = False,
        save_checkpoints: bool = False,
        **kwargs,
    ):
        self.project = project
        self.group = group
        self.api_key_path = api_key_file
        self.api_key = api_key
        self.excludes = excludes or []
        self.log_config = log_config
        self.save_checkpoints = save_checkpoints
        self.kwargs = kwargs

        self._remote_logger_class = None

        self._trial_logging_actors: Dict[
            "Trial", ray.actor.ActorHandle[_WandbLoggingActor]
        ] = {}
        self._trial_logging_futures: Dict["Trial", ray.ObjectRef] = {}
        self._trial_queues: Dict["Trial", Queue] = {}

    def setup(self, *args, **kwargs):
        self.api_key_file = (
            os.path.expanduser(self.api_key_path) if self.api_key_path else None
        )
        _set_api_key(self.api_key_file, self.api_key)

        self.project = _get_wandb_project(self.project)
        if not self.project:
            raise ValueError(
                "Please pass the project name as argument or through "
                f"the {WANDB_PROJECT_ENV_VAR} environment variable."
            )
        if not self.group and os.environ.get(WANDB_GROUP_ENV_VAR):
            self.group = os.environ.get(WANDB_GROUP_ENV_VAR)

    def log_trial_start(self, trial: "Trial"):
        config = trial.config.copy()

        config.pop("callbacks", None)  # Remove callbacks

        exclude_results = self._exclude_results.copy()

        # Additional excludes
        exclude_results += self.excludes

        # Log config keys on each result?
        if not self.log_config:
            exclude_results += ["config"]

        # Fill trial ID and name
        trial_id = trial.trial_id if trial else None
        trial_name = str(trial) if trial else None

        # Project name for Wandb
        wandb_project = self.project

        # Grouping
        wandb_group = self.group or trial.experiment_dir_name if trial else None

        # remove unpickleable items!
        config = _clean_log(config)

        wandb_init_kwargs = dict(
            id=trial_id,
            name=trial_name,
            resume=False,
            reinit=True,
            allow_val_change=True,
            group=wandb_group,
            project=wandb_project,
            config=config,
        )
        wandb_init_kwargs.update(self.kwargs)

        self._start_logging_actor(trial, exclude_results, **wandb_init_kwargs)

    def _start_logging_actor(
        self, trial: "Trial", exclude_results: List[str], **wandb_init_kwargs
    ):
        if not self._remote_logger_class:
            env_vars = {}
            # API key env variable is not set if authenticating through `wandb login`
            if WANDB_ENV_VAR in os.environ:
                env_vars[WANDB_ENV_VAR] = os.environ[WANDB_ENV_VAR]
            self._remote_logger_class = ray.remote(
                num_cpus=0,
                **_force_on_current_node(),
                runtime_env={"env_vars": env_vars},
            )(self._logger_actor_cls)

        self._trial_queues[trial] = Queue(
            actor_options={"num_cpus": 0, **_force_on_current_node()}
        )
        self._trial_logging_actors[trial] = self._remote_logger_class.remote(
            logdir=trial.logdir,
            queue=self._trial_queues[trial],
            exclude=exclude_results,
            to_config=self._config_results,
            **wandb_init_kwargs,
        )
        self._trial_logging_futures[trial] = self._trial_logging_actors[
            trial
        ].run.remote()

    def _stop_logging_actor(self, trial: "Trial", timeout: int = 10):
        self._trial_queues[trial].put((_QueueItem.END, None))

        try:
            ray.get(self._trial_logging_futures[trial], timeout=timeout)
        except TimeoutError:
            ray.kill(self._trial_logging_actors[trial])

        del self._trial_queues[trial]
        del self._trial_logging_actors[trial]
        del self._trial_logging_futures[trial]

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        if trial not in self._trial_logging_actors:
            self.log_trial_start(trial)

        result = _clean_log(result)
        self._trial_queues[trial].put((_QueueItem.RESULT, result))

    def log_trial_save(self, trial: "Trial"):
        if self.save_checkpoints and trial.checkpoint:
            self._trial_queues[trial].put(
                (_QueueItem.CHECKPOINT, trial.checkpoint.dir_or_data)
            )

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        self._stop_logging_actor(trial=trial, timeout=10)

    def __del__(self):
        for trial in list(self._trial_logging_actors):
            self._stop_logging_actor(trial=trial, timeout=2)

        self._trial_logging_actors = {}
        self._trial_logging_futures = {}
        self._trial_queues = {}
