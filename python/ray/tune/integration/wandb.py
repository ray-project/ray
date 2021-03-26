import os
import pickle
from collections.abc import Iterable
from multiprocessing import Process, Queue
from numbers import Number
from typing import Any, Callable, Dict, List, Optional, Tuple
import numpy as np

from ray import logger
from ray.tune import Trainable
from ray.tune.function_runner import FunctionRunner
from ray.tune.logger import LoggerCallback, Logger
from ray.tune.utils import flatten_dict
from ray.tune.trial import Trial

import yaml

try:
    import wandb
except ImportError:
    logger.error("pip install 'wandb' to use WandbLogger/WandbTrainableMixin.")
    wandb = None

WANDB_ENV_VAR = "WANDB_API_KEY"
_WANDB_QUEUE_END = (None, )
_VALID_TYPES = (Number, wandb.data_types.Video)
_VALID_ITERABLE_TYPES = (wandb.data_types.Video)


def _is_allowed_type(obj):
    """Return True if type is allowed for logging to wandb"""
    if isinstance(obj, np.ndarray) and obj.size == 1:
        return isinstance(obj.item(), Number)
    if isinstance(obj, Iterable) and len(obj) > 0:
        return isinstance(obj[0], _VALID_ITERABLE_TYPES)
    return isinstance(obj, _VALID_TYPES)


def _clean_log(obj: Any):
    # Fixes https://github.com/ray-project/ray/issues/10631
    if isinstance(obj, dict):
        return {k: _clean_log(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_clean_log(v) for v in obj]
    elif _is_allowed_type(obj):
        return obj

    # Else
    try:
        pickle.dumps(obj)
        yaml.dump(
            obj,
            Dumper=yaml.SafeDumper,
            default_flow_style=False,
            allow_unicode=True,
            encoding="utf-8")
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


def wandb_mixin(func: Callable):
    """wandb_mixin

    Weights and biases (https://www.wandb.ai/) is a tool for experiment
    tracking, model optimization, and dataset versioning. This Ray Tune
    Trainable mixin helps initializing the Wandb API for use with the
    ``Trainable`` class or with `@wandb_mixin` for the function API.

    For basic usage, just prepend your training function with the
    ``@wandb_mixin`` decorator:

    .. code-block:: python

        from ray.tune.integration.wandb import wandb_mixin

        @wandb_mixin
        def train_fn(config):
            wandb.log()


    Wandb configuration is done by passing a ``wandb`` key to
    the ``config`` parameter of ``tune.run()`` (see example below).

    The content of the ``wandb`` config entry is passed to ``wandb.init()``
    as keyword arguments. The exception are the following settings, which
    are used to configure the ``WandbTrainableMixin`` itself:

    Args:
        api_key_file (str): Path to file containing the Wandb API KEY. This
            file must be on all nodes if using the `wandb_mixin`.
        api_key (str): Wandb API Key. Alternative to setting `api_key_file`.

    Wandb's ``group``, ``run_id`` and ``run_name`` are automatically selected
    by Tune, but can be overwritten by filling out the respective configuration
    values.

    Please see here for all other valid configuration settings:
    https://docs.wandb.ai/library/init

    Example:

    .. code-block:: python

        from ray import tune
        from ray.tune.integration.wandb import wandb_mixin

        @wandb_mixin
        def train_fn(config):
            for i in range(10):
                loss = self.config["a"] + self.config["b"]
                wandb.log({"loss": loss})
            tune.report(loss=loss, done=True)

        tune.run(
            train_fn,
            config={
                # define search space here
                "a": tune.choice([1, 2, 3]),
                "b": tune.choice([4, 5, 6]),
                # wandb configuration
                "wandb": {
                    "project": "Optimization_Project",
                    "api_key_file": "/path/to/file"
                }
            })

    """
    if hasattr(func, "__mixins__"):
        func.__mixins__ = func.__mixins__ + (WandbTrainableMixin, )
    else:
        func.__mixins__ = (WandbTrainableMixin, )
    return func


def _set_api_key(api_key_file: Optional[str] = None,
                 api_key: Optional[str] = None):
    """Set WandB API key from `wandb_config`. Will pop the
    `api_key_file` and `api_key` keys from `wandb_config` parameter"""
    if api_key_file:
        if api_key:
            raise ValueError("Both WandB `api_key_file` and `api_key` set.")
        with open(api_key_file, "rt") as fp:
            api_key = fp.readline().strip()
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
            "or run `wandb login` from the command line".format(WANDB_ENV_VAR))


class _WandbLoggingProcess(Process):
    """
    We need a `multiprocessing.Process` to allow multiple concurrent
    wandb logging instances locally.
    """

    def __init__(self, queue: Queue, exclude: List[str], to_config: List[str],
                 *args, **kwargs):
        super(_WandbLoggingProcess, self).__init__()
        self.queue = queue
        self._exclude = set(exclude)
        self._to_config = set(to_config)
        self.args = args
        self.kwargs = kwargs

    def run(self):
        wandb.init(*self.args, **self.kwargs)
        while True:
            result = self.queue.get()
            if result == _WANDB_QUEUE_END:
                break
            log, config_update = self._handle_result(result)
            wandb.config.update(config_update, allow_val_change=True)
            wandb.log(log)
        wandb.join()

    def _handle_result(self, result: Dict) -> Tuple[Dict, Dict]:
        config_update = result.get("config", {}).copy()
        log = {}
        flat_result = flatten_dict(result, delimiter="/")

        for k, v in flat_result.items():
            if any(
                    k.startswith(item + "/") or k == item
                    for item in self._to_config):
                config_update[k] = v
            elif any(
                    k.startswith(item + "/") or k == item
                    for item in self._exclude):
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
        project (str): Name of the Wandb project. Mandatory.
        group (str): Name of the Wandb group. Defaults to the trainable
            name.
        api_key_file (str): Path to file containing the Wandb API KEY. This
            file only needs to be present on the node running the Tune script
            if using the WandbLogger.
        api_key (str): Wandb API Key. Alternative to setting ``api_key_file``.
        excludes (list): List of metrics that should be excluded from
            the log.
        log_config (bool): Boolean indicating if the ``config`` parameter of
            the ``results`` dict should be logged. This makes sense if
            parameters will change during training, e.g. with
            PopulationBasedTraining. Defaults to False.
        **kwargs: The keyword arguments will be pased to ``wandb.init()``.

    Wandb's ``group``, ``run_id`` and ``run_name`` are automatically selected
    by Tune, but can be overwritten by filling out the respective configuration
    values.

    Please see here for all other valid configuration settings:
    https://docs.wandb.ai/library/init

    Example:

    .. code-block:: python

        from ray.tune.logger import DEFAULT_LOGGERS
        from ray.tune.integration.wandb import WandbLoggerCallback
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
        "trial_id", "experiment_tag", "node_ip", "experiment_id", "hostname",
        "pid", "date"
    ]

    _logger_process_cls = _WandbLoggingProcess

    def __init__(self,
                 project: str,
                 group: Optional[str] = None,
                 api_key_file: Optional[str] = None,
                 api_key: Optional[str] = None,
                 excludes: Optional[List[str]] = None,
                 log_config: bool = False,
                 **kwargs):
        self.project = project
        self.group = group
        self.api_key_file = os.path.expanduser(
            api_key_file) if api_key_file else None
        self.api_key = api_key
        self.excludes = excludes or []
        self.log_config = log_config
        self.kwargs = kwargs

        self._trial_processes: Dict["Trial", _WandbLoggingProcess] = {}
        self._trial_queues: Dict["Trial", Queue] = {}

    def setup(self):
        _set_api_key(self.api_key_file, self.api_key)

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
        wandb_group = self.group or trial.trainable_name if trial else None

        # remove unpickleable items!
        config = _clean_log(config)

        wandb_init_kwargs = dict(
            id=trial_id,
            name=trial_name,
            resume=True,
            reinit=True,
            allow_val_change=True,
            group=wandb_group,
            project=wandb_project,
            config=config)
        wandb_init_kwargs.update(self.kwargs)

        self._trial_queues[trial] = Queue()
        self._trial_processes[trial] = self._logger_process_cls(
            queue=self._trial_queues[trial],
            exclude=exclude_results,
            to_config=self._config_results,
            **wandb_init_kwargs)
        self._trial_processes[trial].start()

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        if trial not in self._trial_processes:
            self.log_trial_start(trial)

        result = _clean_log(result)
        self._trial_queues[trial].put(result)

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        self._trial_queues[trial].put(_WANDB_QUEUE_END)
        self._trial_processes[trial].join(timeout=10)

        del self._trial_queues[trial]
        del self._trial_processes[trial]

    def __del__(self):
        for trial in self._trial_processes:
            if trial in self._trial_queues:
                self._trial_queues[trial].put(_WANDB_QUEUE_END)
                del self._trial_queues[trial]
            self._trial_processes[trial].join(timeout=2)
            del self._trial_processes[trial]


class WandbLogger(Logger):
    """WandbLogger

    .. warning::
        This `Logger` class is deprecated. Use the `WandbLoggerCallback`
        callback instead.

    Weights and biases (https://www.wandb.ai/) is a tool for experiment
    tracking, model optimization, and dataset versioning. This Ray Tune
    ``Logger`` sends metrics to Wandb for automatic tracking and
    visualization.

    Wandb configuration is done by passing a ``wandb`` key to
    the ``config`` parameter of ``tune.run()`` (see example below).

    The ``wandb`` config key can be optionally included in the
    ``logger_config`` subkey of ``config`` to be compatible with RLLib
    trainables (see second example below).

    The content of the ``wandb`` config entry is passed to ``wandb.init()``
    as keyword arguments. The exception are the following settings, which
    are used to configure the WandbLogger itself:

    Args:
        api_key_file (str): Path to file containing the Wandb API KEY. This
            file only needs to be present on the node running the Tune script
            if using the WandbLogger.
        api_key (str): Wandb API Key. Alternative to setting ``api_key_file``.
        excludes (list): List of metrics that should be excluded from
            the log.
        log_config (bool): Boolean indicating if the ``config`` parameter of
            the ``results`` dict should be logged. This makes sense if
            parameters will change during training, e.g. with
            PopulationBasedTraining. Defaults to False.

    Wandb's ``group``, ``run_id`` and ``run_name`` are automatically selected
    by Tune, but can be overwritten by filling out the respective configuration
    values.

    Please see here for all other valid configuration settings:
    https://docs.wandb.ai/library/init

    Example:

    .. code-block:: python

        from ray.tune.logger import DEFAULT_LOGGERS
        from ray.tune.integration.wandb import WandbLogger
        tune.run(
            train_fn,
            config={
                # define search space here
                "parameter_1": tune.choice([1, 2, 3]),
                "parameter_2": tune.choice([4, 5, 6]),
                # wandb configuration
                "wandb": {
                    "project": "Optimization_Project",
                    "api_key_file": "/path/to/file",
                    "log_config": True
                }
            },
            loggers=DEFAULT_LOGGERS + (WandbLogger, ))

    Example for RLLib:

    .. code-block :: python

        from ray import tune
        from ray.tune.integration.wandb import WandbLogger

        tune.run(
            "PPO",
            config={
                "env": "CartPole-v0",
                "logger_config": {
                    "wandb": {
                        "project": "PPO",
                        "api_key_file": "~/.wandb_api_key"
                    }
                }
            },
            loggers=[WandbLogger])


    """
    _experiment_logger_cls = WandbLoggerCallback

    def _init(self):
        config = self.config.copy()
        config.pop("callbacks", None)  # Remove callbacks

        try:
            if config.get("logger_config", {}).get("wandb"):
                logger_config = config.pop("logger_config")
                wandb_config = logger_config.get("wandb").copy()
            else:
                wandb_config = config.pop("wandb").copy()
        except KeyError:
            raise ValueError(
                "Wandb logger specified but no configuration has been passed. "
                "Make sure to include a `wandb` key in your `config` dict "
                "containing at least a `project` specification.")

        self._trial_experiment_logger = self._experiment_logger_cls(
            **wandb_config)
        self._trial_experiment_logger.setup()
        self._trial_experiment_logger.log_trial_start(self.trial)

    def on_result(self, result: Dict):
        self._trial_experiment_logger.log_trial_result(0, self.trial, result)

    def close(self):
        self._trial_experiment_logger.log_trial_end(self.trial, failed=False)
        del self._trial_experiment_logger


class WandbTrainableMixin:
    _wandb = wandb

    def __init__(self, config: Dict, *args, **kwargs):
        if not isinstance(self, Trainable):
            raise ValueError(
                "The `WandbTrainableMixin` can only be used as a mixin "
                "for `tune.Trainable` classes. Please make sure your "
                "class inherits from both. For example: "
                "`class YourTrainable(WandbTrainableMixin)`.")

        super().__init__(config, *args, **kwargs)

        _config = config.copy()

        try:
            wandb_config = _config.pop("wandb").copy()
        except KeyError:
            raise ValueError(
                "Wandb mixin specified but no configuration has been passed. "
                "Make sure to include a `wandb` key in your `config` dict "
                "containing at least a `project` specification.")

        api_key_file = wandb_config.pop("api_key_file", None)
        if api_key_file:
            api_key_file = os.path.expanduser(api_key_file)

        _set_api_key(api_key_file, wandb_config.pop("api_key", None))

        # Fill trial ID and name
        trial_id = self.trial_id
        trial_name = self.trial_name

        # Project name for Wandb
        try:
            wandb_project = wandb_config.pop("project")
        except KeyError:
            raise ValueError(
                "You need to specify a `project` in your wandb `config` dict.")

        # Grouping
        if isinstance(self, FunctionRunner):
            default_group = self._name
        else:
            default_group = type(self).__name__
        wandb_group = wandb_config.pop("group", default_group)

        # remove unpickleable items!
        _config = _clean_log(_config)

        wandb_init_kwargs = dict(
            id=trial_id,
            name=trial_name,
            resume=True,
            reinit=True,
            allow_val_change=True,
            group=wandb_group,
            project=wandb_project,
            config=_config)
        wandb_init_kwargs.update(wandb_config)

        self.wandb = self._wandb.init(**wandb_init_kwargs)

    def stop(self):
        self._wandb.join()
        if hasattr(super(), "stop"):
            super().stop()
