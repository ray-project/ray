import os

from multiprocessing import Process, Queue
from numbers import Number
from typing import List

from ray import logger
from ray.tune.logger import Logger
from ray.tune.utils import unflattened_lookup

try:
    import wandb
except ImportError:
    logger.error("pip install 'wandb' to use WandBLogger.")
    raise

WANDB_ENV_VAR = "WANDB_API_KEY"
_WANDB_QUEUE_END = (None, )


class _WandbLoggingProcess(Process):
    """
    We need a `multiprocessing.Process` to allow multiple concurrent
    wandb logging instances locally.
    """

    def __init__(self, queue, exclude, to_config, *args, **kwargs):
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

    def _handle_result(self, result):
        config_update = result.get("config", {}).copy()
        log = {}

        for k, v in result.items():
            if k in self._to_config:
                config_update[k] = v
            elif k in self._exclude:
                continue
            elif not isinstance(v, Number):
                continue
            else:
                log[k] = v

        config_update.pop("callbacks", None)  # Remove callbacks
        return log, config_update


class WandbLogger(Logger):
    """WandbLogger

    Weights and biases (https://www.wandb.com/) is a tool for experiment
    tracking, model optimization, and dataset versioning. This Ray Tune
    `Logger` sends metrics to Wandb for automatic tracking and
    visualization.

    Wandb configuration is done by passing a `wandb` key to
    the `config` parameter of `tune.run()` (see example below).

    The content of the `wandb` config entry is passed to `wandb.init()`
    as keyword arguments. The exception are the following settings, which
    are used to configure the WandbLogger itself:

    Settings:
        api_key_file (str): Path to file containing the Wandb API KEY.
        api_key (str): Wandb API Key. Alternative to setting `api_key_file`.
        excludes (List[str]): List of metrics that should be excluded from
            the log.
        group_by (Union[str, List[str]]): Decides how the trials should be
            grouped. If `group_by="trainable_name"`, the group is retrieved
            from the trainable name.
        log_config (Boolean): Boolean indicating if the `config` parameter of
            the `results` dict should be logged. This makes sense if parameters
            will change during training, e.g. with PopulationBasedTraining.
            Defaults to False.

    Wandb's `group`, `run_id` and `run_name` are automatically selected by
    Tune, but can be overwritten by filling out the respective configuration
    values.

    Please see here for all other valid configuration settings:
    https://docs.wandb.com/library/init

    Example:
        ```
        from ray.tune.logger import WandbLogger, DEFAULT_LOGGERS
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
                    "log_config": True,
                    "group_by": ["parameter_1", "parameter_2"]
                }
            },
            loggers=DEFAULT_LOGGERS + (WandbLogger, ))
        ```
    """

    # Do not log these result keys
    _exclude_results = ["done", "should_checkpoint"]

    # Use these result keys to update `wandb.config`
    _config_results = [
        "trial_id", "experiment_tag", "node_ip", "experiment_id", "hostname",
        "pid", "date"
    ]

    _logger_process_cls = _WandbLoggingProcess

    def _init(self):
        config = self.config.copy()

        try:
            wandb_config = self.config.pop("wandb").copy()
        except KeyError:
            raise ValueError(
                "Wandb logger specified but no configuration has been passed. "
                "Make sure to include a `wandb` key in your `config` dict "
                "containing at least a `project` specification.")

        # Login using API key.
        api_key_file = wandb_config.pop("api_key_file", None)
        api_key = wandb_config.pop("api_key", None)

        if api_key_file:
            if api_key:
                raise ValueError(
                    "Both WandB `api_key_file` and `api_key` set.")
            with open(api_key_file, "rt") as fp:
                api_key = fp.readline()
        if api_key:
            os.environ[WANDB_ENV_VAR] = api_key
        elif not os.environ.get(WANDB_ENV_VAR):
            raise ValueError(
                "No WandB API key found. Either set the {} environment "
                "variable or pass `api_key` or `api_key_file` in the config".
                format(WANDB_ENV_VAR))

        exclude_results = self._exclude_results.copy()

        # Additional excludes
        additional_excludes = wandb_config.pop("excludes", [])
        exclude_results += additional_excludes

        # Log config keys on each result?
        log_config = wandb_config.pop("log_config", False)
        if not log_config:
            exclude_results += ["config"]

        # Fill trial ID and name
        trial_id = self.trial.trial_id
        trial_name = str(self.trial)

        # Project name for Wandb
        try:
            wandb_project = wandb_config.pop("project")
        except KeyError:
            raise ValueError(
                "You need to specify a `project` in your wandb `config` dict.")

        # Grouping
        group_by = wandb_config.pop("group_by", "trainable_name")
        if isinstance(group_by, List):

            def fmt(v):
                return round(v, 6) if isinstance(v, Number) else v

            try:
                group_name = ",".join([
                    "{}={}".format(k, fmt(unflattened_lookup(k, config, ".")))
                    for k in group_by
                ])
            except KeyError:
                raise ValueError(
                    "The `wandb.group_by` list contains keys that are not "
                    "defined in Tune's search space (`config`).")
        else:
            if group_by == "trainable_name":
                group_name = self.trial.trainable_name
            else:
                raise ValueError(
                    "Invalid value for `group_by` Wandb config parameter: {} ."
                    "Please pass either 'trainable_name' or a list of config "
                    "parameters to group trials by.".format(group_by))

        wandb_group = wandb_config.pop("group", group_name)

        wandb_init_kwargs = dict(
            id=trial_id,
            name=trial_name,
            resume=True,
            reinit=True,
            allow_val_change=True,
            group=wandb_group,
            project=wandb_project,
            config=config)
        wandb_init_kwargs.update(wandb_config)

        self._queue = Queue()
        self._wandb = self._logger_process_cls(
            queue=self._queue,
            exclude=exclude_results,
            to_config=self._config_results,
            **wandb_init_kwargs)
        self._wandb.start()

    def on_result(self, result):
        self._queue.put(result)

    def close(self):
        self._queue.put(_WANDB_QUEUE_END)
        self._wandb.join(timeout=10)
