import os

from multiprocessing import Process, Queue

from ray import logger
from ray.tune.logger import Logger

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
        self._exclude = exclude
        self._to_config = to_config
        self.args = args
        self.kwargs = kwargs

    def run(self):
        wandb.init(*self.args, **self.kwargs)
        while True:
            result = self.queue.get()
            if result == _WANDB_QUEUE_END:
                break
            log = result.copy()
            config_update = log.get("config", {})
            for k in self._to_config:
                if k in log:
                    config_update[k] = log.pop(k)
            for k in self._exclude:
                log.pop(k, None)
            wandb.config.update(config_update, allow_val_change=True)
            wandb.log(log)
        wandb.join()


class WandbLogger(Logger):
    """WandbLogger

    Wandb configuration is done by passing a `wandb` key to
    the `tune.run(config)` parameter (see example below).

    The content of the `wandb` config entry is passed to `wandb.init()`
    as keyword arguments. The exception are the following settings:

    Settings:
        api_key_file: Path to file containing the Wandb API KEY.
        api_key: Wandb API Key. Alternative to setting the `api_key_file`.
        log_config: Boolean indicating if the `config` parameter of the
            `results` dict should be logged. This makes sense if parameters
            will change during training, e.g. with PopulationBasedTraining.
            Defaults to False.

    The `group`, `run_id` and `run_name` are automatically selected by Tune,
    but can be overwritten by filling out the respective configuration
    values.

    Please see here for all other valid configuration settings:
    https://docs.wandb.com/library/init

    Example:
        ```
        tune.run(
            train_fn,
            config={
                # define search space here
                "parameter": tune.choice([1, 2, 3]),
                # wandb configuration
                "wandb": {
                    "project": "Optimization_Project",
                    "api_key_file": "/path/to/file",
                    "log_config": True
                }
            }
        ```
    """

    # Do not log these result keys
    _exclude_results = ["done", "should_checkpoint"]
    # Use these result keys to update the config
    _config_results = [
        "trial_id", "experiment_tag", "node_ip", "experiment_id", "hostname",
        "pid", "date"
    ]

    def _init(self):
        config = self.config.copy()

        wandb_config = self.config.pop("wandb").copy()

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

        # Log config keys on each result?
        log_config = wandb_config.pop("log_config", False)
        exclude_results = self._exclude_results
        if not log_config:
            exclude_results += ["config"]

        trial_id = self.trial.trial_id
        trial_name = str(self.trial)

        group_name = self.trial.trainable_name
        wandb_project = wandb_config.pop("project")
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
        self._wandb = _WandbLoggingProcess(
            queue=self._queue,
            exclude=exclude_results,
            to_config=self._config_results,
            **wandb_init_kwargs)
        self._wandb.start()

    def on_result(self, result):
        self._queue.put(result)

    def close(self):
        self._queue.put(_WANDB_QUEUE_END)
