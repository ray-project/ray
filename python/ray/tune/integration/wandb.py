import os
from typing import List, Dict, Callable, Optional

from ray.tune import Trainable
from ray.tune.trainable import FunctionTrainable

from ray.air.callbacks.wandb import (
    wandb,
    _clean_log,
    _set_api_key,
    WandbLoggerCallback as _WandbLoggerCallback,
)

import logging

from ray.util.annotations import Deprecated

logger = logging.getLogger(__name__)

callback_deprecation_message = (
    "`ray.tune.integration.wandb.WandbLoggerCallback` "
    "is deprecated and will be removed in "
    "the future. Please use `ray.air.callbacks.wandb.WandbLoggerCallback` "
    "instead."
)


@Deprecated(message=callback_deprecation_message)
class WandbLoggerCallback(_WandbLoggerCallback):
    def __init__(
        self,
        project: str,
        group: Optional[str] = None,
        api_key_file: Optional[str] = None,
        api_key: Optional[str] = None,
        excludes: Optional[List[str]] = None,
        log_config: bool = False,
        save_checkpoints: bool = False,
        **kwargs
    ):
        logger.warning(callback_deprecation_message)
        super().__init__(
            project,
            group,
            api_key_file,
            api_key,
            excludes,
            log_config,
            save_checkpoints,
            **kwargs
        )


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
    the ``param_space`` parameter of ``tune.Tuner()`` (see example below).

    The content of the ``wandb`` config entry is passed to ``wandb.init()``
    as keyword arguments. The exception are the following settings, which
    are used to configure the ``WandbTrainableMixin`` itself:

    Args:
        api_key_file: Path to file containing the Wandb API KEY. This
            file must be on all nodes if using the `wandb_mixin`.
        api_key: Wandb API Key. Alternative to setting `api_key_file`.

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

        tuner = tune.Tuner(
            train_fn,
            param_space={
                # define search space here
                "a": tune.choice([1, 2, 3]),
                "b": tune.choice([4, 5, 6]),
                # wandb configuration
                "wandb": {
                    "project": "Optimization_Project",
                    "api_key_file": "/path/to/file"
                }
            })
        tuner.fit()

    """
    if hasattr(func, "__mixins__"):
        func.__mixins__ = func.__mixins__ + (WandbTrainableMixin,)
    else:
        func.__mixins__ = (WandbTrainableMixin,)
    return func


class WandbTrainableMixin:
    _wandb = wandb

    def __init__(self, config: Dict, *args, **kwargs):
        if not isinstance(self, Trainable):
            raise ValueError(
                "The `WandbTrainableMixin` can only be used as a mixin "
                "for `tune.Trainable` classes. Please make sure your "
                "class inherits from both. For example: "
                "`class YourTrainable(WandbTrainableMixin)`."
            )

        _config = config.copy()

        try:
            wandb_config = _config.pop("wandb").copy()
        except KeyError:
            raise ValueError(
                "Wandb mixin specified but no configuration has been passed. "
                "Make sure to include a `wandb` key in your `config` dict "
                "containing at least a `project` specification."
            )

        super().__init__(_config, *args, **kwargs)

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
                "You need to specify a `project` in your wandb `config` dict."
            )

        # Grouping
        if isinstance(self, FunctionTrainable):
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
            config=_config,
        )
        wandb_init_kwargs.update(wandb_config)

        # On windows, we can't fork
        if os.name == "nt":
            os.environ["WANDB_START_METHOD"] = "thread"
        else:
            os.environ["WANDB_START_METHOD"] = "fork"

        self.wandb = self._wandb.init(**wandb_init_kwargs)

    def stop(self):
        self._wandb.finish()
        if hasattr(super(), "stop"):
            super().stop()
