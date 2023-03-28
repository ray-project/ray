# DEPRECATED: Remove whole file in 2.5.

import logging
from typing import List, Dict, Callable, Optional

from ray.air.integrations.wandb import WandbLoggerCallback as _WandbLoggerCallback
from ray.util.annotations import Deprecated

logger = logging.getLogger(__name__)

callback_deprecation_message = (
    "`ray.tune.integration.wandb.WandbLoggerCallback` is deprecated. "
    "Please use `ray.air.integrations.wandb.WandbLoggerCallback` instead."
)

mixin_deprecation_message = (
    "The `wandb_mixin`/`WandbTrainableMixin` is deprecated. "
    "Use `ray.air.integrations.wandb.setup_wandb` instead."
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
        raise DeprecationWarning(callback_deprecation_message)


@Deprecated(message=callback_deprecation_message)
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
    raise DeprecationWarning(mixin_deprecation_message)


# Deprecate: Remove in 2.4
@Deprecated(message=mixin_deprecation_message)
class WandbTrainableMixin:
    def __init__(self, config: Dict, *args, **kwargs):
        raise DeprecationWarning(mixin_deprecation_message)
