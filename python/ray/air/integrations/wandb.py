import os
import pickle

import numpy as np
from numbers import Number

from types import ModuleType
from typing import Any, Dict, Optional, Sequence

from mock.mock import MagicMock

from ray import logger
from ray.air import session
from ray._private.storage import _load_class


try:
    import wandb
except ImportError:
    logger.error("pip install 'wandb' to use WandbLoggerCallback/WandbTrainableMixin.")
    wandb = None


_MockWandb = MagicMock


def setup_wandb(config: Dict, rank_zero_only: bool = True):
    try:
        # Do a try-catch here if we are not in a train session
        if rank_zero_only and session.get_local_rank() != 0:
            return _MockWandb()
    except Exception:
        pass

    return _setup_wandb(
        trial_id=session.get_trial_id(),
        trial_name=session.get_trial_name(),
        config=config,
        group=session.get_experiment_name(),
    )


def _setup_wandb(
    trial_id: str,
    trial_name: str,
    config: Dict,
    _wandb: Optional[ModuleType] = None,
    **kwargs,
):
    _config = config.copy()

    wandb_config = _config.pop("wandb", {}).copy()

    # If key file is specified, set
    api_key_file = wandb_config.pop("api_key_file", None)
    if api_key_file:
        api_key_file = os.path.expanduser(api_key_file)

    _set_api_key(api_key_file, wandb_config.pop("api_key", None))

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

    return _wandb.init(**wandb_init_kwargs)


if wandb:
    from wandb.util import json_dumps_safer
else:
    json_dumps_safer = None

WANDB_ENV_VAR = "WANDB_API_KEY"
WANDB_PROJECT_ENV_VAR = "WANDB_PROJECT_NAME"
WANDB_GROUP_ENV_VAR = "WANDB_GROUP_NAME"
# Hook that is invoked before wandb.init in the setup method of WandbLoggerCallback
# to populate the API key if it isn't already set when initializing the callback.
# It doesn't take in any arguments and returns the W&B API key.
# Example: "your.module.wandb_setup_api_key_hook".
WANDB_SETUP_API_KEY_HOOK = "WANDB_SETUP_API_KEY_HOOK"
# Hook that is invoked after running wandb.init in WandbLoggerCallback
# to process information about the W&B run.
# It takes in a W&B run object and doesn't return anything.
# Example: "your.module.wandb_process_run_info_hook".
WANDB_PROCESS_RUN_INFO_HOOK = "WANDB_PROCESS_RUN_INFO_HOOK"
_VALID_TYPES = (
    Number,
    wandb.data_types.Audio,
    wandb.data_types.BoundingBoxes2D,
    wandb.data_types.Graph,
    wandb.data_types.Histogram,
    wandb.data_types.Html,
    wandb.data_types.Image,
    wandb.data_types.ImageMask,
    wandb.data_types.Molecule,
    wandb.data_types.Object3D,
    wandb.data_types.Plotly,
    wandb.data_types.Table,
    wandb.data_types.Video,
)
_VALID_ITERABLE_TYPES = (
    wandb.data_types.Audio,
    wandb.data_types.BoundingBoxes2D,
    wandb.data_types.Graph,
    wandb.data_types.Histogram,
    wandb.data_types.Html,
    wandb.data_types.Image,
    wandb.data_types.ImageMask,
    wandb.data_types.Molecule,
    wandb.data_types.Object3D,
    wandb.data_types.Plotly,
    wandb.data_types.Table,
    wandb.data_types.Video,
)


def _is_allowed_type(obj):
    """Return True if type is allowed for logging to wandb"""
    if isinstance(obj, np.ndarray) and obj.size == 1:
        return isinstance(obj.item(), Number)
    if isinstance(obj, Sequence) and len(obj) > 0:
        return isinstance(obj[0], _VALID_ITERABLE_TYPES)
    return isinstance(obj, _VALID_TYPES)


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
