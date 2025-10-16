import importlib
import os
from typing import List

from ray.train.v2._internal.constants import RAY_TRAIN_CALLBACKS_ENV_VAR
from ray.train.v2._internal.execution.callback import RayTrainCallback


def _initialize_env_callbacks() -> List[RayTrainCallback]:
    """Initialize callbacks from environment variable.

    Returns:
        List of callbacks initialized from environment variable.
    """
    callbacks = []
    callbacks_str = os.environ.get(RAY_TRAIN_CALLBACKS_ENV_VAR, "")
    if not callbacks_str:
        return callbacks

    for callback_path in callbacks_str.split(","):
        callback_path = callback_path.strip()
        if not callback_path:
            continue

        try:
            module_path, class_name = callback_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            callback_cls = getattr(module, class_name)
            if not issubclass(callback_cls, RayTrainCallback):
                raise TypeError(
                    f"Callback class '{callback_path}' must be a subclass of "
                    f"RayTrainCallback, got {type(callback_cls).__name__}"
                )
            callback = callback_cls()
            callbacks.append(callback)
        except (ImportError, AttributeError, ValueError, TypeError) as e:
            raise ValueError(f"Failed to import callback from '{callback_path}'") from e

    return callbacks
