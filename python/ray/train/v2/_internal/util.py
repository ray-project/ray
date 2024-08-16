import functools
import time
from datetime import datetime
from typing import Any, Callable, ContextManager, Dict, Optional, TypeVar, Union

from ray.train._internal.utils import count_required_parameters

T = TypeVar("T")


def bundle_to_remote_args(bundle: dict) -> dict:
    """Convert a bundle of resources to Ray actor/task arguments.

    >>> bundle_to_remote_args({"GPU": 1, "memory": 1, "custom": 0.1})
    {'num_cpus': 0, 'num_gpus': 1, 'memory': 1, 'resources': {'custom': 0.1}}
    """
    bundle = bundle.copy()
    args = {
        "num_cpus": bundle.pop("CPU", 0),
        "num_gpus": bundle.pop("GPU", 0),
        "memory": bundle.pop("memory", 0),
    }
    if bundle:
        args["resources"] = bundle
    return args


def construct_train_func(
    train_func: Union[Callable[[], T], Callable[[Dict[str, Any]], T]],
    config: Optional[Dict[str, Any]],
    train_func_context: ContextManager,
    fn_arg_name: Optional[str] = "train_func",
) -> Callable[[], T]:
    """Validates and constructs the training function to execute.
    Args:
        train_func: The training function to execute.
            This can either take in no arguments or a ``config`` dict.
        config (Optional[Dict]): Configurations to pass into
            ``train_func``. If None then an empty Dict will be created.
        train_func_context: Context manager for user's `train_func`, which executes
            backend-specific logic before and after the training function.
        fn_arg_name (Optional[str]): The name of training function to use for error
            messages.
    Returns:
        A valid training function.
    Raises:
        ValueError: if the input ``train_func`` is invalid.
    """
    num_required_params = count_required_parameters(train_func)

    if num_required_params > 1:
        err_msg = (
            f"{fn_arg_name} should take in 0 or 1 required arguments, but it accepts "
            f"{num_required_params} required arguments instead."
        )
        raise ValueError(err_msg)

    if num_required_params == 1:
        config = config or {}

        @functools.wraps(train_func)
        def train_fn():
            with train_func_context():
                return train_func(config)

    else:  # num_params == 0

        @functools.wraps(train_func)
        def train_fn():
            with train_func_context():
                return train_func()

    return train_fn


def date_str(include_ms: bool = False):
    pattern = "%Y-%m-%d_%H-%M-%S"
    if include_ms:
        pattern += ".%f"
    return datetime.today().strftime(pattern)


def time_monotonic():
    return time.monotonic()


def _copy_doc(copy_func):
    def wrapped(func):
        func.__doc__ = copy_func.__doc__
        return func

    return wrapped
