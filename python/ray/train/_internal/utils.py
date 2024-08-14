import abc
import functools
import inspect
import logging
import os
from pathlib import Path
from typing import (
    Any,
    Callable,
    ContextManager,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import ray
from ray.actor import ActorHandle
from ray.air._internal.util import StartTraceback, find_free_port
from ray.exceptions import RayActorError
from ray.types import ObjectRef

T = TypeVar("T")

logger = logging.getLogger(__name__)


def check_for_failure(
    remote_values: List[ObjectRef],
) -> Tuple[bool, Optional[Exception]]:
    """Check for actor failure when retrieving the remote values.

    Args:
        remote_values: List of object references from Ray actor methods.

    Returns:
        A tuple of (bool, Exception). The bool is
        True if evaluating all object references is successful, False otherwise.
    """
    unfinished = remote_values.copy()

    while len(unfinished) > 0:
        finished, unfinished = ray.wait(unfinished)

        # If a failure occurs the ObjectRef will be marked as finished.
        # Calling ray.get will expose the failure as a RayActorError.
        for object_ref in finished:
            # Everything in finished has either failed or completed
            # successfully.
            try:
                ray.get(object_ref)
            except RayActorError as exc:
                failed_actor_rank = remote_values.index(object_ref)
                logger.info(f"Worker {failed_actor_rank} has failed.")
                return False, exc
            except Exception as exc:
                # Other (e.g. training) errors should be directly raised
                raise StartTraceback from exc

    return True, None


def get_address_and_port() -> Tuple[str, int]:
    """Returns the IP address and a free port on this node."""
    addr = ray.util.get_node_ip_address()
    port = find_free_port()

    return addr, port


def construct_path(path: Path, parent_path: Path) -> Path:
    """Constructs a path relative to a parent.

    Args:
        path: A relative or absolute path.
        parent_path: A relative path or absolute path.

    Returns: An absolute path.
    """
    if path.expanduser().is_absolute():
        return path.expanduser().resolve()
    else:
        return parent_path.joinpath(path).expanduser().resolve()


def update_env_vars(env_vars: Dict[str, Any]):
    """Updates the environment variables on this worker process.

    Args:
        env_vars: Environment variables to set.
    """
    sanitized = {k: str(v) for k, v in env_vars.items()}
    os.environ.update(sanitized)


def count_required_parameters(fn: Callable) -> int:
    """Counts the number of required parameters of a function.

    NOTE: *args counts as 1 required parameter.

    Examples
    --------

    >>> def fn(a, b, /, c, *args, d=1, e=2, **kwargs):
    ...    pass
    >>> count_required_parameters(fn)
    4

    >>> fn = lambda: 1
    >>> count_required_parameters(fn)
    0

    >>> def fn(config, a, b=1, c=2):
    ...     pass
    >>> from functools import partial
    >>> count_required_parameters(partial(fn, a=0))
    1
    """
    params = inspect.signature(fn).parameters.values()

    positional_param_kinds = {
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.VAR_POSITIONAL,
    }
    return len(
        [
            p
            for p in params
            if p.default == inspect.Parameter.empty and p.kind in positional_param_kinds
        ]
    )


def construct_train_func(
    train_func: Union[Callable[[], T], Callable[[Dict[str, Any]], T]],
    config: Optional[Dict[str, Any]],
    train_func_context: ContextManager,
    fn_arg_name: Optional[str] = "train_func",
    discard_returns: bool = False,
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
        discard_returns: Whether to discard any returns from train_func or not.
    Returns:
        A valid training function.
    Raises:
        ValueError: if the input ``train_func`` is invalid.
    """
    num_required_params = count_required_parameters(train_func)

    if discard_returns:
        # Discard any returns from the function so that
        # BackendExecutor doesn't try to deserialize them.
        # Those returns are inaccesible with AIR anyway.
        @functools.wraps(train_func)
        def discard_return_wrapper(*args, **kwargs):
            try:
                train_func(*args, **kwargs)
            except Exception as e:
                raise StartTraceback from e

        wrapped_train_func = discard_return_wrapper
    else:
        wrapped_train_func = train_func

    if num_required_params > 1:
        err_msg = (
            f"{fn_arg_name} should take in 0 or 1 required arguments, but it accepts "
            f"{num_required_params} required arguments instead."
        )
        raise ValueError(err_msg)
    elif num_required_params == 1:
        config = {} if config is None else config

        @functools.wraps(wrapped_train_func)
        def train_fn():
            try:
                with train_func_context():
                    return wrapped_train_func(config)
            except Exception as e:
                raise StartTraceback from e

    else:  # num_params == 0

        @functools.wraps(wrapped_train_func)
        def train_fn():
            try:
                with train_func_context():
                    return wrapped_train_func()
            except Exception as e:
                raise StartTraceback from e

    return train_fn


class Singleton(abc.ABCMeta):
    """Singleton Abstract Base Class

    https://stackoverflow.com/questions/33364070/implementing
    -singleton-as-metaclass-but-for-abstract-classes
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class ActorWrapper:
    """Wraps an actor to provide same API as using the base class directly."""

    def __init__(self, actor: ActorHandle):
        self.actor = actor

    def __getattr__(self, item):
        # The below will fail if trying to access an attribute (not a method) from the
        # actor.
        actor_method = getattr(self.actor, item)
        return lambda *args, **kwargs: ray.get(actor_method.remote(*args, **kwargs))
