import contextlib
import functools
import logging
import time
import traceback
from datetime import datetime
from typing import (
    Any,
    Callable,
    ContextManager,
    Dict,
    Generator,
    Generic,
    List,
    Optional,
    TypeVar,
    Union,
)

import ray
from ray.train._internal.utils import count_required_parameters
from ray.train.v2._internal.exceptions import UserExceptionWithTraceback
from ray.types import ObjectRef

logger = logging.getLogger(__name__)


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
    fn_arg_name: Optional[str] = "train_loop_per_worker",
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


class ObjectRefWrapper(Generic[T]):
    """Thin wrapper around ray.put to manually control dereferencing."""

    def __init__(self, obj: T):
        self._ref = ray.put(obj)

    def get(self) -> T:
        return ray.get(self._ref)


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


def ray_get_safe(
    object_refs: Union[ObjectRef, List[ObjectRef]],
) -> Union[Any, List[Any]]:
    """This is a safe version of `ray.get` that raises an exception immediately
    if an input task dies, while the others are still running.

    TODO(ml-team, core-team): This is NOT a long-term solution,
    and we should not maintain this function indefinitely.
    This is a mitigation for a Ray Core bug, and should be removed when
    that is fixed.
    See here: https://github.com/ray-project/ray/issues/47204

    Args:
        object_refs: A single or list of object refs to wait on.

    Returns:
        task_outputs: The outputs of the tasks.

    Raises:
        `RayTaskError`/`RayActorError`: if any of the tasks encounter a runtime error
            or fail due to actor/task death (ex: node failure).
    """
    is_list = isinstance(object_refs, list)
    object_refs = object_refs if is_list else [object_refs]

    unready = object_refs
    task_to_output = {}
    while unready:
        ready, unready = ray.wait(unready, num_returns=1)
        if ready:
            for task, task_output in zip(ready, ray.get(ready)):
                task_to_output[task] = task_output

    assert len(task_to_output) == len(object_refs)
    ordered_outputs = [task_to_output[task] for task in object_refs]
    return ordered_outputs if is_list else ordered_outputs[0]


@contextlib.contextmanager
def invoke_context_managers(
    context_managers: List[ContextManager],
) -> Generator[None, None, None]:
    """
    Utility to invoke a list of context managers and yield sequentially.

    Args:
        context_managers: List of context managers to invoke.
    """
    with contextlib.ExitStack() as stack:
        for context_manager in context_managers:
            stack.enter_context(context_manager())
        yield


def get_module_name(obj: object) -> str:
    """Returns the full module name of the given object, including its qualified name.

    Args:
        obj: The object (class, function, etc.) whose module name is required.

    Returns:
        Full module and qualified name as a string.
    """
    return f"{obj.__module__}.{obj.__qualname__}"


def get_callable_name(fn: Callable) -> str:
    """Returns a readable name for any callable.

    Examples:

        >>> get_callable_name(lambda x: x)
        '<lambda>'
        >>> def foo(a, b): pass
        >>> get_callable_name(foo)
        'foo'
        >>> from functools import partial
        >>> bar = partial(partial(foo, a=1), b=2)
        >>> get_callable_name(bar)
        'foo'
        >>> class Dummy:
        ...     def __call__(self, a, b): pass
        >>> get_callable_name(Dummy())
        'Dummy'
    """
    if isinstance(fn, functools.partial):
        return get_callable_name(fn.func)

    # Use __name__ for regular functions and lambdas
    if hasattr(fn, "__name__"):
        return fn.__name__

    # Fallback to the class name for objects that implement __call__
    return fn.__class__.__name__


def construct_user_exception_with_traceback(
    e: BaseException, exclude_frames: int = 0
) -> UserExceptionWithTraceback:
    """Construct a UserExceptionWithTraceback from a base exception.

    Args:
        e: The base exception to construct a UserExceptionWithTraceback from.
        exclude_frames: The number of frames to exclude from the beginnning of
            the traceback.

    Returns:
        A UserExceptionWithTraceback object.
    """
    # TODO(justinvyu): This is brittle and may break if the call stack
    # changes. Figure out a more robust way to exclude these frames.
    exc_traceback_str = traceback.format_exc(
        limit=-(len(traceback.extract_tb(e.__traceback__)) - exclude_frames)
    )
    logger.error(f"Error in training function:\n{exc_traceback_str}")
    return UserExceptionWithTraceback(e, traceback_str=exc_traceback_str)


def _in_ray_train_worker() -> bool:
    """Check if the current process is a Ray Train V2 worker."""
    from ray.train.v2._internal.execution.train_fn_utils import get_train_fn_utils

    try:
        get_train_fn_utils()
        return True
    except RuntimeError:
        return False


def requires_train_worker(raise_in_tune_session: bool = False) -> Callable:
    """Check that the caller is a Ray Train worker spawned by Ray Train,
    with access to training function utilities.

    Args:
        raise_in_tune_session: Whether to raise a specific error message if the caller
            is in a Tune session. If True, will raise a DeprecationWarning.

    Returns:
        A decorator that performs this check, which raises an error if the caller
        is not a Ray Train worker.
    """

    def _wrap(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def _wrapped_fn(*args, **kwargs):
            from ray.tune.trainable.trainable_fn_utils import _in_tune_session

            if raise_in_tune_session and _in_tune_session():
                raise DeprecationWarning(
                    f"`ray.train.{fn.__name__}` is deprecated when running in a function "
                    "passed to Ray Tune. Please use the equivalent `ray.tune` API instead. "
                    "See this issue for more context: "
                    "https://github.com/ray-project/ray/issues/49454"
                )

            if not _in_ray_train_worker():
                raise RuntimeError(
                    f"`{fn.__name__}` cannot be used outside of a Ray Train training function. "
                    "You are calling this API from the driver or another non-training process. "
                    "These utilities are only available within a function launched by `trainer.fit()`."
                )
            return fn(*args, **kwargs)

        return _wrapped_fn

    return _wrap
