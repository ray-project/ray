import re
import sys
import warnings
from types import FrameType
from typing import Any, Callable, Dict, Tuple

from ray.util.annotations import RayDeprecationWarning


def _copy_doc(copy_func):
    def wrapped(func):
        func.__doc__ = copy_func.__doc__
        return func

    return wrapped


def _log_deprecation_warning(message: str):
    warnings.warn(
        message,
        RayDeprecationWarning,
        stacklevel=2,
    )


# Modules that look up attributes on behalf of their caller: `from x import y`
# goes through importlib for packages, and pickling a class looks it up by name.
_LOOKUP_MACHINERY_MODULES = ("copyreg", "importlib", "pickle")


def _is_lookup_machinery(module_name: str) -> bool:
    return module_name.startswith(("_frozen_importlib",)) or any(
        module_name == m or module_name.startswith(m + ".")
        for m in _LOOKUP_MACHINERY_MODULES
    )


def _is_skipped_by_warnings(frame: FrameType) -> bool:
    # `warnings.warn` skips the frozen import machinery frames and doesn't count
    # them towards `stacklevel`. This mirrors `_is_internal_frame` in CPython's
    # `warnings` module.
    filename = frame.f_code.co_filename
    return "importlib" in filename and "_bootstrap" in filename


def _is_ray_module(module_name: str) -> bool:
    return module_name == "ray" or module_name.startswith("ray.")


def _find_caller(depth: int, skip_ray: bool) -> Tuple[int, bool]:
    """Find the frame that a deprecation warning should be attributed to.

    Starting ``depth`` frames above the function that calls this one, skip
    frames of the import and pickle machinery and, if ``skip_ray`` is set,
    frames inside the ``ray`` package.

    Args:
        depth: The number of frames above the caller of this function to start
            from. 1 is the caller's caller.
        skip_ray: Whether to also skip frames inside the ``ray`` package.

    Returns:
        A tuple of the ``stacklevel`` to pass to ``warnings.warn`` from the caller
        of this function, and whether the frame found is inside the ``ray``
        package.
    """
    # `stacklevel=1` is the caller of this function.
    frame = sys._getframe(1).f_back
    stacklevel = 1
    while frame is not None:
        if not _is_skipped_by_warnings(frame):
            stacklevel += 1
            module_name = frame.f_globals.get("__name__", "")
            if (
                stacklevel > depth
                and not _is_lookup_machinery(module_name)
                and not (skip_ray and _is_ray_module(module_name))
            ):
                return stacklevel, _is_ray_module(module_name)
        frame = frame.f_back
    return stacklevel, False


def _make_deprecated_module_getattr(
    module_name: str, deprecated_attrs: Dict[str, Tuple[Any, str]]
) -> Callable[[str], Any]:
    """Build a module ``__getattr__`` that warns when a deprecated name is accessed.

    The deprecated names must not be module globals, otherwise Python never calls
    ``__getattr__``. Accesses from code inside the ``ray`` package don't warn,
    because users can neither fix nor silence them. This covers internal imports
    and ``ray.cloudpickle`` looking up classes by reference.

    The warning is gated on ``RAY_TRAIN_ENABLE_V2_MIGRATION_WARNINGS``.

    Args:
        module_name: The ``__name__`` of the module, for the ``AttributeError``.
        deprecated_attrs: Maps each deprecated name to a tuple of the object to
            return and the deprecation message.

    Returns:
        A function to assign to the module's ``__getattr__``.
    """

    def __getattr__(name: str) -> Any:
        if name not in deprecated_attrs:
            raise AttributeError(f"module {module_name!r} has no attribute {name!r}")
        obj, message = deprecated_attrs[name]

        from ray.train.constants import _v2_migration_warnings_enabled

        if _v2_migration_warnings_enabled():
            stacklevel, from_ray = _find_caller(depth=1, skip_ray=False)
            if not from_ray:
                warnings.warn(message, RayDeprecationWarning, stacklevel=stacklevel)
        return obj

    return __getattr__


def _in_ray_worker_process() -> bool:
    import ray
    from ray._private.worker import global_worker

    return global_worker.mode == ray.WORKER_MODE


def _log_v1_mode_deprecation_warning() -> None:
    """Warn that Ray Train v1 is deprecated because RAY_TRAIN_V2_ENABLED=0 is set.

    Called once per process, when ``ray.train`` is imported in v1 mode. Ray worker
    processes don't warn, so the warning appears once on the driver rather than
    once for every Train worker, Tune trial and RLlib learner.
    """
    from ray.train.constants import (
        V1_MODE_DEPRECATION_MESSAGE,
        _v2_migration_warnings_enabled,
    )

    if not _v2_migration_warnings_enabled() or _in_ray_worker_process():
        return
    stacklevel, _ = _find_caller(depth=1, skip_ray=True)
    warnings.warn(
        V1_MODE_DEPRECATION_MESSAGE, RayDeprecationWarning, stacklevel=stacklevel
    )


_V1_FRAMEWORK_TRAINER_MODULE = re.compile(
    r"^ray\.train\.(torch|tensorflow|xgboost|lightgbm)\.\w+_trainer$"
)


def _v1_trainer_deprecation_message(trainer_cls: type) -> str:
    """Return the deprecation message for a Ray Train v1 trainer class.

    The message names the Ray Train v2 replacement. For a user subclass of a v1
    framework trainer, it names the replacement for that framework trainer.
    """
    from ray.train.constants import V1_REMOVAL_VERSION, V2_MIGRATION_GUIDE_MESSAGE

    replacement = (
        "a Ray Train v2 trainer, such as `ray.train.torch.TorchTrainer`, that runs "
        "your training function"
    )
    for cls in trainer_cls.__mro__:
        match = _V1_FRAMEWORK_TRAINER_MODULE.match(cls.__module__)
        if match:
            replacement = f"`ray.train.{match.group(1)}.{cls.__name__}`"
            break
        if cls.__module__ == "ray.train.horovod.horovod_trainer":
            replacement = (
                "`ray.train.torch.TorchTrainer` or "
                "`ray.train.tensorflow.TensorflowTrainer`, because Horovod has no "
                "Ray Train v2 trainer"
            )
            break

    return (
        f"`{trainer_cls.__module__}.{trainer_cls.__qualname__}` is a Ray Train v1 "
        "trainer. Ray Train v1 is deprecated and will be removed in "
        f"{V1_REMOVAL_VERSION}. Use {replacement} instead, with "
        "RAY_TRAIN_V2_ENABLED unset. "
        f"{V2_MIGRATION_GUIDE_MESSAGE}"
    )


def _log_v1_trainer_deprecation_warning(trainer_cls: type) -> None:
    """Warn that a Ray Train v1 trainer was constructed.

    The warning points at the first frame outside the ``ray`` package, so it names
    the line of user code that built the trainer, even through ``super().__init__``
    calls. Ray worker processes don't warn, because Ray rebuilds the trainer inside
    the Tune trial actor that runs it.
    """
    from ray.train.constants import _v2_migration_warnings_enabled

    if not _v2_migration_warnings_enabled() or _in_ray_worker_process():
        return
    stacklevel, _ = _find_caller(depth=1, skip_ray=True)
    warnings.warn(
        _v1_trainer_deprecation_message(trainer_cls),
        RayDeprecationWarning,
        stacklevel=stacklevel,
    )


def _in_ray_train_worker() -> bool:
    from ray.train.v2._internal.constants import is_v2_enabled

    if is_v2_enabled():
        from ray.train.v2._internal.util import (
            _in_ray_train_worker as _in_ray_train_v2_worker,
        )

        return _in_ray_train_v2_worker()
    else:
        from ray.train._internal.session import (
            _in_ray_train_worker as _in_ray_train_v1_worker,
        )

        return _in_ray_train_v1_worker()
