"""Ray-managed execution of
:class:`~ray.data.util.torch_inference.TorchInference`.

``Dataset.map_batches`` detects UDF classes that subclass
``TorchInference`` and wraps them in a managed callable that drives the
``collate`` -> host-to-device transfer -> ``process_on_device`` ->
device-to-host transfer -> ``finalize`` flow, so users only implement the
model-specific pieces.

NOTE: This module must stay importable without ``torch``: detection runs for
every ``map_batches`` call, so anything needing torch is imported lazily.
"""

import inspect
import logging
from collections.abc import Mapping
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Optional,
    Tuple,
    Type,
)

from ray.data.util.torch_inference import TorchInference

if TYPE_CHECKING:
    import torch

    from ray.data._internal.compute import ComputeStrategy
    from ray.data.block import CallableClass, DataBatch
    from ray.data.collate_fn import TensorBatchType

logger = logging.getLogger(__name__)

# Methods a `TorchInference` subclass may override; validated (e.g. must
# not be async) before wrapping.
_TORCH_INFERENCE_METHODS = (
    "initialize",
    "get_device",
    "collate",
    "process_on_device",
    "finalize",
)


class _BaseTorchInferenceUDFWrapper:
    """Marker base class for the Ray-managed ``TorchInference`` wrappers."""


def is_torch_inference_class(fn: Any) -> bool:
    """True iff ``fn`` is a ``TorchInference`` subclass (the class itself,
    not an instance)."""
    return isinstance(fn, type) and issubclass(fn, TorchInference)


def is_torch_inference_instance(fn: Any) -> bool:
    """True iff ``fn`` is an *instance* of a ``TorchInference`` subclass
    (users must pass the class, not an instance)."""
    return isinstance(fn, TorchInference)


def validate_torch_inference_op(
    cls: Type[TorchInference],
    fn_args: Optional[Iterable[Any]],
    fn_kwargs: Optional[Dict[str, Any]],
    compute: "ComputeStrategy",
    ray_remote_args: Dict[str, Any],
) -> None:
    """Validate a ``map_batches`` call whose UDF is a ``TorchInference``.

    Raises for arguments the managed flow can't honor; warns for likely
    misconfigurations. Does not modify any of its inputs.
    (``fn_constructor_args``/``fn_constructor_kwargs`` are supported — they
    are forwarded to ``initialize``.)
    """
    from ray.data._internal.compute import ActorPoolStrategy

    assert isinstance(compute, ActorPoolStrategy)

    if fn_args or fn_kwargs:
        raise ValueError(
            "`fn_args` and `fn_kwargs` are not supported with a "
            "`TorchInference`; its methods only take the batch."
        )

    for method_name in _TORCH_INFERENCE_METHODS:
        method = getattr(cls, method_name, None)
        if method is None:
            continue
        if inspect.iscoroutinefunction(method) or inspect.isasyncgenfunction(method):
            raise TypeError(
                f"`{cls.__name__}.{method_name}` must not be async; "
                "`TorchInference` methods are called synchronously."
            )

    if "__call__" in cls.__dict__:
        logger.warning(
            f"`TorchInference` subclass `{cls.__name__}` defines "
            "`__call__`, but it will not be used directly: batches flow "
            "through the Ray-managed flow (`collate` -> `process_on_device` "
            "-> `finalize`), driven by a Ray-provided `__call__`."
        )

    if not ray_remote_args.get("num_gpus"):
        logger.warning(
            f"`{cls.__name__}` is a `TorchInference` but `num_gpus` is "
            "not set; the managed flow is GPU-only, so pass `num_gpus` to "
            "`map_batches` so the actor is scheduled on a GPU."
        )


def split_batch_and_other(ret: Any) -> "Tuple[Any, Any]":
    """Split a ``collate``/``process_on_device`` return into
    ``(tensors, other)``.

    A 2-tuple always means ``(tensors, other)``; any other return is
    ``(ret, None)``. (To return a batch that IS a pair of tensors, use a
    list or also do: ``((tensors, tensors), None)``.)
    """
    if isinstance(ret, tuple) and len(ret) == 2:
        return ret[0], ret[1]
    return ret, None


def _resolve_cuda_device(user: TorchInference) -> "torch.device":
    """Resolve and validate ``user.get_device()`` into a concrete CUDA device."""
    import torch

    # `torch.device(...)` is idempotent, so a `get_device` that returns a
    # device string still works.
    device = torch.device(user.get_device())
    if device.type != "cuda":
        raise ValueError(
            f"`{type(user).__name__}.get_device()` must return a CUDA device; "
            f"got `{device}`. The `TorchInference` flow is GPU-only."
        )
    if torch.cuda.is_available() and device.index is None:
        # Resolve "cuda" to a concrete index so different actor threads all
        # deterministically resolve to the same device.
        device = torch.device("cuda", torch.cuda.current_device())
    return device


def _validate_no_tensors_off_device(
    batch: "TensorBatchType", device: "torch.device", requirement: str
) -> None:
    """Raise if any tensor in ``batch`` is not on ``device``; ``requirement``
    is the caller's message prefix (what must hold, and why)."""
    from ray.data._internal.utils.torch_utils import find_tensor_off_device

    off_device = find_tensor_off_device(batch, device)
    if off_device is not None:
        raise ValueError(f"{requirement}; found a tensor on `{off_device.device}`.")


def validate_collated_batch(collated: Any, user_cls: Type[TorchInference]) -> None:
    """Validate the ``collate`` output: a ``TensorBatchType`` of CPU tensors."""
    import torch

    from ray.data.collate_fn import is_tensor_batch_type

    if not is_tensor_batch_type(collated):
        raise ValueError(
            f"`{user_cls.__name__}.collate` must return a `TensorBatchType` "
            "(a `torch.Tensor`, sequence of tensors, or mapping of str to "
            f"tensors), or a `(TensorBatchType, other)` tuple; got "
            f"{type(collated)}."
        )
    _validate_no_tensors_off_device(
        collated,
        torch.device("cpu"),
        f"`{user_cls.__name__}.collate` must return CPU tensors (Ray "
        "manages the transfer to the device)",
    )


def validate_processed_batch(
    out: Any, device: "torch.device", user_cls: Type[TorchInference]
) -> None:
    """Validate the ``process_on_device`` output: a ``TensorBatchType`` on
    ``device``."""
    from ray.data.collate_fn import is_tensor_batch_type

    if not is_tensor_batch_type(out):
        raise ValueError(
            f"`{user_cls.__name__}.process_on_device` must return a "
            "`TensorBatchType`, or a `(TensorBatchType, other)` tuple; got "
            f"{type(out)}."
        )
    _validate_no_tensors_off_device(
        out,
        device,
        f"`{user_cls.__name__}.process_on_device` must return tensors on "
        f"`{device}` (Ray manages the transfer back to the host)",
    )


def make_torch_inference_callable(user_cls: Type[TorchInference]) -> "CallableClass":
    """Wrap a ``TorchInference`` subclass in a managed callable class.

    Per batch, the wrapper's ``__call__`` runs the flow serially: ``collate``
    (validated CPU tensors) -> synchronous host-to-device transfer ->
    ``process_on_device`` (validated on-device tensors) -> synchronous
    device-to-host transfer -> ``finalize``. Everything runs on the current
    stream; there is no overlap between the stages.
    """
    import torch

    from ray.data.util.torch_inference import TorchInference
    from ray.data.util.torch_utils import move_tensors_to_device

    class _TorchInferenceUDFWrapper(_BaseTorchInferenceUDFWrapper):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._ti_user: TorchInference = user_cls(*args, **kwargs)
            assert isinstance(self._ti_user, TorchInference)

            self._ti_device = _resolve_cuda_device(self._ti_user)

        def __repr__(self) -> str:
            return repr(self._ti_user)

        @torch.no_grad()
        def __call__(self, batch: "DataBatch") -> "DataBatch":
            # Shallow copy: `collate` replacing keys in the batch mapping
            # can't corrupt the `input_batch` handed to process_on_device/
            # finalize (the underlying arrays are shared, not copied).
            input_batch = dict(batch) if isinstance(batch, Mapping) else batch

            collated, collated_other = split_batch_and_other(
                self._ti_user.collate(batch)
            )
            validate_collated_batch(collated, user_cls)

            moved = move_tensors_to_device(
                collated, self._ti_device, non_blocking=False
            )

            out, output_other = split_batch_and_other(
                self._ti_user.process_on_device(input_batch, moved, collated_other)
            )
            validate_processed_batch(out, self._ti_device, user_cls)

            cpu_out = move_tensors_to_device(out, "cpu", non_blocking=False)

            return self._ti_user.finalize(input_batch, cpu_out, output_other)

    # Wrapping happens before the MapBatches logical op is built, so take the
    # user's class name for operator naming (`_get_operator_name` uses
    # `fn.__name__`).
    _TorchInferenceUDFWrapper.__name__ = user_cls.__name__
    return _TorchInferenceUDFWrapper
