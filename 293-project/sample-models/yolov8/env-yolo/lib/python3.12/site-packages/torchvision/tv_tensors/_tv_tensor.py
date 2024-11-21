from __future__ import annotations

from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple, Type, TypeVar, Union

import torch
from torch._C import DisableTorchFunctionSubclass
from torch.types import _device, _dtype, _size

from torchvision.tv_tensors._torch_function_helpers import _FORCE_TORCHFUNCTION_SUBCLASS, _must_return_subclass


D = TypeVar("D", bound="TVTensor")


class TVTensor(torch.Tensor):
    """Base class for all TVTensors.

    You probably don't want to use this class unless you're defining your own
    custom TVTensors. See
    :ref:`sphx_glr_auto_examples_transforms_plot_custom_tv_tensors.py` for details.
    """

    @staticmethod
    def _to_tensor(
        data: Any,
        dtype: Optional[torch.dtype] = None,
        device: Optional[Union[torch.device, str, int]] = None,
        requires_grad: Optional[bool] = None,
    ) -> torch.Tensor:
        if requires_grad is None:
            requires_grad = data.requires_grad if isinstance(data, torch.Tensor) else False
        return torch.as_tensor(data, dtype=dtype, device=device).requires_grad_(requires_grad)

    @classmethod
    def _wrap_output(
        cls,
        output: torch.Tensor,
        args: Sequence[Any] = (),
        kwargs: Optional[Mapping[str, Any]] = None,
    ) -> torch.Tensor:
        # Same as torch._tensor._convert
        if isinstance(output, torch.Tensor) and not isinstance(output, cls):
            output = output.as_subclass(cls)

        if isinstance(output, (tuple, list)):
            # Also handles things like namedtuples
            output = type(output)(cls._wrap_output(part, args, kwargs) for part in output)
        return output

    @classmethod
    def __torch_function__(
        cls,
        func: Callable[..., torch.Tensor],
        types: Tuple[Type[torch.Tensor], ...],
        args: Sequence[Any] = (),
        kwargs: Optional[Mapping[str, Any]] = None,
    ) -> torch.Tensor:
        """For general information about how the __torch_function__ protocol works,
        see https://pytorch.org/docs/stable/notes/extending.html#extending-torch

        TL;DR: Every time a PyTorch operator is called, it goes through the inputs and looks for the
        ``__torch_function__`` method. If one is found, it is invoked with the operator as ``func`` as well as the
        ``args`` and ``kwargs`` of the original call.

        Why do we override this? Because the base implementation in torch.Tensor would preserve the TVTensor type
        of the output. In our case, we want to return pure tensors instead (with a few exceptions). Refer to the
        "TVTensors FAQ" gallery example for a rationale of this behaviour (TL;DR: perf + no silver bullet).

        Our implementation below is very similar to the base implementation in ``torch.Tensor`` - go check it out.
        """
        if not all(issubclass(cls, t) for t in types):
            return NotImplemented

        # Like in the base Tensor.__torch_function__ implementation, it's easier to always use
        # DisableTorchFunctionSubclass and then manually re-wrap the output if necessary
        with DisableTorchFunctionSubclass():
            output = func(*args, **kwargs or dict())

        must_return_subclass = _must_return_subclass()
        if must_return_subclass or (func in _FORCE_TORCHFUNCTION_SUBCLASS and isinstance(args[0], cls)):
            # If you're wondering why we need the `isinstance(args[0], cls)` check, remove it and see what fails
            # in test_to_tv_tensor_reference().
            # The __torch_function__ protocol will invoke the __torch_function__ method on *all* types involved in
            # the computation by walking the MRO upwards. For example,
            # `out = a_pure_tensor.to(an_image)` will invoke `Image.__torch_function__` with
            # `args = (a_pure_tensor, an_image)` first. Without this guard, `out` would
            # be wrapped into an `Image`.
            return cls._wrap_output(output, args, kwargs)

        if not must_return_subclass and isinstance(output, cls):
            # DisableTorchFunctionSubclass is ignored by inplace ops like `.add_(...)`,
            # so for those, the output is still a TVTensor. Thus, we need to manually unwrap.
            return output.as_subclass(torch.Tensor)

        return output

    def _make_repr(self, **kwargs: Any) -> str:
        # This is a poor man's implementation of the proposal in https://github.com/pytorch/pytorch/issues/76532.
        # If that ever gets implemented, remove this in favor of the solution on the `torch.Tensor` class.
        extra_repr = ", ".join(f"{key}={value}" for key, value in kwargs.items())
        return f"{super().__repr__()[:-1]}, {extra_repr})"

    # Add properties for common attributes like shape, dtype, device, ndim etc
    # this way we return the result without passing into __torch_function__
    @property
    def shape(self) -> _size:  # type: ignore[override]
        with DisableTorchFunctionSubclass():
            return super().shape

    @property
    def ndim(self) -> int:  # type: ignore[override]
        with DisableTorchFunctionSubclass():
            return super().ndim

    @property
    def device(self, *args: Any, **kwargs: Any) -> _device:  # type: ignore[override]
        with DisableTorchFunctionSubclass():
            return super().device

    @property
    def dtype(self) -> _dtype:  # type: ignore[override]
        with DisableTorchFunctionSubclass():
            return super().dtype

    def __deepcopy__(self: D, memo: Dict[int, Any]) -> D:
        # We need to detach first, since a plain `Tensor.clone` will be part of the computation graph, which does
        # *not* happen for `deepcopy(Tensor)`. A side-effect from detaching is that the `Tensor.requires_grad`
        # attribute is cleared, so we need to refill it before we return.
        # Note: We don't explicitly handle deep-copying of the metadata here. The only metadata we currently have is
        # `BoundingBoxes.format` and `BoundingBoxes.canvas_size`, which are immutable and thus implicitly deep-copied by
        # `BoundingBoxes.clone()`.
        return self.detach().clone().requires_grad_(self.requires_grad)  # type: ignore[return-value]
