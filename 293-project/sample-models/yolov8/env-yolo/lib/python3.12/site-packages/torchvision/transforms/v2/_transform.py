from __future__ import annotations

import enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import PIL.Image
import torch
from torch import nn
from torch.utils._pytree import tree_flatten, tree_unflatten
from torchvision import tv_tensors
from torchvision.transforms.v2._utils import check_type, has_any, is_pure_tensor
from torchvision.utils import _log_api_usage_once

from .functional._utils import _get_kernel


class Transform(nn.Module):

    # Class attribute defining transformed types. Other types are passed-through without any transformation
    # We support both Types and callables that are able to do further checks on the type of the input.
    _transformed_types: Tuple[Union[Type, Callable[[Any], bool]], ...] = (torch.Tensor, PIL.Image.Image)

    def __init__(self) -> None:
        super().__init__()
        _log_api_usage_once(self)

    def _check_inputs(self, flat_inputs: List[Any]) -> None:
        pass

    def _get_params(self, flat_inputs: List[Any]) -> Dict[str, Any]:
        return dict()

    def _call_kernel(self, functional: Callable, inpt: Any, *args: Any, **kwargs: Any) -> Any:
        kernel = _get_kernel(functional, type(inpt), allow_passthrough=True)
        return kernel(inpt, *args, **kwargs)

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        raise NotImplementedError

    def forward(self, *inputs: Any) -> Any:
        flat_inputs, spec = tree_flatten(inputs if len(inputs) > 1 else inputs[0])

        self._check_inputs(flat_inputs)

        needs_transform_list = self._needs_transform_list(flat_inputs)
        params = self._get_params(
            [inpt for (inpt, needs_transform) in zip(flat_inputs, needs_transform_list) if needs_transform]
        )

        flat_outputs = [
            self._transform(inpt, params) if needs_transform else inpt
            for (inpt, needs_transform) in zip(flat_inputs, needs_transform_list)
        ]

        return tree_unflatten(flat_outputs, spec)

    def _needs_transform_list(self, flat_inputs: List[Any]) -> List[bool]:
        # Below is a heuristic on how to deal with pure tensor inputs:
        # 1. Pure tensors, i.e. tensors that are not a tv_tensor, are passed through if there is an explicit image
        #    (`tv_tensors.Image` or `PIL.Image.Image`) or video (`tv_tensors.Video`) in the sample.
        # 2. If there is no explicit image or video in the sample, only the first encountered pure tensor is
        #    transformed as image, while the rest is passed through. The order is defined by the returned `flat_inputs`
        #    of `tree_flatten`, which recurses depth-first through the input.
        #
        # This heuristic stems from two requirements:
        # 1. We need to keep BC for single input pure tensors and treat them as images.
        # 2. We don't want to treat all pure tensors as images, because some datasets like `CelebA` or `Widerface`
        #    return supplemental numerical data as tensors that cannot be transformed as images.
        #
        # The heuristic should work well for most people in practice. The only case where it doesn't is if someone
        # tries to transform multiple pure tensors at the same time, expecting them all to be treated as images.
        # However, this case wasn't supported by transforms v1 either, so there is no BC concern.

        needs_transform_list = []
        transform_pure_tensor = not has_any(flat_inputs, tv_tensors.Image, tv_tensors.Video, PIL.Image.Image)
        for inpt in flat_inputs:
            needs_transform = True

            if not check_type(inpt, self._transformed_types):
                needs_transform = False
            elif is_pure_tensor(inpt):
                if transform_pure_tensor:
                    transform_pure_tensor = False
                else:
                    needs_transform = False
            needs_transform_list.append(needs_transform)
        return needs_transform_list

    def extra_repr(self) -> str:
        extra = []
        for name, value in self.__dict__.items():
            if name.startswith("_") or name == "training":
                continue

            if not isinstance(value, (bool, int, float, str, tuple, list, enum.Enum)):
                continue

            extra.append(f"{name}={value}")

        return ", ".join(extra)

    # This attribute should be set on all transforms that have a v1 equivalent. Doing so enables two things:
    # 1. In case the v1 transform has a static `get_params` method, it will also be available under the same name on
    #    the v2 transform. See `__init_subclass__` for details.
    # 2. The v2 transform will be JIT scriptable. See `_extract_params_for_v1_transform` and `__prepare_scriptable__`
    #    for details.
    _v1_transform_cls: Optional[Type[nn.Module]] = None

    def __init_subclass__(cls) -> None:
        # Since `get_params` is a `@staticmethod`, we have to bind it to the class itself rather than to an instance.
        # This method is called after subclassing has happened, i.e. `cls` is the subclass, e.g. `Resize`.
        if cls._v1_transform_cls is not None and hasattr(cls._v1_transform_cls, "get_params"):
            cls.get_params = staticmethod(cls._v1_transform_cls.get_params)  # type: ignore[attr-defined]

    def _extract_params_for_v1_transform(self) -> Dict[str, Any]:
        # This method is called by `__prepare_scriptable__` to instantiate the equivalent v1 transform from the current
        # v2 transform instance. It extracts all available public attributes that are specific to that transform and
        # not `nn.Module` in general.
        # Overwrite this method on the v2 transform class if the above is not sufficient. For example, this might happen
        # if the v2 transform introduced new parameters that are not support by the v1 transform.
        common_attrs = nn.Module().__dict__.keys()
        return {
            attr: value
            for attr, value in self.__dict__.items()
            if not attr.startswith("_") and attr not in common_attrs
        }

    def __prepare_scriptable__(self) -> nn.Module:
        # This method is called early on when `torch.jit.script`'ing an `nn.Module` instance. If it succeeds, the return
        # value is used for scripting over the original object that should have been scripted. Since the v1 transforms
        # are JIT scriptable, and we made sure that for single image inputs v1 and v2 are equivalent, we just return the
        # equivalent v1 transform here. This of course only makes transforms v2 JIT scriptable as long as transforms v1
        # is around.
        if self._v1_transform_cls is None:
            raise RuntimeError(
                f"Transform {type(self).__name__} cannot be JIT scripted. "
                "torchscript is only supported for backward compatibility with transforms "
                "which are already in torchvision.transforms. "
                "For torchscript support (on tensors only), you can use the functional API instead."
            )

        return self._v1_transform_cls(**self._extract_params_for_v1_transform())


class _RandomApplyTransform(Transform):
    def __init__(self, p: float = 0.5) -> None:
        if not (0.0 <= p <= 1.0):
            raise ValueError("`p` should be a floating point value in the interval [0.0, 1.0].")

        super().__init__()
        self.p = p

    def forward(self, *inputs: Any) -> Any:
        # We need to almost duplicate `Transform.forward()` here since we always want to check the inputs, but return
        # early afterwards in case the random check triggers. The same result could be achieved by calling
        # `super().forward()` after the random check, but that would call `self._check_inputs` twice.

        inputs = inputs if len(inputs) > 1 else inputs[0]
        flat_inputs, spec = tree_flatten(inputs)

        self._check_inputs(flat_inputs)

        if torch.rand(1) >= self.p:
            return inputs

        needs_transform_list = self._needs_transform_list(flat_inputs)
        params = self._get_params(
            [inpt for (inpt, needs_transform) in zip(flat_inputs, needs_transform_list) if needs_transform]
        )

        flat_outputs = [
            self._transform(inpt, params) if needs_transform else inpt
            for (inpt, needs_transform) in zip(flat_inputs, needs_transform_list)
        ]

        return tree_unflatten(flat_outputs, spec)
