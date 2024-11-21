from __future__ import annotations

import collections.abc
import numbers
from contextlib import suppress

from typing import Any, Callable, Dict, List, Literal, Sequence, Tuple, Type, Union

import PIL.Image
import torch

from torchvision import tv_tensors

from torchvision._utils import sequence_to_str

from torchvision.transforms.transforms import _check_sequence_input, _setup_angle, _setup_size  # noqa: F401
from torchvision.transforms.v2.functional import get_dimensions, get_size, is_pure_tensor
from torchvision.transforms.v2.functional._utils import _FillType, _FillTypeJIT


def _setup_number_or_seq(arg: Union[int, float, Sequence[Union[int, float]]], name: str) -> Sequence[float]:
    if not isinstance(arg, (int, float, Sequence)):
        raise TypeError(f"{name} should be a number or a sequence of numbers. Got {type(arg)}")
    if isinstance(arg, Sequence) and len(arg) not in (1, 2):
        raise ValueError(f"If {name} is a sequence its length should be 1 or 2. Got {len(arg)}")
    if isinstance(arg, Sequence):
        for element in arg:
            if not isinstance(element, (int, float)):
                raise ValueError(f"{name} should be a sequence of numbers. Got {type(element)}")

    if isinstance(arg, (int, float)):
        arg = [float(arg), float(arg)]
    elif isinstance(arg, Sequence):
        if len(arg) == 1:
            arg = [float(arg[0]), float(arg[0])]
        else:
            arg = [float(arg[0]), float(arg[1])]
    return arg


def _check_fill_arg(fill: Union[_FillType, Dict[Union[Type, str], _FillType]]) -> None:
    if isinstance(fill, dict):
        for value in fill.values():
            _check_fill_arg(value)
    else:
        if fill is not None and not isinstance(fill, (numbers.Number, tuple, list)):
            raise TypeError("Got inappropriate fill arg, only Numbers, tuples, lists and dicts are allowed.")


def _convert_fill_arg(fill: _FillType) -> _FillTypeJIT:
    # Fill = 0 is not equivalent to None, https://github.com/pytorch/vision/issues/6517
    # So, we can't reassign fill to 0
    # if fill is None:
    #     fill = 0
    if fill is None:
        return fill

    if not isinstance(fill, (int, float)):
        fill = [float(v) for v in list(fill)]
    return fill  # type: ignore[return-value]


def _setup_fill_arg(fill: Union[_FillType, Dict[Union[Type, str], _FillType]]) -> Dict[Union[Type, str], _FillTypeJIT]:
    _check_fill_arg(fill)

    if isinstance(fill, dict):
        for k, v in fill.items():
            fill[k] = _convert_fill_arg(v)
        return fill  # type: ignore[return-value]
    else:
        return {"others": _convert_fill_arg(fill)}


def _get_fill(fill_dict, inpt_type):
    if inpt_type in fill_dict:
        return fill_dict[inpt_type]
    elif "others" in fill_dict:
        return fill_dict["others"]
    else:
        RuntimeError("This should never happen, please open an issue on the torchvision repo if you hit this.")


def _check_padding_arg(padding: Union[int, Sequence[int]]) -> None:
    if not isinstance(padding, (numbers.Number, tuple, list)):
        raise TypeError("Got inappropriate padding arg")

    if isinstance(padding, (tuple, list)) and len(padding) not in [1, 2, 4]:
        raise ValueError(f"Padding must be an int or a 1, 2, or 4 element tuple, not a {len(padding)} element tuple")


# TODO: let's use torchvision._utils.StrEnum to have the best of both worlds (strings and enums)
# https://github.com/pytorch/vision/issues/6250
def _check_padding_mode_arg(padding_mode: Literal["constant", "edge", "reflect", "symmetric"]) -> None:
    if padding_mode not in ["constant", "edge", "reflect", "symmetric"]:
        raise ValueError("Padding mode should be either constant, edge, reflect or symmetric")


def _find_labels_default_heuristic(inputs: Any) -> torch.Tensor:
    """
    This heuristic covers three cases:

    1. The input is tuple or list whose second item is a labels tensor. This happens for already batched
       classification inputs for MixUp and CutMix (typically after the Dataloder).
    2. The input is a tuple or list whose second item is a dictionary that contains the labels tensor
       under a label-like (see below) key. This happens for the inputs of detection models.
    3. The input is a dictionary that is structured as the one from 2.

    What is "label-like" key? We first search for an case-insensitive match of 'labels' inside the keys of the
    dictionary. This is the name our detection models expect. If we can't find that, we look for a case-insensitive
    match of the term 'label' anywhere inside the key, i.e. 'FooLaBeLBar'. If we can't find that either, the dictionary
    contains no "label-like" key.
    """

    if isinstance(inputs, (tuple, list)):
        inputs = inputs[1]

    # MixUp, CutMix
    if is_pure_tensor(inputs):
        return inputs

    if not isinstance(inputs, collections.abc.Mapping):
        raise ValueError(
            f"When using the default labels_getter, the input passed to forward must be a dictionary or a two-tuple "
            f"whose second item is a dictionary or a tensor, but got {inputs} instead."
        )

    candidate_key = None
    with suppress(StopIteration):
        candidate_key = next(key for key in inputs.keys() if key.lower() == "labels")
    if candidate_key is None:
        with suppress(StopIteration):
            candidate_key = next(key for key in inputs.keys() if "label" in key.lower())
    if candidate_key is None:
        raise ValueError(
            "Could not infer where the labels are in the sample. Try passing a callable as the labels_getter parameter?"
            "If there are no labels in the sample by design, pass labels_getter=None."
        )

    return inputs[candidate_key]


def _parse_labels_getter(labels_getter: Union[str, Callable[[Any], Any], None]) -> Callable[[Any], Any]:
    if labels_getter == "default":
        return _find_labels_default_heuristic
    elif callable(labels_getter):
        return labels_getter
    elif labels_getter is None:
        return lambda _: None
    else:
        raise ValueError(f"labels_getter should either be 'default', a callable, or None, but got {labels_getter}.")


def get_bounding_boxes(flat_inputs: List[Any]) -> tv_tensors.BoundingBoxes:
    # This assumes there is only one bbox per sample as per the general convention
    try:
        return next(inpt for inpt in flat_inputs if isinstance(inpt, tv_tensors.BoundingBoxes))
    except StopIteration:
        raise ValueError("No bounding boxes were found in the sample")


def query_chw(flat_inputs: List[Any]) -> Tuple[int, int, int]:
    chws = {
        tuple(get_dimensions(inpt))
        for inpt in flat_inputs
        if check_type(inpt, (is_pure_tensor, tv_tensors.Image, PIL.Image.Image, tv_tensors.Video))
    }
    if not chws:
        raise TypeError("No image or video was found in the sample")
    elif len(chws) > 1:
        raise ValueError(f"Found multiple CxHxW dimensions in the sample: {sequence_to_str(sorted(chws))}")
    c, h, w = chws.pop()
    return c, h, w


def query_size(flat_inputs: List[Any]) -> Tuple[int, int]:
    sizes = {
        tuple(get_size(inpt))
        for inpt in flat_inputs
        if check_type(
            inpt,
            (
                is_pure_tensor,
                tv_tensors.Image,
                PIL.Image.Image,
                tv_tensors.Video,
                tv_tensors.Mask,
                tv_tensors.BoundingBoxes,
            ),
        )
    }
    if not sizes:
        raise TypeError("No image, video, mask or bounding box was found in the sample")
    elif len(sizes) > 1:
        raise ValueError(f"Found multiple HxW dimensions in the sample: {sequence_to_str(sorted(sizes))}")
    h, w = sizes.pop()
    return h, w


def check_type(obj: Any, types_or_checks: Tuple[Union[Type, Callable[[Any], bool]], ...]) -> bool:
    for type_or_check in types_or_checks:
        if isinstance(obj, type_or_check) if isinstance(type_or_check, type) else type_or_check(obj):
            return True
    return False


def has_any(flat_inputs: List[Any], *types_or_checks: Union[Type, Callable[[Any], bool]]) -> bool:
    for inpt in flat_inputs:
        if check_type(inpt, types_or_checks):
            return True
    return False


def has_all(flat_inputs: List[Any], *types_or_checks: Union[Type, Callable[[Any], bool]]) -> bool:
    for type_or_check in types_or_checks:
        for inpt in flat_inputs:
            if isinstance(inpt, type_or_check) if isinstance(type_or_check, type) else type_or_check(inpt):
                break
        else:
            return False
    return True
