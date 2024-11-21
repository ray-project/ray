from typing import Any, Callable, Dict, List, Optional, Sequence, Union

import torch

from torch import nn
from torchvision import transforms as _transforms
from torchvision.transforms.v2 import Transform


class Compose(Transform):
    """Composes several transforms together.

    This transform does not support torchscript.
    Please, see the note below.

    Args:
        transforms (list of ``Transform`` objects): list of transforms to compose.

    Example:
        >>> transforms.Compose([
        >>>     transforms.CenterCrop(10),
        >>>     transforms.PILToTensor(),
        >>>     transforms.ConvertImageDtype(torch.float),
        >>> ])

    .. note::
        In order to script the transformations, please use ``torch.nn.Sequential`` as below.

        >>> transforms = torch.nn.Sequential(
        >>>     transforms.CenterCrop(10),
        >>>     transforms.Normalize((0.485, 0.456, 0.406), (0.229, 0.224, 0.225)),
        >>> )
        >>> scripted_transforms = torch.jit.script(transforms)

        Make sure to use only scriptable transformations, i.e. that work with ``torch.Tensor``, does not require
        `lambda` functions or ``PIL.Image``.

    """

    def __init__(self, transforms: Sequence[Callable]) -> None:
        super().__init__()
        if not isinstance(transforms, Sequence):
            raise TypeError("Argument transforms should be a sequence of callables")
        elif not transforms:
            raise ValueError("Pass at least one transform")
        self.transforms = transforms

    def forward(self, *inputs: Any) -> Any:
        needs_unpacking = len(inputs) > 1
        for transform in self.transforms:
            outputs = transform(*inputs)
            inputs = outputs if needs_unpacking else (outputs,)
        return outputs

    def extra_repr(self) -> str:
        format_string = []
        for t in self.transforms:
            format_string.append(f"    {t}")
        return "\n".join(format_string)


class RandomApply(Transform):
    """Apply randomly a list of transformations with a given probability.

    .. note::
        In order to script the transformation, please use ``torch.nn.ModuleList`` as input instead of list/tuple of
        transforms as shown below:

        >>> transforms = transforms.RandomApply(torch.nn.ModuleList([
        >>>     transforms.ColorJitter(),
        >>> ]), p=0.3)
        >>> scripted_transforms = torch.jit.script(transforms)

        Make sure to use only scriptable transformations, i.e. that work with ``torch.Tensor``, does not require
        `lambda` functions or ``PIL.Image``.

    Args:
        transforms (sequence or torch.nn.Module): list of transformations
        p (float): probability of applying the list of transforms
    """

    _v1_transform_cls = _transforms.RandomApply

    def __init__(self, transforms: Union[Sequence[Callable], nn.ModuleList], p: float = 0.5) -> None:
        super().__init__()

        if not isinstance(transforms, (Sequence, nn.ModuleList)):
            raise TypeError("Argument transforms should be a sequence of callables or a `nn.ModuleList`")
        self.transforms = transforms

        if not (0.0 <= p <= 1.0):
            raise ValueError("`p` should be a floating point value in the interval [0.0, 1.0].")
        self.p = p

    def _extract_params_for_v1_transform(self) -> Dict[str, Any]:
        return {"transforms": self.transforms, "p": self.p}

    def forward(self, *inputs: Any) -> Any:
        needs_unpacking = len(inputs) > 1

        if torch.rand(1) >= self.p:
            return inputs if needs_unpacking else inputs[0]

        for transform in self.transforms:
            outputs = transform(*inputs)
            inputs = outputs if needs_unpacking else (outputs,)
        return outputs

    def extra_repr(self) -> str:
        format_string = []
        for t in self.transforms:
            format_string.append(f"    {t}")
        return "\n".join(format_string)


class RandomChoice(Transform):
    """Apply single transformation randomly picked from a list.

    This transform does not support torchscript.

    Args:
        transforms (sequence or torch.nn.Module): list of transformations
        p (list of floats or None, optional): probability of each transform being picked.
            If ``p`` doesn't sum to 1, it is automatically normalized. If ``None``
            (default), all transforms have the same probability.
    """

    def __init__(
        self,
        transforms: Sequence[Callable],
        p: Optional[List[float]] = None,
    ) -> None:
        if not isinstance(transforms, Sequence):
            raise TypeError("Argument transforms should be a sequence of callables")

        if p is None:
            p = [1] * len(transforms)
        elif len(p) != len(transforms):
            raise ValueError(f"Length of p doesn't match the number of transforms: {len(p)} != {len(transforms)}")

        super().__init__()

        self.transforms = transforms
        total = sum(p)
        self.p = [prob / total for prob in p]

    def forward(self, *inputs: Any) -> Any:
        idx = int(torch.multinomial(torch.tensor(self.p), 1))
        transform = self.transforms[idx]
        return transform(*inputs)


class RandomOrder(Transform):
    """Apply a list of transformations in a random order.

    This transform does not support torchscript.

    Args:
        transforms (sequence or torch.nn.Module): list of transformations
    """

    def __init__(self, transforms: Sequence[Callable]) -> None:
        if not isinstance(transforms, Sequence):
            raise TypeError("Argument transforms should be a sequence of callables")
        super().__init__()
        self.transforms = transforms

    def forward(self, *inputs: Any) -> Any:
        needs_unpacking = len(inputs) > 1
        for idx in torch.randperm(len(self.transforms)):
            transform = self.transforms[idx]
            outputs = transform(*inputs)
            inputs = outputs if needs_unpacking else (outputs,)
        return outputs
