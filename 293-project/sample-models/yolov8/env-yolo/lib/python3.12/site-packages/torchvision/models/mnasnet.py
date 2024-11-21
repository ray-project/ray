import warnings
from functools import partial
from typing import Any, Dict, List, Optional

import torch
import torch.nn as nn
from torch import Tensor

from ..transforms._presets import ImageClassification
from ..utils import _log_api_usage_once
from ._api import register_model, Weights, WeightsEnum
from ._meta import _IMAGENET_CATEGORIES
from ._utils import _ovewrite_named_param, handle_legacy_interface


__all__ = [
    "MNASNet",
    "MNASNet0_5_Weights",
    "MNASNet0_75_Weights",
    "MNASNet1_0_Weights",
    "MNASNet1_3_Weights",
    "mnasnet0_5",
    "mnasnet0_75",
    "mnasnet1_0",
    "mnasnet1_3",
]


# Paper suggests 0.9997 momentum, for TensorFlow. Equivalent PyTorch momentum is
# 1.0 - tensorflow.
_BN_MOMENTUM = 1 - 0.9997


class _InvertedResidual(nn.Module):
    def __init__(
        self, in_ch: int, out_ch: int, kernel_size: int, stride: int, expansion_factor: int, bn_momentum: float = 0.1
    ) -> None:
        super().__init__()
        if stride not in [1, 2]:
            raise ValueError(f"stride should be 1 or 2 instead of {stride}")
        if kernel_size not in [3, 5]:
            raise ValueError(f"kernel_size should be 3 or 5 instead of {kernel_size}")
        mid_ch = in_ch * expansion_factor
        self.apply_residual = in_ch == out_ch and stride == 1
        self.layers = nn.Sequential(
            # Pointwise
            nn.Conv2d(in_ch, mid_ch, 1, bias=False),
            nn.BatchNorm2d(mid_ch, momentum=bn_momentum),
            nn.ReLU(inplace=True),
            # Depthwise
            nn.Conv2d(mid_ch, mid_ch, kernel_size, padding=kernel_size // 2, stride=stride, groups=mid_ch, bias=False),
            nn.BatchNorm2d(mid_ch, momentum=bn_momentum),
            nn.ReLU(inplace=True),
            # Linear pointwise. Note that there's no activation.
            nn.Conv2d(mid_ch, out_ch, 1, bias=False),
            nn.BatchNorm2d(out_ch, momentum=bn_momentum),
        )

    def forward(self, input: Tensor) -> Tensor:
        if self.apply_residual:
            return self.layers(input) + input
        else:
            return self.layers(input)


def _stack(
    in_ch: int, out_ch: int, kernel_size: int, stride: int, exp_factor: int, repeats: int, bn_momentum: float
) -> nn.Sequential:
    """Creates a stack of inverted residuals."""
    if repeats < 1:
        raise ValueError(f"repeats should be >= 1, instead got {repeats}")
    # First one has no skip, because feature map size changes.
    first = _InvertedResidual(in_ch, out_ch, kernel_size, stride, exp_factor, bn_momentum=bn_momentum)
    remaining = []
    for _ in range(1, repeats):
        remaining.append(_InvertedResidual(out_ch, out_ch, kernel_size, 1, exp_factor, bn_momentum=bn_momentum))
    return nn.Sequential(first, *remaining)


def _round_to_multiple_of(val: float, divisor: int, round_up_bias: float = 0.9) -> int:
    """Asymmetric rounding to make `val` divisible by `divisor`. With default
    bias, will round up, unless the number is no more than 10% greater than the
    smaller divisible value, i.e. (83, 8) -> 80, but (84, 8) -> 88."""
    if not 0.0 < round_up_bias < 1.0:
        raise ValueError(f"round_up_bias should be greater than 0.0 and smaller than 1.0 instead of {round_up_bias}")
    new_val = max(divisor, int(val + divisor / 2) // divisor * divisor)
    return new_val if new_val >= round_up_bias * val else new_val + divisor


def _get_depths(alpha: float) -> List[int]:
    """Scales tensor depths as in reference MobileNet code, prefers rounding up
    rather than down."""
    depths = [32, 16, 24, 40, 80, 96, 192, 320]
    return [_round_to_multiple_of(depth * alpha, 8) for depth in depths]


class MNASNet(torch.nn.Module):
    """MNASNet, as described in https://arxiv.org/abs/1807.11626. This
    implements the B1 variant of the model.
    >>> model = MNASNet(1.0, num_classes=1000)
    >>> x = torch.rand(1, 3, 224, 224)
    >>> y = model(x)
    >>> y.dim()
    2
    >>> y.nelement()
    1000
    """

    # Version 2 adds depth scaling in the initial stages of the network.
    _version = 2

    def __init__(self, alpha: float, num_classes: int = 1000, dropout: float = 0.2) -> None:
        super().__init__()
        _log_api_usage_once(self)
        if alpha <= 0.0:
            raise ValueError(f"alpha should be greater than 0.0 instead of {alpha}")
        self.alpha = alpha
        self.num_classes = num_classes
        depths = _get_depths(alpha)
        layers = [
            # First layer: regular conv.
            nn.Conv2d(3, depths[0], 3, padding=1, stride=2, bias=False),
            nn.BatchNorm2d(depths[0], momentum=_BN_MOMENTUM),
            nn.ReLU(inplace=True),
            # Depthwise separable, no skip.
            nn.Conv2d(depths[0], depths[0], 3, padding=1, stride=1, groups=depths[0], bias=False),
            nn.BatchNorm2d(depths[0], momentum=_BN_MOMENTUM),
            nn.ReLU(inplace=True),
            nn.Conv2d(depths[0], depths[1], 1, padding=0, stride=1, bias=False),
            nn.BatchNorm2d(depths[1], momentum=_BN_MOMENTUM),
            # MNASNet blocks: stacks of inverted residuals.
            _stack(depths[1], depths[2], 3, 2, 3, 3, _BN_MOMENTUM),
            _stack(depths[2], depths[3], 5, 2, 3, 3, _BN_MOMENTUM),
            _stack(depths[3], depths[4], 5, 2, 6, 3, _BN_MOMENTUM),
            _stack(depths[4], depths[5], 3, 1, 6, 2, _BN_MOMENTUM),
            _stack(depths[5], depths[6], 5, 2, 6, 4, _BN_MOMENTUM),
            _stack(depths[6], depths[7], 3, 1, 6, 1, _BN_MOMENTUM),
            # Final mapping to classifier input.
            nn.Conv2d(depths[7], 1280, 1, padding=0, stride=1, bias=False),
            nn.BatchNorm2d(1280, momentum=_BN_MOMENTUM),
            nn.ReLU(inplace=True),
        ]
        self.layers = nn.Sequential(*layers)
        self.classifier = nn.Sequential(nn.Dropout(p=dropout, inplace=True), nn.Linear(1280, num_classes))

        for m in self.modules():
            if isinstance(m, nn.Conv2d):
                nn.init.kaiming_normal_(m.weight, mode="fan_out", nonlinearity="relu")
                if m.bias is not None:
                    nn.init.zeros_(m.bias)
            elif isinstance(m, nn.BatchNorm2d):
                nn.init.ones_(m.weight)
                nn.init.zeros_(m.bias)
            elif isinstance(m, nn.Linear):
                nn.init.kaiming_uniform_(m.weight, mode="fan_out", nonlinearity="sigmoid")
                nn.init.zeros_(m.bias)

    def forward(self, x: Tensor) -> Tensor:
        x = self.layers(x)
        # Equivalent to global avgpool and removing H and W dimensions.
        x = x.mean([2, 3])
        return self.classifier(x)

    def _load_from_state_dict(
        self,
        state_dict: Dict,
        prefix: str,
        local_metadata: Dict,
        strict: bool,
        missing_keys: List[str],
        unexpected_keys: List[str],
        error_msgs: List[str],
    ) -> None:
        version = local_metadata.get("version", None)
        if version not in [1, 2]:
            raise ValueError(f"version shluld be set to 1 or 2 instead of {version}")

        if version == 1 and not self.alpha == 1.0:
            # In the initial version of the model (v1), stem was fixed-size.
            # All other layer configurations were the same. This will patch
            # the model so that it's identical to v1. Model with alpha 1.0 is
            # unaffected.
            depths = _get_depths(self.alpha)
            v1_stem = [
                nn.Conv2d(3, 32, 3, padding=1, stride=2, bias=False),
                nn.BatchNorm2d(32, momentum=_BN_MOMENTUM),
                nn.ReLU(inplace=True),
                nn.Conv2d(32, 32, 3, padding=1, stride=1, groups=32, bias=False),
                nn.BatchNorm2d(32, momentum=_BN_MOMENTUM),
                nn.ReLU(inplace=True),
                nn.Conv2d(32, 16, 1, padding=0, stride=1, bias=False),
                nn.BatchNorm2d(16, momentum=_BN_MOMENTUM),
                _stack(16, depths[2], 3, 2, 3, 3, _BN_MOMENTUM),
            ]
            for idx, layer in enumerate(v1_stem):
                self.layers[idx] = layer

            # The model is now identical to v1, and must be saved as such.
            self._version = 1
            warnings.warn(
                "A new version of MNASNet model has been implemented. "
                "Your checkpoint was saved using the previous version. "
                "This checkpoint will load and work as before, but "
                "you may want to upgrade by training a newer model or "
                "transfer learning from an updated ImageNet checkpoint.",
                UserWarning,
            )

        super()._load_from_state_dict(
            state_dict, prefix, local_metadata, strict, missing_keys, unexpected_keys, error_msgs
        )


_COMMON_META = {
    "min_size": (1, 1),
    "categories": _IMAGENET_CATEGORIES,
    "recipe": "https://github.com/1e100/mnasnet_trainer",
}


class MNASNet0_5_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/mnasnet0.5_top1_67.823-3ffadce67e.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 2218512,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 67.734,
                    "acc@5": 87.490,
                }
            },
            "_ops": 0.104,
            "_file_size": 8.591,
            "_docs": """These weights reproduce closely the results of the paper.""",
        },
    )
    DEFAULT = IMAGENET1K_V1


class MNASNet0_75_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/mnasnet0_75-7090bc5f.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "recipe": "https://github.com/pytorch/vision/pull/6019",
            "num_params": 3170208,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 71.180,
                    "acc@5": 90.496,
                }
            },
            "_ops": 0.215,
            "_file_size": 12.303,
            "_docs": """
                These weights were trained from scratch by using TorchVision's `new training recipe
                <https://pytorch.org/blog/how-to-train-state-of-the-art-models-using-torchvision-latest-primitives/>`_.
            """,
        },
    )
    DEFAULT = IMAGENET1K_V1


class MNASNet1_0_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/mnasnet1.0_top1_73.512-f206786ef8.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 4383312,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 73.456,
                    "acc@5": 91.510,
                }
            },
            "_ops": 0.314,
            "_file_size": 16.915,
            "_docs": """These weights reproduce closely the results of the paper.""",
        },
    )
    DEFAULT = IMAGENET1K_V1


class MNASNet1_3_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/mnasnet1_3-a4c69d6f.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "recipe": "https://github.com/pytorch/vision/pull/6019",
            "num_params": 6282256,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 76.506,
                    "acc@5": 93.522,
                }
            },
            "_ops": 0.526,
            "_file_size": 24.246,
            "_docs": """
                These weights were trained from scratch by using TorchVision's `new training recipe
                <https://pytorch.org/blog/how-to-train-state-of-the-art-models-using-torchvision-latest-primitives/>`_.
            """,
        },
    )
    DEFAULT = IMAGENET1K_V1


def _mnasnet(alpha: float, weights: Optional[WeightsEnum], progress: bool, **kwargs: Any) -> MNASNet:
    if weights is not None:
        _ovewrite_named_param(kwargs, "num_classes", len(weights.meta["categories"]))

    model = MNASNet(alpha, **kwargs)

    if weights:
        model.load_state_dict(weights.get_state_dict(progress=progress, check_hash=True))

    return model


@register_model()
@handle_legacy_interface(weights=("pretrained", MNASNet0_5_Weights.IMAGENET1K_V1))
def mnasnet0_5(*, weights: Optional[MNASNet0_5_Weights] = None, progress: bool = True, **kwargs: Any) -> MNASNet:
    """MNASNet with depth multiplier of 0.5 from
    `MnasNet: Platform-Aware Neural Architecture Search for Mobile
    <https://arxiv.org/abs/1807.11626>`_ paper.

    Args:
        weights (:class:`~torchvision.models.MNASNet0_5_Weights`, optional): The
            pretrained weights to use. See
            :class:`~torchvision.models.MNASNet0_5_Weights` below for
            more details, and possible values. By default, no pre-trained
            weights are used.
        progress (bool, optional): If True, displays a progress bar of the
            download to stderr. Default is True.
        **kwargs: parameters passed to the ``torchvision.models.mnasnet.MNASNet``
            base class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/mnasnet.py>`_
            for more details about this class.

    .. autoclass:: torchvision.models.MNASNet0_5_Weights
        :members:
    """
    weights = MNASNet0_5_Weights.verify(weights)

    return _mnasnet(0.5, weights, progress, **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", MNASNet0_75_Weights.IMAGENET1K_V1))
def mnasnet0_75(*, weights: Optional[MNASNet0_75_Weights] = None, progress: bool = True, **kwargs: Any) -> MNASNet:
    """MNASNet with depth multiplier of 0.75 from
    `MnasNet: Platform-Aware Neural Architecture Search for Mobile
    <https://arxiv.org/abs/1807.11626>`_ paper.

    Args:
        weights (:class:`~torchvision.models.MNASNet0_75_Weights`, optional): The
            pretrained weights to use. See
            :class:`~torchvision.models.MNASNet0_75_Weights` below for
            more details, and possible values. By default, no pre-trained
            weights are used.
        progress (bool, optional): If True, displays a progress bar of the
            download to stderr. Default is True.
        **kwargs: parameters passed to the ``torchvision.models.mnasnet.MNASNet``
            base class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/mnasnet.py>`_
            for more details about this class.

    .. autoclass:: torchvision.models.MNASNet0_75_Weights
        :members:
    """
    weights = MNASNet0_75_Weights.verify(weights)

    return _mnasnet(0.75, weights, progress, **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", MNASNet1_0_Weights.IMAGENET1K_V1))
def mnasnet1_0(*, weights: Optional[MNASNet1_0_Weights] = None, progress: bool = True, **kwargs: Any) -> MNASNet:
    """MNASNet with depth multiplier of 1.0 from
    `MnasNet: Platform-Aware Neural Architecture Search for Mobile
    <https://arxiv.org/abs/1807.11626>`_ paper.

    Args:
        weights (:class:`~torchvision.models.MNASNet1_0_Weights`, optional): The
            pretrained weights to use. See
            :class:`~torchvision.models.MNASNet1_0_Weights` below for
            more details, and possible values. By default, no pre-trained
            weights are used.
        progress (bool, optional): If True, displays a progress bar of the
            download to stderr. Default is True.
        **kwargs: parameters passed to the ``torchvision.models.mnasnet.MNASNet``
            base class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/mnasnet.py>`_
            for more details about this class.

    .. autoclass:: torchvision.models.MNASNet1_0_Weights
        :members:
    """
    weights = MNASNet1_0_Weights.verify(weights)

    return _mnasnet(1.0, weights, progress, **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", MNASNet1_3_Weights.IMAGENET1K_V1))
def mnasnet1_3(*, weights: Optional[MNASNet1_3_Weights] = None, progress: bool = True, **kwargs: Any) -> MNASNet:
    """MNASNet with depth multiplier of 1.3 from
    `MnasNet: Platform-Aware Neural Architecture Search for Mobile
    <https://arxiv.org/abs/1807.11626>`_ paper.

    Args:
        weights (:class:`~torchvision.models.MNASNet1_3_Weights`, optional): The
            pretrained weights to use. See
            :class:`~torchvision.models.MNASNet1_3_Weights` below for
            more details, and possible values. By default, no pre-trained
            weights are used.
        progress (bool, optional): If True, displays a progress bar of the
            download to stderr. Default is True.
        **kwargs: parameters passed to the ``torchvision.models.mnasnet.MNASNet``
            base class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/mnasnet.py>`_
            for more details about this class.

    .. autoclass:: torchvision.models.MNASNet1_3_Weights
        :members:
    """
    weights = MNASNet1_3_Weights.verify(weights)

    return _mnasnet(1.3, weights, progress, **kwargs)
