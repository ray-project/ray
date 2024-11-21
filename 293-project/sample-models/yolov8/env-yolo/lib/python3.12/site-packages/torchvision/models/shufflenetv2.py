from functools import partial
from typing import Any, Callable, List, Optional

import torch
import torch.nn as nn
from torch import Tensor

from ..transforms._presets import ImageClassification
from ..utils import _log_api_usage_once
from ._api import register_model, Weights, WeightsEnum
from ._meta import _IMAGENET_CATEGORIES
from ._utils import _ovewrite_named_param, handle_legacy_interface


__all__ = [
    "ShuffleNetV2",
    "ShuffleNet_V2_X0_5_Weights",
    "ShuffleNet_V2_X1_0_Weights",
    "ShuffleNet_V2_X1_5_Weights",
    "ShuffleNet_V2_X2_0_Weights",
    "shufflenet_v2_x0_5",
    "shufflenet_v2_x1_0",
    "shufflenet_v2_x1_5",
    "shufflenet_v2_x2_0",
]


def channel_shuffle(x: Tensor, groups: int) -> Tensor:
    batchsize, num_channels, height, width = x.size()
    channels_per_group = num_channels // groups

    # reshape
    x = x.view(batchsize, groups, channels_per_group, height, width)

    x = torch.transpose(x, 1, 2).contiguous()

    # flatten
    x = x.view(batchsize, num_channels, height, width)

    return x


class InvertedResidual(nn.Module):
    def __init__(self, inp: int, oup: int, stride: int) -> None:
        super().__init__()

        if not (1 <= stride <= 3):
            raise ValueError("illegal stride value")
        self.stride = stride

        branch_features = oup // 2
        if (self.stride == 1) and (inp != branch_features << 1):
            raise ValueError(
                f"Invalid combination of stride {stride}, inp {inp} and oup {oup} values. If stride == 1 then inp should be equal to oup // 2 << 1."
            )

        if self.stride > 1:
            self.branch1 = nn.Sequential(
                self.depthwise_conv(inp, inp, kernel_size=3, stride=self.stride, padding=1),
                nn.BatchNorm2d(inp),
                nn.Conv2d(inp, branch_features, kernel_size=1, stride=1, padding=0, bias=False),
                nn.BatchNorm2d(branch_features),
                nn.ReLU(inplace=True),
            )
        else:
            self.branch1 = nn.Sequential()

        self.branch2 = nn.Sequential(
            nn.Conv2d(
                inp if (self.stride > 1) else branch_features,
                branch_features,
                kernel_size=1,
                stride=1,
                padding=0,
                bias=False,
            ),
            nn.BatchNorm2d(branch_features),
            nn.ReLU(inplace=True),
            self.depthwise_conv(branch_features, branch_features, kernel_size=3, stride=self.stride, padding=1),
            nn.BatchNorm2d(branch_features),
            nn.Conv2d(branch_features, branch_features, kernel_size=1, stride=1, padding=0, bias=False),
            nn.BatchNorm2d(branch_features),
            nn.ReLU(inplace=True),
        )

    @staticmethod
    def depthwise_conv(
        i: int, o: int, kernel_size: int, stride: int = 1, padding: int = 0, bias: bool = False
    ) -> nn.Conv2d:
        return nn.Conv2d(i, o, kernel_size, stride, padding, bias=bias, groups=i)

    def forward(self, x: Tensor) -> Tensor:
        if self.stride == 1:
            x1, x2 = x.chunk(2, dim=1)
            out = torch.cat((x1, self.branch2(x2)), dim=1)
        else:
            out = torch.cat((self.branch1(x), self.branch2(x)), dim=1)

        out = channel_shuffle(out, 2)

        return out


class ShuffleNetV2(nn.Module):
    def __init__(
        self,
        stages_repeats: List[int],
        stages_out_channels: List[int],
        num_classes: int = 1000,
        inverted_residual: Callable[..., nn.Module] = InvertedResidual,
    ) -> None:
        super().__init__()
        _log_api_usage_once(self)

        if len(stages_repeats) != 3:
            raise ValueError("expected stages_repeats as list of 3 positive ints")
        if len(stages_out_channels) != 5:
            raise ValueError("expected stages_out_channels as list of 5 positive ints")
        self._stage_out_channels = stages_out_channels

        input_channels = 3
        output_channels = self._stage_out_channels[0]
        self.conv1 = nn.Sequential(
            nn.Conv2d(input_channels, output_channels, 3, 2, 1, bias=False),
            nn.BatchNorm2d(output_channels),
            nn.ReLU(inplace=True),
        )
        input_channels = output_channels

        self.maxpool = nn.MaxPool2d(kernel_size=3, stride=2, padding=1)

        # Static annotations for mypy
        self.stage2: nn.Sequential
        self.stage3: nn.Sequential
        self.stage4: nn.Sequential
        stage_names = [f"stage{i}" for i in [2, 3, 4]]
        for name, repeats, output_channels in zip(stage_names, stages_repeats, self._stage_out_channels[1:]):
            seq = [inverted_residual(input_channels, output_channels, 2)]
            for i in range(repeats - 1):
                seq.append(inverted_residual(output_channels, output_channels, 1))
            setattr(self, name, nn.Sequential(*seq))
            input_channels = output_channels

        output_channels = self._stage_out_channels[-1]
        self.conv5 = nn.Sequential(
            nn.Conv2d(input_channels, output_channels, 1, 1, 0, bias=False),
            nn.BatchNorm2d(output_channels),
            nn.ReLU(inplace=True),
        )

        self.fc = nn.Linear(output_channels, num_classes)

    def _forward_impl(self, x: Tensor) -> Tensor:
        # See note [TorchScript super()]
        x = self.conv1(x)
        x = self.maxpool(x)
        x = self.stage2(x)
        x = self.stage3(x)
        x = self.stage4(x)
        x = self.conv5(x)
        x = x.mean([2, 3])  # globalpool
        x = self.fc(x)
        return x

    def forward(self, x: Tensor) -> Tensor:
        return self._forward_impl(x)


def _shufflenetv2(
    weights: Optional[WeightsEnum],
    progress: bool,
    *args: Any,
    **kwargs: Any,
) -> ShuffleNetV2:
    if weights is not None:
        _ovewrite_named_param(kwargs, "num_classes", len(weights.meta["categories"]))

    model = ShuffleNetV2(*args, **kwargs)

    if weights is not None:
        model.load_state_dict(weights.get_state_dict(progress=progress, check_hash=True))

    return model


_COMMON_META = {
    "min_size": (1, 1),
    "categories": _IMAGENET_CATEGORIES,
    "recipe": "https://github.com/ericsun99/Shufflenet-v2-Pytorch",
}


class ShuffleNet_V2_X0_5_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        # Weights ported from https://github.com/ericsun99/Shufflenet-v2-Pytorch
        url="https://download.pytorch.org/models/shufflenetv2_x0.5-f707e7126e.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 1366792,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 60.552,
                    "acc@5": 81.746,
                }
            },
            "_ops": 0.04,
            "_file_size": 5.282,
            "_docs": """These weights were trained from scratch to reproduce closely the results of the paper.""",
        },
    )
    DEFAULT = IMAGENET1K_V1


class ShuffleNet_V2_X1_0_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        # Weights ported from https://github.com/ericsun99/Shufflenet-v2-Pytorch
        url="https://download.pytorch.org/models/shufflenetv2_x1-5666bf0f80.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 2278604,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 69.362,
                    "acc@5": 88.316,
                }
            },
            "_ops": 0.145,
            "_file_size": 8.791,
            "_docs": """These weights were trained from scratch to reproduce closely the results of the paper.""",
        },
    )
    DEFAULT = IMAGENET1K_V1


class ShuffleNet_V2_X1_5_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/shufflenetv2_x1_5-3c479a10.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "recipe": "https://github.com/pytorch/vision/pull/5906",
            "num_params": 3503624,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 72.996,
                    "acc@5": 91.086,
                }
            },
            "_ops": 0.296,
            "_file_size": 13.557,
            "_docs": """
                These weights were trained from scratch by using TorchVision's `new training recipe
                <https://pytorch.org/blog/how-to-train-state-of-the-art-models-using-torchvision-latest-primitives/>`_.
            """,
        },
    )
    DEFAULT = IMAGENET1K_V1


class ShuffleNet_V2_X2_0_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/shufflenetv2_x2_0-8be3c8ee.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "recipe": "https://github.com/pytorch/vision/pull/5906",
            "num_params": 7393996,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 76.230,
                    "acc@5": 93.006,
                }
            },
            "_ops": 0.583,
            "_file_size": 28.433,
            "_docs": """
                These weights were trained from scratch by using TorchVision's `new training recipe
                <https://pytorch.org/blog/how-to-train-state-of-the-art-models-using-torchvision-latest-primitives/>`_.
            """,
        },
    )
    DEFAULT = IMAGENET1K_V1


@register_model()
@handle_legacy_interface(weights=("pretrained", ShuffleNet_V2_X0_5_Weights.IMAGENET1K_V1))
def shufflenet_v2_x0_5(
    *, weights: Optional[ShuffleNet_V2_X0_5_Weights] = None, progress: bool = True, **kwargs: Any
) -> ShuffleNetV2:
    """
    Constructs a ShuffleNetV2 architecture with 0.5x output channels, as described in
    `ShuffleNet V2: Practical Guidelines for Efficient CNN Architecture Design
    <https://arxiv.org/abs/1807.11164>`__.

    Args:
        weights (:class:`~torchvision.models.ShuffleNet_V2_X0_5_Weights`, optional): The
            pretrained weights to use. See
            :class:`~torchvision.models.ShuffleNet_V2_X0_5_Weights` below for
            more details, and possible values. By default, no pre-trained
            weights are used.
        progress (bool, optional): If True, displays a progress bar of the
            download to stderr. Default is True.
        **kwargs: parameters passed to the ``torchvision.models.shufflenetv2.ShuffleNetV2``
            base class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/shufflenetv2.py>`_
            for more details about this class.

    .. autoclass:: torchvision.models.ShuffleNet_V2_X0_5_Weights
        :members:
    """
    weights = ShuffleNet_V2_X0_5_Weights.verify(weights)

    return _shufflenetv2(weights, progress, [4, 8, 4], [24, 48, 96, 192, 1024], **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", ShuffleNet_V2_X1_0_Weights.IMAGENET1K_V1))
def shufflenet_v2_x1_0(
    *, weights: Optional[ShuffleNet_V2_X1_0_Weights] = None, progress: bool = True, **kwargs: Any
) -> ShuffleNetV2:
    """
    Constructs a ShuffleNetV2 architecture with 1.0x output channels, as described in
    `ShuffleNet V2: Practical Guidelines for Efficient CNN Architecture Design
    <https://arxiv.org/abs/1807.11164>`__.

    Args:
        weights (:class:`~torchvision.models.ShuffleNet_V2_X1_0_Weights`, optional): The
            pretrained weights to use. See
            :class:`~torchvision.models.ShuffleNet_V2_X1_0_Weights` below for
            more details, and possible values. By default, no pre-trained
            weights are used.
        progress (bool, optional): If True, displays a progress bar of the
            download to stderr. Default is True.
        **kwargs: parameters passed to the ``torchvision.models.shufflenetv2.ShuffleNetV2``
            base class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/shufflenetv2.py>`_
            for more details about this class.

    .. autoclass:: torchvision.models.ShuffleNet_V2_X1_0_Weights
        :members:
    """
    weights = ShuffleNet_V2_X1_0_Weights.verify(weights)

    return _shufflenetv2(weights, progress, [4, 8, 4], [24, 116, 232, 464, 1024], **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", ShuffleNet_V2_X1_5_Weights.IMAGENET1K_V1))
def shufflenet_v2_x1_5(
    *, weights: Optional[ShuffleNet_V2_X1_5_Weights] = None, progress: bool = True, **kwargs: Any
) -> ShuffleNetV2:
    """
    Constructs a ShuffleNetV2 architecture with 1.5x output channels, as described in
    `ShuffleNet V2: Practical Guidelines for Efficient CNN Architecture Design
    <https://arxiv.org/abs/1807.11164>`__.

    Args:
        weights (:class:`~torchvision.models.ShuffleNet_V2_X1_5_Weights`, optional): The
            pretrained weights to use. See
            :class:`~torchvision.models.ShuffleNet_V2_X1_5_Weights` below for
            more details, and possible values. By default, no pre-trained
            weights are used.
        progress (bool, optional): If True, displays a progress bar of the
            download to stderr. Default is True.
        **kwargs: parameters passed to the ``torchvision.models.shufflenetv2.ShuffleNetV2``
            base class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/shufflenetv2.py>`_
            for more details about this class.

    .. autoclass:: torchvision.models.ShuffleNet_V2_X1_5_Weights
        :members:
    """
    weights = ShuffleNet_V2_X1_5_Weights.verify(weights)

    return _shufflenetv2(weights, progress, [4, 8, 4], [24, 176, 352, 704, 1024], **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", ShuffleNet_V2_X2_0_Weights.IMAGENET1K_V1))
def shufflenet_v2_x2_0(
    *, weights: Optional[ShuffleNet_V2_X2_0_Weights] = None, progress: bool = True, **kwargs: Any
) -> ShuffleNetV2:
    """
    Constructs a ShuffleNetV2 architecture with 2.0x output channels, as described in
    `ShuffleNet V2: Practical Guidelines for Efficient CNN Architecture Design
    <https://arxiv.org/abs/1807.11164>`__.

    Args:
        weights (:class:`~torchvision.models.ShuffleNet_V2_X2_0_Weights`, optional): The
            pretrained weights to use. See
            :class:`~torchvision.models.ShuffleNet_V2_X2_0_Weights` below for
            more details, and possible values. By default, no pre-trained
            weights are used.
        progress (bool, optional): If True, displays a progress bar of the
            download to stderr. Default is True.
        **kwargs: parameters passed to the ``torchvision.models.shufflenetv2.ShuffleNetV2``
            base class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/shufflenetv2.py>`_
            for more details about this class.

    .. autoclass:: torchvision.models.ShuffleNet_V2_X2_0_Weights
        :members:
    """
    weights = ShuffleNet_V2_X2_0_Weights.verify(weights)

    return _shufflenetv2(weights, progress, [4, 8, 4], [24, 244, 488, 976, 2048], **kwargs)
