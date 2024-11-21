import math
from collections import OrderedDict
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Tuple

import torch
from torch import nn, Tensor

from ..ops.misc import Conv2dNormActivation, SqueezeExcitation
from ..transforms._presets import ImageClassification, InterpolationMode
from ..utils import _log_api_usage_once
from ._api import register_model, Weights, WeightsEnum
from ._meta import _IMAGENET_CATEGORIES
from ._utils import _make_divisible, _ovewrite_named_param, handle_legacy_interface


__all__ = [
    "RegNet",
    "RegNet_Y_400MF_Weights",
    "RegNet_Y_800MF_Weights",
    "RegNet_Y_1_6GF_Weights",
    "RegNet_Y_3_2GF_Weights",
    "RegNet_Y_8GF_Weights",
    "RegNet_Y_16GF_Weights",
    "RegNet_Y_32GF_Weights",
    "RegNet_Y_128GF_Weights",
    "RegNet_X_400MF_Weights",
    "RegNet_X_800MF_Weights",
    "RegNet_X_1_6GF_Weights",
    "RegNet_X_3_2GF_Weights",
    "RegNet_X_8GF_Weights",
    "RegNet_X_16GF_Weights",
    "RegNet_X_32GF_Weights",
    "regnet_y_400mf",
    "regnet_y_800mf",
    "regnet_y_1_6gf",
    "regnet_y_3_2gf",
    "regnet_y_8gf",
    "regnet_y_16gf",
    "regnet_y_32gf",
    "regnet_y_128gf",
    "regnet_x_400mf",
    "regnet_x_800mf",
    "regnet_x_1_6gf",
    "regnet_x_3_2gf",
    "regnet_x_8gf",
    "regnet_x_16gf",
    "regnet_x_32gf",
]


class SimpleStemIN(Conv2dNormActivation):
    """Simple stem for ImageNet: 3x3, BN, ReLU."""

    def __init__(
        self,
        width_in: int,
        width_out: int,
        norm_layer: Callable[..., nn.Module],
        activation_layer: Callable[..., nn.Module],
    ) -> None:
        super().__init__(
            width_in, width_out, kernel_size=3, stride=2, norm_layer=norm_layer, activation_layer=activation_layer
        )


class BottleneckTransform(nn.Sequential):
    """Bottleneck transformation: 1x1, 3x3 [+SE], 1x1."""

    def __init__(
        self,
        width_in: int,
        width_out: int,
        stride: int,
        norm_layer: Callable[..., nn.Module],
        activation_layer: Callable[..., nn.Module],
        group_width: int,
        bottleneck_multiplier: float,
        se_ratio: Optional[float],
    ) -> None:
        layers: OrderedDict[str, nn.Module] = OrderedDict()
        w_b = int(round(width_out * bottleneck_multiplier))
        g = w_b // group_width

        layers["a"] = Conv2dNormActivation(
            width_in, w_b, kernel_size=1, stride=1, norm_layer=norm_layer, activation_layer=activation_layer
        )
        layers["b"] = Conv2dNormActivation(
            w_b, w_b, kernel_size=3, stride=stride, groups=g, norm_layer=norm_layer, activation_layer=activation_layer
        )

        if se_ratio:
            # The SE reduction ratio is defined with respect to the
            # beginning of the block
            width_se_out = int(round(se_ratio * width_in))
            layers["se"] = SqueezeExcitation(
                input_channels=w_b,
                squeeze_channels=width_se_out,
                activation=activation_layer,
            )

        layers["c"] = Conv2dNormActivation(
            w_b, width_out, kernel_size=1, stride=1, norm_layer=norm_layer, activation_layer=None
        )
        super().__init__(layers)


class ResBottleneckBlock(nn.Module):
    """Residual bottleneck block: x + F(x), F = bottleneck transform."""

    def __init__(
        self,
        width_in: int,
        width_out: int,
        stride: int,
        norm_layer: Callable[..., nn.Module],
        activation_layer: Callable[..., nn.Module],
        group_width: int = 1,
        bottleneck_multiplier: float = 1.0,
        se_ratio: Optional[float] = None,
    ) -> None:
        super().__init__()

        # Use skip connection with projection if shape changes
        self.proj = None
        should_proj = (width_in != width_out) or (stride != 1)
        if should_proj:
            self.proj = Conv2dNormActivation(
                width_in, width_out, kernel_size=1, stride=stride, norm_layer=norm_layer, activation_layer=None
            )
        self.f = BottleneckTransform(
            width_in,
            width_out,
            stride,
            norm_layer,
            activation_layer,
            group_width,
            bottleneck_multiplier,
            se_ratio,
        )
        self.activation = activation_layer(inplace=True)

    def forward(self, x: Tensor) -> Tensor:
        if self.proj is not None:
            x = self.proj(x) + self.f(x)
        else:
            x = x + self.f(x)
        return self.activation(x)


class AnyStage(nn.Sequential):
    """AnyNet stage (sequence of blocks w/ the same output shape)."""

    def __init__(
        self,
        width_in: int,
        width_out: int,
        stride: int,
        depth: int,
        block_constructor: Callable[..., nn.Module],
        norm_layer: Callable[..., nn.Module],
        activation_layer: Callable[..., nn.Module],
        group_width: int,
        bottleneck_multiplier: float,
        se_ratio: Optional[float] = None,
        stage_index: int = 0,
    ) -> None:
        super().__init__()

        for i in range(depth):
            block = block_constructor(
                width_in if i == 0 else width_out,
                width_out,
                stride if i == 0 else 1,
                norm_layer,
                activation_layer,
                group_width,
                bottleneck_multiplier,
                se_ratio,
            )

            self.add_module(f"block{stage_index}-{i}", block)


class BlockParams:
    def __init__(
        self,
        depths: List[int],
        widths: List[int],
        group_widths: List[int],
        bottleneck_multipliers: List[float],
        strides: List[int],
        se_ratio: Optional[float] = None,
    ) -> None:
        self.depths = depths
        self.widths = widths
        self.group_widths = group_widths
        self.bottleneck_multipliers = bottleneck_multipliers
        self.strides = strides
        self.se_ratio = se_ratio

    @classmethod
    def from_init_params(
        cls,
        depth: int,
        w_0: int,
        w_a: float,
        w_m: float,
        group_width: int,
        bottleneck_multiplier: float = 1.0,
        se_ratio: Optional[float] = None,
        **kwargs: Any,
    ) -> "BlockParams":
        """
        Programmatically compute all the per-block settings,
        given the RegNet parameters.

        The first step is to compute the quantized linear block parameters,
        in log space. Key parameters are:
        - `w_a` is the width progression slope
        - `w_0` is the initial width
        - `w_m` is the width stepping in the log space

        In other terms
        `log(block_width) = log(w_0) + w_m * block_capacity`,
        with `bock_capacity` ramping up following the w_0 and w_a params.
        This block width is finally quantized to multiples of 8.

        The second step is to compute the parameters per stage,
        taking into account the skip connection and the final 1x1 convolutions.
        We use the fact that the output width is constant within a stage.
        """

        QUANT = 8
        STRIDE = 2

        if w_a < 0 or w_0 <= 0 or w_m <= 1 or w_0 % 8 != 0:
            raise ValueError("Invalid RegNet settings")
        # Compute the block widths. Each stage has one unique block width
        widths_cont = torch.arange(depth) * w_a + w_0
        block_capacity = torch.round(torch.log(widths_cont / w_0) / math.log(w_m))
        block_widths = (torch.round(torch.divide(w_0 * torch.pow(w_m, block_capacity), QUANT)) * QUANT).int().tolist()
        num_stages = len(set(block_widths))

        # Convert to per stage parameters
        split_helper = zip(
            block_widths + [0],
            [0] + block_widths,
            block_widths + [0],
            [0] + block_widths,
        )
        splits = [w != wp or r != rp for w, wp, r, rp in split_helper]

        stage_widths = [w for w, t in zip(block_widths, splits[:-1]) if t]
        stage_depths = torch.diff(torch.tensor([d for d, t in enumerate(splits) if t])).int().tolist()

        strides = [STRIDE] * num_stages
        bottleneck_multipliers = [bottleneck_multiplier] * num_stages
        group_widths = [group_width] * num_stages

        # Adjust the compatibility of stage widths and group widths
        stage_widths, group_widths = cls._adjust_widths_groups_compatibilty(
            stage_widths, bottleneck_multipliers, group_widths
        )

        return cls(
            depths=stage_depths,
            widths=stage_widths,
            group_widths=group_widths,
            bottleneck_multipliers=bottleneck_multipliers,
            strides=strides,
            se_ratio=se_ratio,
        )

    def _get_expanded_params(self):
        return zip(self.widths, self.strides, self.depths, self.group_widths, self.bottleneck_multipliers)

    @staticmethod
    def _adjust_widths_groups_compatibilty(
        stage_widths: List[int], bottleneck_ratios: List[float], group_widths: List[int]
    ) -> Tuple[List[int], List[int]]:
        """
        Adjusts the compatibility of widths and groups,
        depending on the bottleneck ratio.
        """
        # Compute all widths for the current settings
        widths = [int(w * b) for w, b in zip(stage_widths, bottleneck_ratios)]
        group_widths_min = [min(g, w_bot) for g, w_bot in zip(group_widths, widths)]

        # Compute the adjusted widths so that stage and group widths fit
        ws_bot = [_make_divisible(w_bot, g) for w_bot, g in zip(widths, group_widths_min)]
        stage_widths = [int(w_bot / b) for w_bot, b in zip(ws_bot, bottleneck_ratios)]
        return stage_widths, group_widths_min


class RegNet(nn.Module):
    def __init__(
        self,
        block_params: BlockParams,
        num_classes: int = 1000,
        stem_width: int = 32,
        stem_type: Optional[Callable[..., nn.Module]] = None,
        block_type: Optional[Callable[..., nn.Module]] = None,
        norm_layer: Optional[Callable[..., nn.Module]] = None,
        activation: Optional[Callable[..., nn.Module]] = None,
    ) -> None:
        super().__init__()
        _log_api_usage_once(self)

        if stem_type is None:
            stem_type = SimpleStemIN
        if norm_layer is None:
            norm_layer = nn.BatchNorm2d
        if block_type is None:
            block_type = ResBottleneckBlock
        if activation is None:
            activation = nn.ReLU

        # Ad hoc stem
        self.stem = stem_type(
            3,  # width_in
            stem_width,
            norm_layer,
            activation,
        )

        current_width = stem_width

        blocks = []
        for i, (
            width_out,
            stride,
            depth,
            group_width,
            bottleneck_multiplier,
        ) in enumerate(block_params._get_expanded_params()):
            blocks.append(
                (
                    f"block{i+1}",
                    AnyStage(
                        current_width,
                        width_out,
                        stride,
                        depth,
                        block_type,
                        norm_layer,
                        activation,
                        group_width,
                        bottleneck_multiplier,
                        block_params.se_ratio,
                        stage_index=i + 1,
                    ),
                )
            )

            current_width = width_out

        self.trunk_output = nn.Sequential(OrderedDict(blocks))

        self.avgpool = nn.AdaptiveAvgPool2d((1, 1))
        self.fc = nn.Linear(in_features=current_width, out_features=num_classes)

        # Performs ResNet-style weight initialization
        for m in self.modules():
            if isinstance(m, nn.Conv2d):
                # Note that there is no bias due to BN
                fan_out = m.kernel_size[0] * m.kernel_size[1] * m.out_channels
                nn.init.normal_(m.weight, mean=0.0, std=math.sqrt(2.0 / fan_out))
            elif isinstance(m, nn.BatchNorm2d):
                nn.init.ones_(m.weight)
                nn.init.zeros_(m.bias)
            elif isinstance(m, nn.Linear):
                nn.init.normal_(m.weight, mean=0.0, std=0.01)
                nn.init.zeros_(m.bias)

    def forward(self, x: Tensor) -> Tensor:
        x = self.stem(x)
        x = self.trunk_output(x)

        x = self.avgpool(x)
        x = x.flatten(start_dim=1)
        x = self.fc(x)

        return x


def _regnet(
    block_params: BlockParams,
    weights: Optional[WeightsEnum],
    progress: bool,
    **kwargs: Any,
) -> RegNet:
    if weights is not None:
        _ovewrite_named_param(kwargs, "num_classes", len(weights.meta["categories"]))

    norm_layer = kwargs.pop("norm_layer", partial(nn.BatchNorm2d, eps=1e-05, momentum=0.1))
    model = RegNet(block_params, norm_layer=norm_layer, **kwargs)

    if weights is not None:
        model.load_state_dict(weights.get_state_dict(progress=progress, check_hash=True))

    return model


_COMMON_META: Dict[str, Any] = {
    "min_size": (1, 1),
    "categories": _IMAGENET_CATEGORIES,
}

_COMMON_SWAG_META = {
    **_COMMON_META,
    "recipe": "https://github.com/facebookresearch/SWAG",
    "license": "https://github.com/facebookresearch/SWAG/blob/main/LICENSE",
}


class RegNet_Y_400MF_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_y_400mf-c65dace8.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 4344144,
            "recipe": "https://github.com/pytorch/vision/tree/main/references/classification#small-models",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 74.046,
                    "acc@5": 91.716,
                }
            },
            "_ops": 0.402,
            "_file_size": 16.806,
            "_docs": """These weights reproduce closely the results of the paper using a simple training recipe.""",
        },
    )
    IMAGENET1K_V2 = Weights(
        url="https://download.pytorch.org/models/regnet_y_400mf-e6988f5f.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "num_params": 4344144,
            "recipe": "https://github.com/pytorch/vision/issues/3995#new-recipe",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 75.804,
                    "acc@5": 92.742,
                }
            },
            "_ops": 0.402,
            "_file_size": 16.806,
            "_docs": """
                These weights improve upon the results of the original paper by using a modified version of TorchVision's
                `new training recipe
                <https://pytorch.org/blog/how-to-train-state-of-the-art-models-using-torchvision-latest-primitives/>`_.
            """,
        },
    )
    DEFAULT = IMAGENET1K_V2


class RegNet_Y_800MF_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_y_800mf-1b27b58c.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 6432512,
            "recipe": "https://github.com/pytorch/vision/tree/main/references/classification#small-models",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 76.420,
                    "acc@5": 93.136,
                }
            },
            "_ops": 0.834,
            "_file_size": 24.774,
            "_docs": """These weights reproduce closely the results of the paper using a simple training recipe.""",
        },
    )
    IMAGENET1K_V2 = Weights(
        url="https://download.pytorch.org/models/regnet_y_800mf-58fc7688.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "num_params": 6432512,
            "recipe": "https://github.com/pytorch/vision/issues/3995#new-recipe",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 78.828,
                    "acc@5": 94.502,
                }
            },
            "_ops": 0.834,
            "_file_size": 24.774,
            "_docs": """
                These weights improve upon the results of the original paper by using a modified version of TorchVision's
                `new training recipe
                <https://pytorch.org/blog/how-to-train-state-of-the-art-models-using-torchvision-latest-primitives/>`_.
            """,
        },
    )
    DEFAULT = IMAGENET1K_V2


class RegNet_Y_1_6GF_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_y_1_6gf-b11a554e.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 11202430,
            "recipe": "https://github.com/pytorch/vision/tree/main/references/classification#small-models",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 77.950,
                    "acc@5": 93.966,
                }
            },
            "_ops": 1.612,
            "_file_size": 43.152,
            "_docs": """These weights reproduce closely the results of the paper using a simple training recipe.""",
        },
    )
    IMAGENET1K_V2 = Weights(
        url="https://download.pytorch.org/models/regnet_y_1_6gf-0d7bc02a.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "num_params": 11202430,
            "recipe": "https://github.com/pytorch/vision/issues/3995#new-recipe",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 80.876,
                    "acc@5": 95.444,
                }
            },
            "_ops": 1.612,
            "_file_size": 43.152,
            "_docs": """
                These weights improve upon the results of the original paper by using a modified version of TorchVision's
                `new training recipe
                <https://pytorch.org/blog/how-to-train-state-of-the-art-models-using-torchvision-latest-primitives/>`_.
            """,
        },
    )
    DEFAULT = IMAGENET1K_V2


class RegNet_Y_3_2GF_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_y_3_2gf-b5a9779c.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 19436338,
            "recipe": "https://github.com/pytorch/vision/tree/main/references/classification#medium-models",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 78.948,
                    "acc@5": 94.576,
                }
            },
            "_ops": 3.176,
            "_file_size": 74.567,
            "_docs": """These weights reproduce closely the results of the paper using a simple training recipe.""",
        },
    )
    IMAGENET1K_V2 = Weights(
        url="https://download.pytorch.org/models/regnet_y_3_2gf-9180c971.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "num_params": 19436338,
            "recipe": "https://github.com/pytorch/vision/issues/3995#new-recipe",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 81.982,
                    "acc@5": 95.972,
                }
            },
            "_ops": 3.176,
            "_file_size": 74.567,
            "_docs": """
                These weights improve upon the results of the original paper by using a modified version of TorchVision's
                `new training recipe
                <https://pytorch.org/blog/how-to-train-state-of-the-art-models-using-torchvision-latest-primitives/>`_.
            """,
        },
    )
    DEFAULT = IMAGENET1K_V2


class RegNet_Y_8GF_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_y_8gf-d0d0e4a8.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 39381472,
            "recipe": "https://github.com/pytorch/vision/tree/main/references/classification#medium-models",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 80.032,
                    "acc@5": 95.048,
                }
            },
            "_ops": 8.473,
            "_file_size": 150.701,
            "_docs": """These weights reproduce closely the results of the paper using a simple training recipe.""",
        },
    )
    IMAGENET1K_V2 = Weights(
        url="https://download.pytorch.org/models/regnet_y_8gf-dc2b1b54.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "num_params": 39381472,
            "recipe": "https://github.com/pytorch/vision/issues/3995#new-recipe",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 82.828,
                    "acc@5": 96.330,
                }
            },
            "_ops": 8.473,
            "_file_size": 150.701,
            "_docs": """
                These weights improve upon the results of the original paper by using a modified version of TorchVision's
                `new training recipe
                <https://pytorch.org/blog/how-to-train-state-of-the-art-models-using-torchvision-latest-primitives/>`_.
            """,
        },
    )
    DEFAULT = IMAGENET1K_V2


class RegNet_Y_16GF_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_y_16gf-9e6ed7dd.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 83590140,
            "recipe": "https://github.com/pytorch/vision/tree/main/references/classification#large-models",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 80.424,
                    "acc@5": 95.240,
                }
            },
            "_ops": 15.912,
            "_file_size": 319.49,
            "_docs": """These weights reproduce closely the results of the paper using a simple training recipe.""",
        },
    )
    IMAGENET1K_V2 = Weights(
        url="https://download.pytorch.org/models/regnet_y_16gf-3e4a00f9.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "num_params": 83590140,
            "recipe": "https://github.com/pytorch/vision/issues/3995#new-recipe",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 82.886,
                    "acc@5": 96.328,
                }
            },
            "_ops": 15.912,
            "_file_size": 319.49,
            "_docs": """
                These weights improve upon the results of the original paper by using a modified version of TorchVision's
                `new training recipe
                <https://pytorch.org/blog/how-to-train-state-of-the-art-models-using-torchvision-latest-primitives/>`_.
            """,
        },
    )
    IMAGENET1K_SWAG_E2E_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_y_16gf_swag-43afe44d.pth",
        transforms=partial(
            ImageClassification, crop_size=384, resize_size=384, interpolation=InterpolationMode.BICUBIC
        ),
        meta={
            **_COMMON_SWAG_META,
            "num_params": 83590140,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 86.012,
                    "acc@5": 98.054,
                }
            },
            "_ops": 46.735,
            "_file_size": 319.49,
            "_docs": """
                These weights are learnt via transfer learning by end-to-end fine-tuning the original
                `SWAG <https://arxiv.org/abs/2201.08371>`_ weights on ImageNet-1K data.
            """,
        },
    )
    IMAGENET1K_SWAG_LINEAR_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_y_16gf_lc_swag-f3ec0043.pth",
        transforms=partial(
            ImageClassification, crop_size=224, resize_size=224, interpolation=InterpolationMode.BICUBIC
        ),
        meta={
            **_COMMON_SWAG_META,
            "recipe": "https://github.com/pytorch/vision/pull/5793",
            "num_params": 83590140,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 83.976,
                    "acc@5": 97.244,
                }
            },
            "_ops": 15.912,
            "_file_size": 319.49,
            "_docs": """
                These weights are composed of the original frozen `SWAG <https://arxiv.org/abs/2201.08371>`_ trunk
                weights and a linear classifier learnt on top of them trained on ImageNet-1K data.
            """,
        },
    )
    DEFAULT = IMAGENET1K_V2


class RegNet_Y_32GF_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_y_32gf-4dee3f7a.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 145046770,
            "recipe": "https://github.com/pytorch/vision/tree/main/references/classification#large-models",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 80.878,
                    "acc@5": 95.340,
                }
            },
            "_ops": 32.28,
            "_file_size": 554.076,
            "_docs": """These weights reproduce closely the results of the paper using a simple training recipe.""",
        },
    )
    IMAGENET1K_V2 = Weights(
        url="https://download.pytorch.org/models/regnet_y_32gf-8db6d4b5.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "num_params": 145046770,
            "recipe": "https://github.com/pytorch/vision/issues/3995#new-recipe",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 83.368,
                    "acc@5": 96.498,
                }
            },
            "_ops": 32.28,
            "_file_size": 554.076,
            "_docs": """
                These weights improve upon the results of the original paper by using a modified version of TorchVision's
                `new training recipe
                <https://pytorch.org/blog/how-to-train-state-of-the-art-models-using-torchvision-latest-primitives/>`_.
            """,
        },
    )
    IMAGENET1K_SWAG_E2E_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_y_32gf_swag-04fdfa75.pth",
        transforms=partial(
            ImageClassification, crop_size=384, resize_size=384, interpolation=InterpolationMode.BICUBIC
        ),
        meta={
            **_COMMON_SWAG_META,
            "num_params": 145046770,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 86.838,
                    "acc@5": 98.362,
                }
            },
            "_ops": 94.826,
            "_file_size": 554.076,
            "_docs": """
                These weights are learnt via transfer learning by end-to-end fine-tuning the original
                `SWAG <https://arxiv.org/abs/2201.08371>`_ weights on ImageNet-1K data.
            """,
        },
    )
    IMAGENET1K_SWAG_LINEAR_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_y_32gf_lc_swag-e1583746.pth",
        transforms=partial(
            ImageClassification, crop_size=224, resize_size=224, interpolation=InterpolationMode.BICUBIC
        ),
        meta={
            **_COMMON_SWAG_META,
            "recipe": "https://github.com/pytorch/vision/pull/5793",
            "num_params": 145046770,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 84.622,
                    "acc@5": 97.480,
                }
            },
            "_ops": 32.28,
            "_file_size": 554.076,
            "_docs": """
                These weights are composed of the original frozen `SWAG <https://arxiv.org/abs/2201.08371>`_ trunk
                weights and a linear classifier learnt on top of them trained on ImageNet-1K data.
            """,
        },
    )
    DEFAULT = IMAGENET1K_V2


class RegNet_Y_128GF_Weights(WeightsEnum):
    IMAGENET1K_SWAG_E2E_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_y_128gf_swag-c8ce3e52.pth",
        transforms=partial(
            ImageClassification, crop_size=384, resize_size=384, interpolation=InterpolationMode.BICUBIC
        ),
        meta={
            **_COMMON_SWAG_META,
            "num_params": 644812894,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 88.228,
                    "acc@5": 98.682,
                }
            },
            "_ops": 374.57,
            "_file_size": 2461.564,
            "_docs": """
                These weights are learnt via transfer learning by end-to-end fine-tuning the original
                `SWAG <https://arxiv.org/abs/2201.08371>`_ weights on ImageNet-1K data.
            """,
        },
    )
    IMAGENET1K_SWAG_LINEAR_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_y_128gf_lc_swag-cbe8ce12.pth",
        transforms=partial(
            ImageClassification, crop_size=224, resize_size=224, interpolation=InterpolationMode.BICUBIC
        ),
        meta={
            **_COMMON_SWAG_META,
            "recipe": "https://github.com/pytorch/vision/pull/5793",
            "num_params": 644812894,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 86.068,
                    "acc@5": 97.844,
                }
            },
            "_ops": 127.518,
            "_file_size": 2461.564,
            "_docs": """
                These weights are composed of the original frozen `SWAG <https://arxiv.org/abs/2201.08371>`_ trunk
                weights and a linear classifier learnt on top of them trained on ImageNet-1K data.
            """,
        },
    )
    DEFAULT = IMAGENET1K_SWAG_E2E_V1


class RegNet_X_400MF_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_x_400mf-adf1edd5.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 5495976,
            "recipe": "https://github.com/pytorch/vision/tree/main/references/classification#small-models",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 72.834,
                    "acc@5": 90.950,
                }
            },
            "_ops": 0.414,
            "_file_size": 21.258,
            "_docs": """These weights reproduce closely the results of the paper using a simple training recipe.""",
        },
    )
    IMAGENET1K_V2 = Weights(
        url="https://download.pytorch.org/models/regnet_x_400mf-62229a5f.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "num_params": 5495976,
            "recipe": "https://github.com/pytorch/vision/issues/3995#new-recipe-with-fixres",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 74.864,
                    "acc@5": 92.322,
                }
            },
            "_ops": 0.414,
            "_file_size": 21.257,
            "_docs": """
                These weights improve upon the results of the original paper by using a modified version of TorchVision's
                `new training recipe
                <https://pytorch.org/blog/how-to-train-state-of-the-art-models-using-torchvision-latest-primitives/>`_.
            """,
        },
    )
    DEFAULT = IMAGENET1K_V2


class RegNet_X_800MF_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_x_800mf-ad17e45c.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 7259656,
            "recipe": "https://github.com/pytorch/vision/tree/main/references/classification#small-models",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 75.212,
                    "acc@5": 92.348,
                }
            },
            "_ops": 0.8,
            "_file_size": 27.945,
            "_docs": """These weights reproduce closely the results of the paper using a simple training recipe.""",
        },
    )
    IMAGENET1K_V2 = Weights(
        url="https://download.pytorch.org/models/regnet_x_800mf-94a99ebd.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "num_params": 7259656,
            "recipe": "https://github.com/pytorch/vision/issues/3995#new-recipe-with-fixres",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 77.522,
                    "acc@5": 93.826,
                }
            },
            "_ops": 0.8,
            "_file_size": 27.945,
            "_docs": """
                These weights improve upon the results of the original paper by using a modified version of TorchVision's
                `new training recipe
                <https://pytorch.org/blog/how-to-train-state-of-the-art-models-using-torchvision-latest-primitives/>`_.
            """,
        },
    )
    DEFAULT = IMAGENET1K_V2


class RegNet_X_1_6GF_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_x_1_6gf-e3633e7f.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 9190136,
            "recipe": "https://github.com/pytorch/vision/tree/main/references/classification#small-models",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 77.040,
                    "acc@5": 93.440,
                }
            },
            "_ops": 1.603,
            "_file_size": 35.339,
            "_docs": """These weights reproduce closely the results of the paper using a simple training recipe.""",
        },
    )
    IMAGENET1K_V2 = Weights(
        url="https://download.pytorch.org/models/regnet_x_1_6gf-a12f2b72.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "num_params": 9190136,
            "recipe": "https://github.com/pytorch/vision/issues/3995#new-recipe-with-fixres",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 79.668,
                    "acc@5": 94.922,
                }
            },
            "_ops": 1.603,
            "_file_size": 35.339,
            "_docs": """
                These weights improve upon the results of the original paper by using a modified version of TorchVision's
                `new training recipe
                <https://pytorch.org/blog/how-to-train-state-of-the-art-models-using-torchvision-latest-primitives/>`_.
            """,
        },
    )
    DEFAULT = IMAGENET1K_V2


class RegNet_X_3_2GF_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_x_3_2gf-f342aeae.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 15296552,
            "recipe": "https://github.com/pytorch/vision/tree/main/references/classification#medium-models",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 78.364,
                    "acc@5": 93.992,
                }
            },
            "_ops": 3.177,
            "_file_size": 58.756,
            "_docs": """These weights reproduce closely the results of the paper using a simple training recipe.""",
        },
    )
    IMAGENET1K_V2 = Weights(
        url="https://download.pytorch.org/models/regnet_x_3_2gf-7071aa85.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "num_params": 15296552,
            "recipe": "https://github.com/pytorch/vision/issues/3995#new-recipe",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 81.196,
                    "acc@5": 95.430,
                }
            },
            "_ops": 3.177,
            "_file_size": 58.756,
            "_docs": """
                These weights improve upon the results of the original paper by using a modified version of TorchVision's
                `new training recipe
                <https://pytorch.org/blog/how-to-train-state-of-the-art-models-using-torchvision-latest-primitives/>`_.
            """,
        },
    )
    DEFAULT = IMAGENET1K_V2


class RegNet_X_8GF_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_x_8gf-03ceed89.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 39572648,
            "recipe": "https://github.com/pytorch/vision/tree/main/references/classification#medium-models",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 79.344,
                    "acc@5": 94.686,
                }
            },
            "_ops": 7.995,
            "_file_size": 151.456,
            "_docs": """These weights reproduce closely the results of the paper using a simple training recipe.""",
        },
    )
    IMAGENET1K_V2 = Weights(
        url="https://download.pytorch.org/models/regnet_x_8gf-2b70d774.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "num_params": 39572648,
            "recipe": "https://github.com/pytorch/vision/issues/3995#new-recipe",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 81.682,
                    "acc@5": 95.678,
                }
            },
            "_ops": 7.995,
            "_file_size": 151.456,
            "_docs": """
                These weights improve upon the results of the original paper by using a modified version of TorchVision's
                `new training recipe
                <https://pytorch.org/blog/how-to-train-state-of-the-art-models-using-torchvision-latest-primitives/>`_.
            """,
        },
    )
    DEFAULT = IMAGENET1K_V2


class RegNet_X_16GF_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_x_16gf-2007eb11.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 54278536,
            "recipe": "https://github.com/pytorch/vision/tree/main/references/classification#medium-models",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 80.058,
                    "acc@5": 94.944,
                }
            },
            "_ops": 15.941,
            "_file_size": 207.627,
            "_docs": """These weights reproduce closely the results of the paper using a simple training recipe.""",
        },
    )
    IMAGENET1K_V2 = Weights(
        url="https://download.pytorch.org/models/regnet_x_16gf-ba3796d7.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "num_params": 54278536,
            "recipe": "https://github.com/pytorch/vision/issues/3995#new-recipe",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 82.716,
                    "acc@5": 96.196,
                }
            },
            "_ops": 15.941,
            "_file_size": 207.627,
            "_docs": """
                These weights improve upon the results of the original paper by using a modified version of TorchVision's
                `new training recipe
                <https://pytorch.org/blog/how-to-train-state-of-the-art-models-using-torchvision-latest-primitives/>`_.
            """,
        },
    )
    DEFAULT = IMAGENET1K_V2


class RegNet_X_32GF_Weights(WeightsEnum):
    IMAGENET1K_V1 = Weights(
        url="https://download.pytorch.org/models/regnet_x_32gf-9d47f8d0.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 107811560,
            "recipe": "https://github.com/pytorch/vision/tree/main/references/classification#large-models",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 80.622,
                    "acc@5": 95.248,
                }
            },
            "_ops": 31.736,
            "_file_size": 412.039,
            "_docs": """These weights reproduce closely the results of the paper using a simple training recipe.""",
        },
    )
    IMAGENET1K_V2 = Weights(
        url="https://download.pytorch.org/models/regnet_x_32gf-6eb8fdc6.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "num_params": 107811560,
            "recipe": "https://github.com/pytorch/vision/issues/3995#new-recipe",
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 83.014,
                    "acc@5": 96.288,
                }
            },
            "_ops": 31.736,
            "_file_size": 412.039,
            "_docs": """
                These weights improve upon the results of the original paper by using a modified version of TorchVision's
                `new training recipe
                <https://pytorch.org/blog/how-to-train-state-of-the-art-models-using-torchvision-latest-primitives/>`_.
            """,
        },
    )
    DEFAULT = IMAGENET1K_V2


@register_model()
@handle_legacy_interface(weights=("pretrained", RegNet_Y_400MF_Weights.IMAGENET1K_V1))
def regnet_y_400mf(*, weights: Optional[RegNet_Y_400MF_Weights] = None, progress: bool = True, **kwargs: Any) -> RegNet:
    """
    Constructs a RegNetY_400MF architecture from
    `Designing Network Design Spaces <https://arxiv.org/abs/2003.13678>`_.

    Args:
        weights (:class:`~torchvision.models.RegNet_Y_400MF_Weights`, optional): The pretrained weights to use.
            See :class:`~torchvision.models.RegNet_Y_400MF_Weights` below for more details and possible values.
            By default, no pretrained weights are used.
        progress (bool, optional): If True, displays a progress bar of the download to stderr. Default is True.
        **kwargs: parameters passed to either ``torchvision.models.regnet.RegNet`` or
            ``torchvision.models.regnet.BlockParams`` class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/regnet.py>`_
            for more detail about the classes.

    .. autoclass:: torchvision.models.RegNet_Y_400MF_Weights
        :members:
    """
    weights = RegNet_Y_400MF_Weights.verify(weights)

    params = BlockParams.from_init_params(depth=16, w_0=48, w_a=27.89, w_m=2.09, group_width=8, se_ratio=0.25, **kwargs)
    return _regnet(params, weights, progress, **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", RegNet_Y_800MF_Weights.IMAGENET1K_V1))
def regnet_y_800mf(*, weights: Optional[RegNet_Y_800MF_Weights] = None, progress: bool = True, **kwargs: Any) -> RegNet:
    """
    Constructs a RegNetY_800MF architecture from
    `Designing Network Design Spaces <https://arxiv.org/abs/2003.13678>`_.

    Args:
        weights (:class:`~torchvision.models.RegNet_Y_800MF_Weights`, optional): The pretrained weights to use.
            See :class:`~torchvision.models.RegNet_Y_800MF_Weights` below for more details and possible values.
            By default, no pretrained weights are used.
        progress (bool, optional): If True, displays a progress bar of the download to stderr. Default is True.
        **kwargs: parameters passed to either ``torchvision.models.regnet.RegNet`` or
            ``torchvision.models.regnet.BlockParams`` class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/regnet.py>`_
            for more detail about the classes.

    .. autoclass:: torchvision.models.RegNet_Y_800MF_Weights
        :members:
    """
    weights = RegNet_Y_800MF_Weights.verify(weights)

    params = BlockParams.from_init_params(depth=14, w_0=56, w_a=38.84, w_m=2.4, group_width=16, se_ratio=0.25, **kwargs)
    return _regnet(params, weights, progress, **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", RegNet_Y_1_6GF_Weights.IMAGENET1K_V1))
def regnet_y_1_6gf(*, weights: Optional[RegNet_Y_1_6GF_Weights] = None, progress: bool = True, **kwargs: Any) -> RegNet:
    """
    Constructs a RegNetY_1.6GF architecture from
    `Designing Network Design Spaces <https://arxiv.org/abs/2003.13678>`_.

    Args:
        weights (:class:`~torchvision.models.RegNet_Y_1_6GF_Weights`, optional): The pretrained weights to use.
            See :class:`~torchvision.models.RegNet_Y_1_6GF_Weights` below for more details and possible values.
            By default, no pretrained weights are used.
        progress (bool, optional): If True, displays a progress bar of the download to stderr. Default is True.
        **kwargs: parameters passed to either ``torchvision.models.regnet.RegNet`` or
            ``torchvision.models.regnet.BlockParams`` class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/regnet.py>`_
            for more detail about the classes.

    .. autoclass:: torchvision.models.RegNet_Y_1_6GF_Weights
        :members:
    """
    weights = RegNet_Y_1_6GF_Weights.verify(weights)

    params = BlockParams.from_init_params(
        depth=27, w_0=48, w_a=20.71, w_m=2.65, group_width=24, se_ratio=0.25, **kwargs
    )
    return _regnet(params, weights, progress, **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", RegNet_Y_3_2GF_Weights.IMAGENET1K_V1))
def regnet_y_3_2gf(*, weights: Optional[RegNet_Y_3_2GF_Weights] = None, progress: bool = True, **kwargs: Any) -> RegNet:
    """
    Constructs a RegNetY_3.2GF architecture from
    `Designing Network Design Spaces <https://arxiv.org/abs/2003.13678>`_.

    Args:
        weights (:class:`~torchvision.models.RegNet_Y_3_2GF_Weights`, optional): The pretrained weights to use.
            See :class:`~torchvision.models.RegNet_Y_3_2GF_Weights` below for more details and possible values.
            By default, no pretrained weights are used.
        progress (bool, optional): If True, displays a progress bar of the download to stderr. Default is True.
        **kwargs: parameters passed to either ``torchvision.models.regnet.RegNet`` or
            ``torchvision.models.regnet.BlockParams`` class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/regnet.py>`_
            for more detail about the classes.

    .. autoclass:: torchvision.models.RegNet_Y_3_2GF_Weights
        :members:
    """
    weights = RegNet_Y_3_2GF_Weights.verify(weights)

    params = BlockParams.from_init_params(
        depth=21, w_0=80, w_a=42.63, w_m=2.66, group_width=24, se_ratio=0.25, **kwargs
    )
    return _regnet(params, weights, progress, **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", RegNet_Y_8GF_Weights.IMAGENET1K_V1))
def regnet_y_8gf(*, weights: Optional[RegNet_Y_8GF_Weights] = None, progress: bool = True, **kwargs: Any) -> RegNet:
    """
    Constructs a RegNetY_8GF architecture from
    `Designing Network Design Spaces <https://arxiv.org/abs/2003.13678>`_.

    Args:
        weights (:class:`~torchvision.models.RegNet_Y_8GF_Weights`, optional): The pretrained weights to use.
            See :class:`~torchvision.models.RegNet_Y_8GF_Weights` below for more details and possible values.
            By default, no pretrained weights are used.
        progress (bool, optional): If True, displays a progress bar of the download to stderr. Default is True.
        **kwargs: parameters passed to either ``torchvision.models.regnet.RegNet`` or
            ``torchvision.models.regnet.BlockParams`` class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/regnet.py>`_
            for more detail about the classes.

    .. autoclass:: torchvision.models.RegNet_Y_8GF_Weights
        :members:
    """
    weights = RegNet_Y_8GF_Weights.verify(weights)

    params = BlockParams.from_init_params(
        depth=17, w_0=192, w_a=76.82, w_m=2.19, group_width=56, se_ratio=0.25, **kwargs
    )
    return _regnet(params, weights, progress, **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", RegNet_Y_16GF_Weights.IMAGENET1K_V1))
def regnet_y_16gf(*, weights: Optional[RegNet_Y_16GF_Weights] = None, progress: bool = True, **kwargs: Any) -> RegNet:
    """
    Constructs a RegNetY_16GF architecture from
    `Designing Network Design Spaces <https://arxiv.org/abs/2003.13678>`_.

    Args:
        weights (:class:`~torchvision.models.RegNet_Y_16GF_Weights`, optional): The pretrained weights to use.
            See :class:`~torchvision.models.RegNet_Y_16GF_Weights` below for more details and possible values.
            By default, no pretrained weights are used.
        progress (bool, optional): If True, displays a progress bar of the download to stderr. Default is True.
        **kwargs: parameters passed to either ``torchvision.models.regnet.RegNet`` or
            ``torchvision.models.regnet.BlockParams`` class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/regnet.py>`_
            for more detail about the classes.

    .. autoclass:: torchvision.models.RegNet_Y_16GF_Weights
        :members:
    """
    weights = RegNet_Y_16GF_Weights.verify(weights)

    params = BlockParams.from_init_params(
        depth=18, w_0=200, w_a=106.23, w_m=2.48, group_width=112, se_ratio=0.25, **kwargs
    )
    return _regnet(params, weights, progress, **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", RegNet_Y_32GF_Weights.IMAGENET1K_V1))
def regnet_y_32gf(*, weights: Optional[RegNet_Y_32GF_Weights] = None, progress: bool = True, **kwargs: Any) -> RegNet:
    """
    Constructs a RegNetY_32GF architecture from
    `Designing Network Design Spaces <https://arxiv.org/abs/2003.13678>`_.

    Args:
        weights (:class:`~torchvision.models.RegNet_Y_32GF_Weights`, optional): The pretrained weights to use.
            See :class:`~torchvision.models.RegNet_Y_32GF_Weights` below for more details and possible values.
            By default, no pretrained weights are used.
        progress (bool, optional): If True, displays a progress bar of the download to stderr. Default is True.
        **kwargs: parameters passed to either ``torchvision.models.regnet.RegNet`` or
            ``torchvision.models.regnet.BlockParams`` class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/regnet.py>`_
            for more detail about the classes.

    .. autoclass:: torchvision.models.RegNet_Y_32GF_Weights
        :members:
    """
    weights = RegNet_Y_32GF_Weights.verify(weights)

    params = BlockParams.from_init_params(
        depth=20, w_0=232, w_a=115.89, w_m=2.53, group_width=232, se_ratio=0.25, **kwargs
    )
    return _regnet(params, weights, progress, **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", None))
def regnet_y_128gf(*, weights: Optional[RegNet_Y_128GF_Weights] = None, progress: bool = True, **kwargs: Any) -> RegNet:
    """
    Constructs a RegNetY_128GF architecture from
    `Designing Network Design Spaces <https://arxiv.org/abs/2003.13678>`_.

    Args:
        weights (:class:`~torchvision.models.RegNet_Y_128GF_Weights`, optional): The pretrained weights to use.
            See :class:`~torchvision.models.RegNet_Y_128GF_Weights` below for more details and possible values.
            By default, no pretrained weights are used.
        progress (bool, optional): If True, displays a progress bar of the download to stderr. Default is True.
        **kwargs: parameters passed to either ``torchvision.models.regnet.RegNet`` or
            ``torchvision.models.regnet.BlockParams`` class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/regnet.py>`_
            for more detail about the classes.

    .. autoclass:: torchvision.models.RegNet_Y_128GF_Weights
        :members:
    """
    weights = RegNet_Y_128GF_Weights.verify(weights)

    params = BlockParams.from_init_params(
        depth=27, w_0=456, w_a=160.83, w_m=2.52, group_width=264, se_ratio=0.25, **kwargs
    )
    return _regnet(params, weights, progress, **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", RegNet_X_400MF_Weights.IMAGENET1K_V1))
def regnet_x_400mf(*, weights: Optional[RegNet_X_400MF_Weights] = None, progress: bool = True, **kwargs: Any) -> RegNet:
    """
    Constructs a RegNetX_400MF architecture from
    `Designing Network Design Spaces <https://arxiv.org/abs/2003.13678>`_.

    Args:
        weights (:class:`~torchvision.models.RegNet_X_400MF_Weights`, optional): The pretrained weights to use.
            See :class:`~torchvision.models.RegNet_X_400MF_Weights` below for more details and possible values.
            By default, no pretrained weights are used.
        progress (bool, optional): If True, displays a progress bar of the download to stderr. Default is True.
        **kwargs: parameters passed to either ``torchvision.models.regnet.RegNet`` or
            ``torchvision.models.regnet.BlockParams`` class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/regnet.py>`_
            for more detail about the classes.

    .. autoclass:: torchvision.models.RegNet_X_400MF_Weights
        :members:
    """
    weights = RegNet_X_400MF_Weights.verify(weights)

    params = BlockParams.from_init_params(depth=22, w_0=24, w_a=24.48, w_m=2.54, group_width=16, **kwargs)
    return _regnet(params, weights, progress, **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", RegNet_X_800MF_Weights.IMAGENET1K_V1))
def regnet_x_800mf(*, weights: Optional[RegNet_X_800MF_Weights] = None, progress: bool = True, **kwargs: Any) -> RegNet:
    """
    Constructs a RegNetX_800MF architecture from
    `Designing Network Design Spaces <https://arxiv.org/abs/2003.13678>`_.

    Args:
        weights (:class:`~torchvision.models.RegNet_X_800MF_Weights`, optional): The pretrained weights to use.
            See :class:`~torchvision.models.RegNet_X_800MF_Weights` below for more details and possible values.
            By default, no pretrained weights are used.
        progress (bool, optional): If True, displays a progress bar of the download to stderr. Default is True.
        **kwargs: parameters passed to either ``torchvision.models.regnet.RegNet`` or
            ``torchvision.models.regnet.BlockParams`` class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/regnet.py>`_
            for more detail about the classes.

    .. autoclass:: torchvision.models.RegNet_X_800MF_Weights
        :members:
    """
    weights = RegNet_X_800MF_Weights.verify(weights)

    params = BlockParams.from_init_params(depth=16, w_0=56, w_a=35.73, w_m=2.28, group_width=16, **kwargs)
    return _regnet(params, weights, progress, **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", RegNet_X_1_6GF_Weights.IMAGENET1K_V1))
def regnet_x_1_6gf(*, weights: Optional[RegNet_X_1_6GF_Weights] = None, progress: bool = True, **kwargs: Any) -> RegNet:
    """
    Constructs a RegNetX_1.6GF architecture from
    `Designing Network Design Spaces <https://arxiv.org/abs/2003.13678>`_.

    Args:
        weights (:class:`~torchvision.models.RegNet_X_1_6GF_Weights`, optional): The pretrained weights to use.
            See :class:`~torchvision.models.RegNet_X_1_6GF_Weights` below for more details and possible values.
            By default, no pretrained weights are used.
        progress (bool, optional): If True, displays a progress bar of the download to stderr. Default is True.
        **kwargs: parameters passed to either ``torchvision.models.regnet.RegNet`` or
            ``torchvision.models.regnet.BlockParams`` class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/regnet.py>`_
            for more detail about the classes.

    .. autoclass:: torchvision.models.RegNet_X_1_6GF_Weights
        :members:
    """
    weights = RegNet_X_1_6GF_Weights.verify(weights)

    params = BlockParams.from_init_params(depth=18, w_0=80, w_a=34.01, w_m=2.25, group_width=24, **kwargs)
    return _regnet(params, weights, progress, **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", RegNet_X_3_2GF_Weights.IMAGENET1K_V1))
def regnet_x_3_2gf(*, weights: Optional[RegNet_X_3_2GF_Weights] = None, progress: bool = True, **kwargs: Any) -> RegNet:
    """
    Constructs a RegNetX_3.2GF architecture from
    `Designing Network Design Spaces <https://arxiv.org/abs/2003.13678>`_.

    Args:
        weights (:class:`~torchvision.models.RegNet_X_3_2GF_Weights`, optional): The pretrained weights to use.
            See :class:`~torchvision.models.RegNet_X_3_2GF_Weights` below for more details and possible values.
            By default, no pretrained weights are used.
        progress (bool, optional): If True, displays a progress bar of the download to stderr. Default is True.
        **kwargs: parameters passed to either ``torchvision.models.regnet.RegNet`` or
            ``torchvision.models.regnet.BlockParams`` class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/regnet.py>`_
            for more detail about the classes.

    .. autoclass:: torchvision.models.RegNet_X_3_2GF_Weights
        :members:
    """
    weights = RegNet_X_3_2GF_Weights.verify(weights)

    params = BlockParams.from_init_params(depth=25, w_0=88, w_a=26.31, w_m=2.25, group_width=48, **kwargs)
    return _regnet(params, weights, progress, **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", RegNet_X_8GF_Weights.IMAGENET1K_V1))
def regnet_x_8gf(*, weights: Optional[RegNet_X_8GF_Weights] = None, progress: bool = True, **kwargs: Any) -> RegNet:
    """
    Constructs a RegNetX_8GF architecture from
    `Designing Network Design Spaces <https://arxiv.org/abs/2003.13678>`_.

    Args:
        weights (:class:`~torchvision.models.RegNet_X_8GF_Weights`, optional): The pretrained weights to use.
            See :class:`~torchvision.models.RegNet_X_8GF_Weights` below for more details and possible values.
            By default, no pretrained weights are used.
        progress (bool, optional): If True, displays a progress bar of the download to stderr. Default is True.
        **kwargs: parameters passed to either ``torchvision.models.regnet.RegNet`` or
            ``torchvision.models.regnet.BlockParams`` class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/regnet.py>`_
            for more detail about the classes.

    .. autoclass:: torchvision.models.RegNet_X_8GF_Weights
        :members:
    """
    weights = RegNet_X_8GF_Weights.verify(weights)

    params = BlockParams.from_init_params(depth=23, w_0=80, w_a=49.56, w_m=2.88, group_width=120, **kwargs)
    return _regnet(params, weights, progress, **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", RegNet_X_16GF_Weights.IMAGENET1K_V1))
def regnet_x_16gf(*, weights: Optional[RegNet_X_16GF_Weights] = None, progress: bool = True, **kwargs: Any) -> RegNet:
    """
    Constructs a RegNetX_16GF architecture from
    `Designing Network Design Spaces <https://arxiv.org/abs/2003.13678>`_.

    Args:
        weights (:class:`~torchvision.models.RegNet_X_16GF_Weights`, optional): The pretrained weights to use.
            See :class:`~torchvision.models.RegNet_X_16GF_Weights` below for more details and possible values.
            By default, no pretrained weights are used.
        progress (bool, optional): If True, displays a progress bar of the download to stderr. Default is True.
        **kwargs: parameters passed to either ``torchvision.models.regnet.RegNet`` or
            ``torchvision.models.regnet.BlockParams`` class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/regnet.py>`_
            for more detail about the classes.

    .. autoclass:: torchvision.models.RegNet_X_16GF_Weights
        :members:
    """
    weights = RegNet_X_16GF_Weights.verify(weights)

    params = BlockParams.from_init_params(depth=22, w_0=216, w_a=55.59, w_m=2.1, group_width=128, **kwargs)
    return _regnet(params, weights, progress, **kwargs)


@register_model()
@handle_legacy_interface(weights=("pretrained", RegNet_X_32GF_Weights.IMAGENET1K_V1))
def regnet_x_32gf(*, weights: Optional[RegNet_X_32GF_Weights] = None, progress: bool = True, **kwargs: Any) -> RegNet:
    """
    Constructs a RegNetX_32GF architecture from
    `Designing Network Design Spaces <https://arxiv.org/abs/2003.13678>`_.

    Args:
        weights (:class:`~torchvision.models.RegNet_X_32GF_Weights`, optional): The pretrained weights to use.
            See :class:`~torchvision.models.RegNet_X_32GF_Weights` below for more details and possible values.
            By default, no pretrained weights are used.
        progress (bool, optional): If True, displays a progress bar of the download to stderr. Default is True.
        **kwargs: parameters passed to either ``torchvision.models.regnet.RegNet`` or
            ``torchvision.models.regnet.BlockParams`` class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/regnet.py>`_
            for more detail about the classes.

    .. autoclass:: torchvision.models.RegNet_X_32GF_Weights
        :members:
    """
    weights = RegNet_X_32GF_Weights.verify(weights)

    params = BlockParams.from_init_params(depth=23, w_0=320, w_a=69.86, w_m=2.0, group_width=168, **kwargs)
    return _regnet(params, weights, progress, **kwargs)
