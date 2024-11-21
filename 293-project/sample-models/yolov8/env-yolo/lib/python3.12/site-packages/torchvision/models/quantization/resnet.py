from functools import partial
from typing import Any, List, Optional, Type, Union

import torch
import torch.nn as nn
from torch import Tensor
from torchvision.models.resnet import (
    BasicBlock,
    Bottleneck,
    ResNet,
    ResNet18_Weights,
    ResNet50_Weights,
    ResNeXt101_32X8D_Weights,
    ResNeXt101_64X4D_Weights,
)

from ...transforms._presets import ImageClassification
from .._api import register_model, Weights, WeightsEnum
from .._meta import _IMAGENET_CATEGORIES
from .._utils import _ovewrite_named_param, handle_legacy_interface
from .utils import _fuse_modules, _replace_relu, quantize_model


__all__ = [
    "QuantizableResNet",
    "ResNet18_QuantizedWeights",
    "ResNet50_QuantizedWeights",
    "ResNeXt101_32X8D_QuantizedWeights",
    "ResNeXt101_64X4D_QuantizedWeights",
    "resnet18",
    "resnet50",
    "resnext101_32x8d",
    "resnext101_64x4d",
]


class QuantizableBasicBlock(BasicBlock):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.add_relu = torch.nn.quantized.FloatFunctional()

    def forward(self, x: Tensor) -> Tensor:
        identity = x

        out = self.conv1(x)
        out = self.bn1(out)
        out = self.relu(out)

        out = self.conv2(out)
        out = self.bn2(out)

        if self.downsample is not None:
            identity = self.downsample(x)

        out = self.add_relu.add_relu(out, identity)

        return out

    def fuse_model(self, is_qat: Optional[bool] = None) -> None:
        _fuse_modules(self, [["conv1", "bn1", "relu"], ["conv2", "bn2"]], is_qat, inplace=True)
        if self.downsample:
            _fuse_modules(self.downsample, ["0", "1"], is_qat, inplace=True)


class QuantizableBottleneck(Bottleneck):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.skip_add_relu = nn.quantized.FloatFunctional()
        self.relu1 = nn.ReLU(inplace=False)
        self.relu2 = nn.ReLU(inplace=False)

    def forward(self, x: Tensor) -> Tensor:
        identity = x
        out = self.conv1(x)
        out = self.bn1(out)
        out = self.relu1(out)
        out = self.conv2(out)
        out = self.bn2(out)
        out = self.relu2(out)

        out = self.conv3(out)
        out = self.bn3(out)

        if self.downsample is not None:
            identity = self.downsample(x)
        out = self.skip_add_relu.add_relu(out, identity)

        return out

    def fuse_model(self, is_qat: Optional[bool] = None) -> None:
        _fuse_modules(
            self, [["conv1", "bn1", "relu1"], ["conv2", "bn2", "relu2"], ["conv3", "bn3"]], is_qat, inplace=True
        )
        if self.downsample:
            _fuse_modules(self.downsample, ["0", "1"], is_qat, inplace=True)


class QuantizableResNet(ResNet):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.quant = torch.ao.quantization.QuantStub()
        self.dequant = torch.ao.quantization.DeQuantStub()

    def forward(self, x: Tensor) -> Tensor:
        x = self.quant(x)
        # Ensure scriptability
        # super(QuantizableResNet,self).forward(x)
        # is not scriptable
        x = self._forward_impl(x)
        x = self.dequant(x)
        return x

    def fuse_model(self, is_qat: Optional[bool] = None) -> None:
        r"""Fuse conv/bn/relu modules in resnet models

        Fuse conv+bn+relu/ Conv+relu/conv+Bn modules to prepare for quantization.
        Model is modified in place.  Note that this operation does not change numerics
        and the model after modification is in floating point
        """
        _fuse_modules(self, ["conv1", "bn1", "relu"], is_qat, inplace=True)
        for m in self.modules():
            if type(m) is QuantizableBottleneck or type(m) is QuantizableBasicBlock:
                m.fuse_model(is_qat)


def _resnet(
    block: Type[Union[QuantizableBasicBlock, QuantizableBottleneck]],
    layers: List[int],
    weights: Optional[WeightsEnum],
    progress: bool,
    quantize: bool,
    **kwargs: Any,
) -> QuantizableResNet:
    if weights is not None:
        _ovewrite_named_param(kwargs, "num_classes", len(weights.meta["categories"]))
        if "backend" in weights.meta:
            _ovewrite_named_param(kwargs, "backend", weights.meta["backend"])
    backend = kwargs.pop("backend", "fbgemm")

    model = QuantizableResNet(block, layers, **kwargs)
    _replace_relu(model)
    if quantize:
        quantize_model(model, backend)

    if weights is not None:
        model.load_state_dict(weights.get_state_dict(progress=progress, check_hash=True))

    return model


_COMMON_META = {
    "min_size": (1, 1),
    "categories": _IMAGENET_CATEGORIES,
    "backend": "fbgemm",
    "recipe": "https://github.com/pytorch/vision/tree/main/references/classification#post-training-quantized-models",
    "_docs": """
        These weights were produced by doing Post Training Quantization (eager mode) on top of the unquantized
        weights listed below.
    """,
}


class ResNet18_QuantizedWeights(WeightsEnum):
    IMAGENET1K_FBGEMM_V1 = Weights(
        url="https://download.pytorch.org/models/quantized/resnet18_fbgemm_16fa66dd.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 11689512,
            "unquantized": ResNet18_Weights.IMAGENET1K_V1,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 69.494,
                    "acc@5": 88.882,
                }
            },
            "_ops": 1.814,
            "_file_size": 11.238,
        },
    )
    DEFAULT = IMAGENET1K_FBGEMM_V1


class ResNet50_QuantizedWeights(WeightsEnum):
    IMAGENET1K_FBGEMM_V1 = Weights(
        url="https://download.pytorch.org/models/quantized/resnet50_fbgemm_bf931d71.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 25557032,
            "unquantized": ResNet50_Weights.IMAGENET1K_V1,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 75.920,
                    "acc@5": 92.814,
                }
            },
            "_ops": 4.089,
            "_file_size": 24.759,
        },
    )
    IMAGENET1K_FBGEMM_V2 = Weights(
        url="https://download.pytorch.org/models/quantized/resnet50_fbgemm-23753f79.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "num_params": 25557032,
            "unquantized": ResNet50_Weights.IMAGENET1K_V2,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 80.282,
                    "acc@5": 94.976,
                }
            },
            "_ops": 4.089,
            "_file_size": 24.953,
        },
    )
    DEFAULT = IMAGENET1K_FBGEMM_V2


class ResNeXt101_32X8D_QuantizedWeights(WeightsEnum):
    IMAGENET1K_FBGEMM_V1 = Weights(
        url="https://download.pytorch.org/models/quantized/resnext101_32x8_fbgemm_09835ccf.pth",
        transforms=partial(ImageClassification, crop_size=224),
        meta={
            **_COMMON_META,
            "num_params": 88791336,
            "unquantized": ResNeXt101_32X8D_Weights.IMAGENET1K_V1,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 78.986,
                    "acc@5": 94.480,
                }
            },
            "_ops": 16.414,
            "_file_size": 86.034,
        },
    )
    IMAGENET1K_FBGEMM_V2 = Weights(
        url="https://download.pytorch.org/models/quantized/resnext101_32x8_fbgemm-ee16d00c.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "num_params": 88791336,
            "unquantized": ResNeXt101_32X8D_Weights.IMAGENET1K_V2,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 82.574,
                    "acc@5": 96.132,
                }
            },
            "_ops": 16.414,
            "_file_size": 86.645,
        },
    )
    DEFAULT = IMAGENET1K_FBGEMM_V2


class ResNeXt101_64X4D_QuantizedWeights(WeightsEnum):
    IMAGENET1K_FBGEMM_V1 = Weights(
        url="https://download.pytorch.org/models/quantized/resnext101_64x4d_fbgemm-605a1cb3.pth",
        transforms=partial(ImageClassification, crop_size=224, resize_size=232),
        meta={
            **_COMMON_META,
            "num_params": 83455272,
            "recipe": "https://github.com/pytorch/vision/pull/5935",
            "unquantized": ResNeXt101_64X4D_Weights.IMAGENET1K_V1,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 82.898,
                    "acc@5": 96.326,
                }
            },
            "_ops": 15.46,
            "_file_size": 81.556,
        },
    )
    DEFAULT = IMAGENET1K_FBGEMM_V1


@register_model(name="quantized_resnet18")
@handle_legacy_interface(
    weights=(
        "pretrained",
        lambda kwargs: ResNet18_QuantizedWeights.IMAGENET1K_FBGEMM_V1
        if kwargs.get("quantize", False)
        else ResNet18_Weights.IMAGENET1K_V1,
    )
)
def resnet18(
    *,
    weights: Optional[Union[ResNet18_QuantizedWeights, ResNet18_Weights]] = None,
    progress: bool = True,
    quantize: bool = False,
    **kwargs: Any,
) -> QuantizableResNet:
    """ResNet-18 model from
    `Deep Residual Learning for Image Recognition <https://arxiv.org/abs/1512.03385>`_

    .. note::
        Note that ``quantize = True`` returns a quantized model with 8 bit
        weights. Quantized models only support inference and run on CPUs.
        GPU inference is not yet supported.

    Args:
        weights (:class:`~torchvision.models.quantization.ResNet18_QuantizedWeights` or :class:`~torchvision.models.ResNet18_Weights`, optional): The
            pretrained weights for the model. See
            :class:`~torchvision.models.quantization.ResNet18_QuantizedWeights` below for
            more details, and possible values. By default, no pre-trained
            weights are used.
        progress (bool, optional): If True, displays a progress bar of the
            download to stderr. Default is True.
        quantize (bool, optional): If True, return a quantized version of the model. Default is False.
        **kwargs: parameters passed to the ``torchvision.models.quantization.QuantizableResNet``
            base class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/quantization/resnet.py>`_
            for more details about this class.

    .. autoclass:: torchvision.models.quantization.ResNet18_QuantizedWeights
        :members:

    .. autoclass:: torchvision.models.ResNet18_Weights
        :members:
        :noindex:
    """
    weights = (ResNet18_QuantizedWeights if quantize else ResNet18_Weights).verify(weights)

    return _resnet(QuantizableBasicBlock, [2, 2, 2, 2], weights, progress, quantize, **kwargs)


@register_model(name="quantized_resnet50")
@handle_legacy_interface(
    weights=(
        "pretrained",
        lambda kwargs: ResNet50_QuantizedWeights.IMAGENET1K_FBGEMM_V1
        if kwargs.get("quantize", False)
        else ResNet50_Weights.IMAGENET1K_V1,
    )
)
def resnet50(
    *,
    weights: Optional[Union[ResNet50_QuantizedWeights, ResNet50_Weights]] = None,
    progress: bool = True,
    quantize: bool = False,
    **kwargs: Any,
) -> QuantizableResNet:
    """ResNet-50 model from
    `Deep Residual Learning for Image Recognition <https://arxiv.org/abs/1512.03385>`_

    .. note::
        Note that ``quantize = True`` returns a quantized model with 8 bit
        weights. Quantized models only support inference and run on CPUs.
        GPU inference is not yet supported.

    Args:
        weights (:class:`~torchvision.models.quantization.ResNet50_QuantizedWeights` or :class:`~torchvision.models.ResNet50_Weights`, optional): The
            pretrained weights for the model. See
            :class:`~torchvision.models.quantization.ResNet50_QuantizedWeights` below for
            more details, and possible values. By default, no pre-trained
            weights are used.
        progress (bool, optional): If True, displays a progress bar of the
            download to stderr. Default is True.
        quantize (bool, optional): If True, return a quantized version of the model. Default is False.
        **kwargs: parameters passed to the ``torchvision.models.quantization.QuantizableResNet``
            base class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/quantization/resnet.py>`_
            for more details about this class.

    .. autoclass:: torchvision.models.quantization.ResNet50_QuantizedWeights
        :members:

    .. autoclass:: torchvision.models.ResNet50_Weights
        :members:
        :noindex:
    """
    weights = (ResNet50_QuantizedWeights if quantize else ResNet50_Weights).verify(weights)

    return _resnet(QuantizableBottleneck, [3, 4, 6, 3], weights, progress, quantize, **kwargs)


@register_model(name="quantized_resnext101_32x8d")
@handle_legacy_interface(
    weights=(
        "pretrained",
        lambda kwargs: ResNeXt101_32X8D_QuantizedWeights.IMAGENET1K_FBGEMM_V1
        if kwargs.get("quantize", False)
        else ResNeXt101_32X8D_Weights.IMAGENET1K_V1,
    )
)
def resnext101_32x8d(
    *,
    weights: Optional[Union[ResNeXt101_32X8D_QuantizedWeights, ResNeXt101_32X8D_Weights]] = None,
    progress: bool = True,
    quantize: bool = False,
    **kwargs: Any,
) -> QuantizableResNet:
    """ResNeXt-101 32x8d model from
    `Aggregated Residual Transformation for Deep Neural Networks <https://arxiv.org/abs/1611.05431>`_

    .. note::
        Note that ``quantize = True`` returns a quantized model with 8 bit
        weights. Quantized models only support inference and run on CPUs.
        GPU inference is not yet supported.

    Args:
        weights (:class:`~torchvision.models.quantization.ResNeXt101_32X8D_QuantizedWeights` or :class:`~torchvision.models.ResNeXt101_32X8D_Weights`, optional): The
            pretrained weights for the model. See
            :class:`~torchvision.models.quantization.ResNet101_32X8D_QuantizedWeights` below for
            more details, and possible values. By default, no pre-trained
            weights are used.
        progress (bool, optional): If True, displays a progress bar of the
            download to stderr. Default is True.
        quantize (bool, optional): If True, return a quantized version of the model. Default is False.
        **kwargs: parameters passed to the ``torchvision.models.quantization.QuantizableResNet``
            base class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/quantization/resnet.py>`_
            for more details about this class.

    .. autoclass:: torchvision.models.quantization.ResNeXt101_32X8D_QuantizedWeights
        :members:

    .. autoclass:: torchvision.models.ResNeXt101_32X8D_Weights
        :members:
        :noindex:
    """
    weights = (ResNeXt101_32X8D_QuantizedWeights if quantize else ResNeXt101_32X8D_Weights).verify(weights)

    _ovewrite_named_param(kwargs, "groups", 32)
    _ovewrite_named_param(kwargs, "width_per_group", 8)
    return _resnet(QuantizableBottleneck, [3, 4, 23, 3], weights, progress, quantize, **kwargs)


@register_model(name="quantized_resnext101_64x4d")
@handle_legacy_interface(
    weights=(
        "pretrained",
        lambda kwargs: ResNeXt101_64X4D_QuantizedWeights.IMAGENET1K_FBGEMM_V1
        if kwargs.get("quantize", False)
        else ResNeXt101_64X4D_Weights.IMAGENET1K_V1,
    )
)
def resnext101_64x4d(
    *,
    weights: Optional[Union[ResNeXt101_64X4D_QuantizedWeights, ResNeXt101_64X4D_Weights]] = None,
    progress: bool = True,
    quantize: bool = False,
    **kwargs: Any,
) -> QuantizableResNet:
    """ResNeXt-101 64x4d model from
    `Aggregated Residual Transformation for Deep Neural Networks <https://arxiv.org/abs/1611.05431>`_

    .. note::
        Note that ``quantize = True`` returns a quantized model with 8 bit
        weights. Quantized models only support inference and run on CPUs.
        GPU inference is not yet supported.

    Args:
        weights (:class:`~torchvision.models.quantization.ResNeXt101_64X4D_QuantizedWeights` or :class:`~torchvision.models.ResNeXt101_64X4D_Weights`, optional): The
            pretrained weights for the model. See
            :class:`~torchvision.models.quantization.ResNet101_64X4D_QuantizedWeights` below for
            more details, and possible values. By default, no pre-trained
            weights are used.
        progress (bool, optional): If True, displays a progress bar of the
            download to stderr. Default is True.
        quantize (bool, optional): If True, return a quantized version of the model. Default is False.
        **kwargs: parameters passed to the ``torchvision.models.quantization.QuantizableResNet``
            base class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/quantization/resnet.py>`_
            for more details about this class.

    .. autoclass:: torchvision.models.quantization.ResNeXt101_64X4D_QuantizedWeights
        :members:

    .. autoclass:: torchvision.models.ResNeXt101_64X4D_Weights
        :members:
        :noindex:
    """
    weights = (ResNeXt101_64X4D_QuantizedWeights if quantize else ResNeXt101_64X4D_Weights).verify(weights)

    _ovewrite_named_param(kwargs, "groups", 64)
    _ovewrite_named_param(kwargs, "width_per_group", 4)
    return _resnet(QuantizableBottleneck, [3, 4, 23, 3], weights, progress, quantize, **kwargs)
