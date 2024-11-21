import warnings
from functools import partial
from typing import Any, List, Optional, Union

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch import Tensor
from torchvision.models import inception as inception_module
from torchvision.models.inception import Inception_V3_Weights, InceptionOutputs

from ...transforms._presets import ImageClassification
from .._api import register_model, Weights, WeightsEnum
from .._meta import _IMAGENET_CATEGORIES
from .._utils import _ovewrite_named_param, handle_legacy_interface
from .utils import _fuse_modules, _replace_relu, quantize_model


__all__ = [
    "QuantizableInception3",
    "Inception_V3_QuantizedWeights",
    "inception_v3",
]


class QuantizableBasicConv2d(inception_module.BasicConv2d):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.relu = nn.ReLU()

    def forward(self, x: Tensor) -> Tensor:
        x = self.conv(x)
        x = self.bn(x)
        x = self.relu(x)
        return x

    def fuse_model(self, is_qat: Optional[bool] = None) -> None:
        _fuse_modules(self, ["conv", "bn", "relu"], is_qat, inplace=True)


class QuantizableInceptionA(inception_module.InceptionA):
    # TODO https://github.com/pytorch/vision/pull/4232#pullrequestreview-730461659
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, conv_block=QuantizableBasicConv2d, **kwargs)  # type: ignore[misc]
        self.myop = nn.quantized.FloatFunctional()

    def forward(self, x: Tensor) -> Tensor:
        outputs = self._forward(x)
        return self.myop.cat(outputs, 1)


class QuantizableInceptionB(inception_module.InceptionB):
    # TODO https://github.com/pytorch/vision/pull/4232#pullrequestreview-730461659
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, conv_block=QuantizableBasicConv2d, **kwargs)  # type: ignore[misc]
        self.myop = nn.quantized.FloatFunctional()

    def forward(self, x: Tensor) -> Tensor:
        outputs = self._forward(x)
        return self.myop.cat(outputs, 1)


class QuantizableInceptionC(inception_module.InceptionC):
    # TODO https://github.com/pytorch/vision/pull/4232#pullrequestreview-730461659
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, conv_block=QuantizableBasicConv2d, **kwargs)  # type: ignore[misc]
        self.myop = nn.quantized.FloatFunctional()

    def forward(self, x: Tensor) -> Tensor:
        outputs = self._forward(x)
        return self.myop.cat(outputs, 1)


class QuantizableInceptionD(inception_module.InceptionD):
    # TODO https://github.com/pytorch/vision/pull/4232#pullrequestreview-730461659
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, conv_block=QuantizableBasicConv2d, **kwargs)  # type: ignore[misc]
        self.myop = nn.quantized.FloatFunctional()

    def forward(self, x: Tensor) -> Tensor:
        outputs = self._forward(x)
        return self.myop.cat(outputs, 1)


class QuantizableInceptionE(inception_module.InceptionE):
    # TODO https://github.com/pytorch/vision/pull/4232#pullrequestreview-730461659
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, conv_block=QuantizableBasicConv2d, **kwargs)  # type: ignore[misc]
        self.myop1 = nn.quantized.FloatFunctional()
        self.myop2 = nn.quantized.FloatFunctional()
        self.myop3 = nn.quantized.FloatFunctional()

    def _forward(self, x: Tensor) -> List[Tensor]:
        branch1x1 = self.branch1x1(x)

        branch3x3 = self.branch3x3_1(x)
        branch3x3 = [self.branch3x3_2a(branch3x3), self.branch3x3_2b(branch3x3)]
        branch3x3 = self.myop1.cat(branch3x3, 1)

        branch3x3dbl = self.branch3x3dbl_1(x)
        branch3x3dbl = self.branch3x3dbl_2(branch3x3dbl)
        branch3x3dbl = [
            self.branch3x3dbl_3a(branch3x3dbl),
            self.branch3x3dbl_3b(branch3x3dbl),
        ]
        branch3x3dbl = self.myop2.cat(branch3x3dbl, 1)

        branch_pool = F.avg_pool2d(x, kernel_size=3, stride=1, padding=1)
        branch_pool = self.branch_pool(branch_pool)

        outputs = [branch1x1, branch3x3, branch3x3dbl, branch_pool]
        return outputs

    def forward(self, x: Tensor) -> Tensor:
        outputs = self._forward(x)
        return self.myop3.cat(outputs, 1)


class QuantizableInceptionAux(inception_module.InceptionAux):
    # TODO https://github.com/pytorch/vision/pull/4232#pullrequestreview-730461659
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, conv_block=QuantizableBasicConv2d, **kwargs)  # type: ignore[misc]


class QuantizableInception3(inception_module.Inception3):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(  # type: ignore[misc]
            *args,
            inception_blocks=[
                QuantizableBasicConv2d,
                QuantizableInceptionA,
                QuantizableInceptionB,
                QuantizableInceptionC,
                QuantizableInceptionD,
                QuantizableInceptionE,
                QuantizableInceptionAux,
            ],
            **kwargs,
        )
        self.quant = torch.ao.quantization.QuantStub()
        self.dequant = torch.ao.quantization.DeQuantStub()

    def forward(self, x: Tensor) -> InceptionOutputs:
        x = self._transform_input(x)
        x = self.quant(x)
        x, aux = self._forward(x)
        x = self.dequant(x)
        aux_defined = self.training and self.aux_logits
        if torch.jit.is_scripting():
            if not aux_defined:
                warnings.warn("Scripted QuantizableInception3 always returns QuantizableInception3 Tuple")
            return InceptionOutputs(x, aux)
        else:
            return self.eager_outputs(x, aux)

    def fuse_model(self, is_qat: Optional[bool] = None) -> None:
        r"""Fuse conv/bn/relu modules in inception model

        Fuse conv+bn+relu/ conv+relu/conv+bn modules to prepare for quantization.
        Model is modified in place.  Note that this operation does not change numerics
        and the model after modification is in floating point
        """

        for m in self.modules():
            if type(m) is QuantizableBasicConv2d:
                m.fuse_model(is_qat)


class Inception_V3_QuantizedWeights(WeightsEnum):
    IMAGENET1K_FBGEMM_V1 = Weights(
        url="https://download.pytorch.org/models/quantized/inception_v3_google_fbgemm-a2837893.pth",
        transforms=partial(ImageClassification, crop_size=299, resize_size=342),
        meta={
            "num_params": 27161264,
            "min_size": (75, 75),
            "categories": _IMAGENET_CATEGORIES,
            "backend": "fbgemm",
            "recipe": "https://github.com/pytorch/vision/tree/main/references/classification#post-training-quantized-models",
            "unquantized": Inception_V3_Weights.IMAGENET1K_V1,
            "_metrics": {
                "ImageNet-1K": {
                    "acc@1": 77.176,
                    "acc@5": 93.354,
                }
            },
            "_ops": 5.713,
            "_file_size": 23.146,
            "_docs": """
                These weights were produced by doing Post Training Quantization (eager mode) on top of the unquantized
                weights listed below.
            """,
        },
    )
    DEFAULT = IMAGENET1K_FBGEMM_V1


@register_model(name="quantized_inception_v3")
@handle_legacy_interface(
    weights=(
        "pretrained",
        lambda kwargs: Inception_V3_QuantizedWeights.IMAGENET1K_FBGEMM_V1
        if kwargs.get("quantize", False)
        else Inception_V3_Weights.IMAGENET1K_V1,
    )
)
def inception_v3(
    *,
    weights: Optional[Union[Inception_V3_QuantizedWeights, Inception_V3_Weights]] = None,
    progress: bool = True,
    quantize: bool = False,
    **kwargs: Any,
) -> QuantizableInception3:
    r"""Inception v3 model architecture from
    `Rethinking the Inception Architecture for Computer Vision <http://arxiv.org/abs/1512.00567>`__.

    .. note::
        **Important**: In contrast to the other models the inception_v3 expects tensors with a size of
        N x 3 x 299 x 299, so ensure your images are sized accordingly.

    .. note::
        Note that ``quantize = True`` returns a quantized model with 8 bit
        weights. Quantized models only support inference and run on CPUs.
        GPU inference is not yet supported.

    Args:
        weights (:class:`~torchvision.models.quantization.Inception_V3_QuantizedWeights` or :class:`~torchvision.models.Inception_V3_Weights`, optional): The pretrained
            weights for the model. See
            :class:`~torchvision.models.quantization.Inception_V3_QuantizedWeights` below for
            more details, and possible values. By default, no pre-trained
            weights are used.
        progress (bool, optional): If True, displays a progress bar of the download to stderr.
            Default is True.
        quantize (bool, optional): If True, return a quantized version of the model.
            Default is False.
        **kwargs: parameters passed to the ``torchvision.models.quantization.QuantizableInception3``
            base class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/quantization/inception.py>`_
            for more details about this class.

    .. autoclass:: torchvision.models.quantization.Inception_V3_QuantizedWeights
        :members:

    .. autoclass:: torchvision.models.Inception_V3_Weights
        :members:
        :noindex:
    """
    weights = (Inception_V3_QuantizedWeights if quantize else Inception_V3_Weights).verify(weights)

    original_aux_logits = kwargs.get("aux_logits", False)
    if weights is not None:
        if "transform_input" not in kwargs:
            _ovewrite_named_param(kwargs, "transform_input", True)
        _ovewrite_named_param(kwargs, "aux_logits", True)
        _ovewrite_named_param(kwargs, "num_classes", len(weights.meta["categories"]))
        if "backend" in weights.meta:
            _ovewrite_named_param(kwargs, "backend", weights.meta["backend"])
    backend = kwargs.pop("backend", "fbgemm")

    model = QuantizableInception3(**kwargs)
    _replace_relu(model)
    if quantize:
        quantize_model(model, backend)

    if weights is not None:
        if quantize and not original_aux_logits:
            model.aux_logits = False
            model.AuxLogits = None
        model.load_state_dict(weights.get_state_dict(progress=progress, check_hash=True))
        if not quantize and not original_aux_logits:
            model.aux_logits = False
            model.AuxLogits = None

    return model
