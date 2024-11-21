from typing import List, Optional

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch import Tensor
from torch.nn.modules.batchnorm import BatchNorm2d
from torch.nn.modules.instancenorm import InstanceNorm2d
from torchvision.ops import Conv2dNormActivation

from ...transforms._presets import OpticalFlow
from ...utils import _log_api_usage_once
from .._api import register_model, Weights, WeightsEnum
from .._utils import handle_legacy_interface
from ._utils import grid_sample, make_coords_grid, upsample_flow


__all__ = (
    "RAFT",
    "raft_large",
    "raft_small",
    "Raft_Large_Weights",
    "Raft_Small_Weights",
)


class ResidualBlock(nn.Module):
    """Slightly modified Residual block with extra relu and biases."""

    def __init__(self, in_channels, out_channels, *, norm_layer, stride=1, always_project: bool = False):
        super().__init__()

        # Note regarding bias=True:
        # Usually we can pass bias=False in conv layers followed by a norm layer.
        # But in the RAFT training reference, the BatchNorm2d layers are only activated for the first dataset,
        # and frozen for the rest of the training process (i.e. set as eval()). The bias term is thus still useful
        # for the rest of the datasets. Technically, we could remove the bias for other norm layers like Instance norm
        # because these aren't frozen, but we don't bother (also, we wouldn't be able to load the original weights).
        self.convnormrelu1 = Conv2dNormActivation(
            in_channels, out_channels, norm_layer=norm_layer, kernel_size=3, stride=stride, bias=True
        )
        self.convnormrelu2 = Conv2dNormActivation(
            out_channels, out_channels, norm_layer=norm_layer, kernel_size=3, bias=True
        )

        # make mypy happy
        self.downsample: nn.Module

        if stride == 1 and not always_project:
            self.downsample = nn.Identity()
        else:
            self.downsample = Conv2dNormActivation(
                in_channels,
                out_channels,
                norm_layer=norm_layer,
                kernel_size=1,
                stride=stride,
                bias=True,
                activation_layer=None,
            )

        self.relu = nn.ReLU(inplace=True)

    def forward(self, x):
        y = x
        y = self.convnormrelu1(y)
        y = self.convnormrelu2(y)

        x = self.downsample(x)

        return self.relu(x + y)


class BottleneckBlock(nn.Module):
    """Slightly modified BottleNeck block (extra relu and biases)"""

    def __init__(self, in_channels, out_channels, *, norm_layer, stride=1):
        super().__init__()

        # See note in ResidualBlock for the reason behind bias=True
        self.convnormrelu1 = Conv2dNormActivation(
            in_channels, out_channels // 4, norm_layer=norm_layer, kernel_size=1, bias=True
        )
        self.convnormrelu2 = Conv2dNormActivation(
            out_channels // 4, out_channels // 4, norm_layer=norm_layer, kernel_size=3, stride=stride, bias=True
        )
        self.convnormrelu3 = Conv2dNormActivation(
            out_channels // 4, out_channels, norm_layer=norm_layer, kernel_size=1, bias=True
        )
        self.relu = nn.ReLU(inplace=True)

        if stride == 1:
            self.downsample = nn.Identity()
        else:
            self.downsample = Conv2dNormActivation(
                in_channels,
                out_channels,
                norm_layer=norm_layer,
                kernel_size=1,
                stride=stride,
                bias=True,
                activation_layer=None,
            )

    def forward(self, x):
        y = x
        y = self.convnormrelu1(y)
        y = self.convnormrelu2(y)
        y = self.convnormrelu3(y)

        x = self.downsample(x)

        return self.relu(x + y)


class FeatureEncoder(nn.Module):
    """The feature encoder, used both as the actual feature encoder, and as the context encoder.

    It must downsample its input by 8.
    """

    def __init__(
        self, *, block=ResidualBlock, layers=(64, 64, 96, 128, 256), strides=(2, 1, 2, 2), norm_layer=nn.BatchNorm2d
    ):
        super().__init__()

        if len(layers) != 5:
            raise ValueError(f"The expected number of layers is 5, instead got {len(layers)}")

        # See note in ResidualBlock for the reason behind bias=True
        self.convnormrelu = Conv2dNormActivation(
            3, layers[0], norm_layer=norm_layer, kernel_size=7, stride=strides[0], bias=True
        )

        self.layer1 = self._make_2_blocks(block, layers[0], layers[1], norm_layer=norm_layer, first_stride=strides[1])
        self.layer2 = self._make_2_blocks(block, layers[1], layers[2], norm_layer=norm_layer, first_stride=strides[2])
        self.layer3 = self._make_2_blocks(block, layers[2], layers[3], norm_layer=norm_layer, first_stride=strides[3])

        self.conv = nn.Conv2d(layers[3], layers[4], kernel_size=1)

        for m in self.modules():
            if isinstance(m, nn.Conv2d):
                nn.init.kaiming_normal_(m.weight, mode="fan_out", nonlinearity="relu")
            elif isinstance(m, (nn.BatchNorm2d, nn.InstanceNorm2d)):
                if m.weight is not None:
                    nn.init.constant_(m.weight, 1)
                if m.bias is not None:
                    nn.init.constant_(m.bias, 0)

        num_downsamples = len(list(filter(lambda s: s == 2, strides)))
        self.output_dim = layers[-1]
        self.downsample_factor = 2**num_downsamples

    def _make_2_blocks(self, block, in_channels, out_channels, norm_layer, first_stride):
        block1 = block(in_channels, out_channels, norm_layer=norm_layer, stride=first_stride)
        block2 = block(out_channels, out_channels, norm_layer=norm_layer, stride=1)
        return nn.Sequential(block1, block2)

    def forward(self, x):
        x = self.convnormrelu(x)

        x = self.layer1(x)
        x = self.layer2(x)
        x = self.layer3(x)

        x = self.conv(x)

        return x


class MotionEncoder(nn.Module):
    """The motion encoder, part of the update block.

    Takes the current predicted flow and the correlation features as input and returns an encoded version of these.
    """

    def __init__(self, *, in_channels_corr, corr_layers=(256, 192), flow_layers=(128, 64), out_channels=128):
        super().__init__()

        if len(flow_layers) != 2:
            raise ValueError(f"The expected number of flow_layers is 2, instead got {len(flow_layers)}")
        if len(corr_layers) not in (1, 2):
            raise ValueError(f"The number of corr_layers should be 1 or 2, instead got {len(corr_layers)}")

        self.convcorr1 = Conv2dNormActivation(in_channels_corr, corr_layers[0], norm_layer=None, kernel_size=1)
        if len(corr_layers) == 2:
            self.convcorr2 = Conv2dNormActivation(corr_layers[0], corr_layers[1], norm_layer=None, kernel_size=3)
        else:
            self.convcorr2 = nn.Identity()

        self.convflow1 = Conv2dNormActivation(2, flow_layers[0], norm_layer=None, kernel_size=7)
        self.convflow2 = Conv2dNormActivation(flow_layers[0], flow_layers[1], norm_layer=None, kernel_size=3)

        # out_channels - 2 because we cat the flow (2 channels) at the end
        self.conv = Conv2dNormActivation(
            corr_layers[-1] + flow_layers[-1], out_channels - 2, norm_layer=None, kernel_size=3
        )

        self.out_channels = out_channels

    def forward(self, flow, corr_features):
        corr = self.convcorr1(corr_features)
        corr = self.convcorr2(corr)

        flow_orig = flow
        flow = self.convflow1(flow)
        flow = self.convflow2(flow)

        corr_flow = torch.cat([corr, flow], dim=1)
        corr_flow = self.conv(corr_flow)
        return torch.cat([corr_flow, flow_orig], dim=1)


class ConvGRU(nn.Module):
    """Convolutional Gru unit."""

    def __init__(self, *, input_size, hidden_size, kernel_size, padding):
        super().__init__()
        self.convz = nn.Conv2d(hidden_size + input_size, hidden_size, kernel_size=kernel_size, padding=padding)
        self.convr = nn.Conv2d(hidden_size + input_size, hidden_size, kernel_size=kernel_size, padding=padding)
        self.convq = nn.Conv2d(hidden_size + input_size, hidden_size, kernel_size=kernel_size, padding=padding)

    def forward(self, h, x):
        hx = torch.cat([h, x], dim=1)
        z = torch.sigmoid(self.convz(hx))
        r = torch.sigmoid(self.convr(hx))
        q = torch.tanh(self.convq(torch.cat([r * h, x], dim=1)))
        h = (1 - z) * h + z * q
        return h


def _pass_through_h(h, _):
    # Declared here for torchscript
    return h


class RecurrentBlock(nn.Module):
    """Recurrent block, part of the update block.

    Takes the current hidden state and the concatenation of (motion encoder output, context) as input.
    Returns an updated hidden state.
    """

    def __init__(self, *, input_size, hidden_size, kernel_size=((1, 5), (5, 1)), padding=((0, 2), (2, 0))):
        super().__init__()

        if len(kernel_size) != len(padding):
            raise ValueError(
                f"kernel_size should have the same length as padding, instead got len(kernel_size) = {len(kernel_size)} and len(padding) = {len(padding)}"
            )
        if len(kernel_size) not in (1, 2):
            raise ValueError(f"kernel_size should either 1 or 2, instead got {len(kernel_size)}")

        self.convgru1 = ConvGRU(
            input_size=input_size, hidden_size=hidden_size, kernel_size=kernel_size[0], padding=padding[0]
        )
        if len(kernel_size) == 2:
            self.convgru2 = ConvGRU(
                input_size=input_size, hidden_size=hidden_size, kernel_size=kernel_size[1], padding=padding[1]
            )
        else:
            self.convgru2 = _pass_through_h

        self.hidden_size = hidden_size

    def forward(self, h, x):
        h = self.convgru1(h, x)
        h = self.convgru2(h, x)
        return h


class FlowHead(nn.Module):
    """Flow head, part of the update block.

    Takes the hidden state of the recurrent unit as input, and outputs the predicted "delta flow".
    """

    def __init__(self, *, in_channels, hidden_size):
        super().__init__()
        self.conv1 = nn.Conv2d(in_channels, hidden_size, 3, padding=1)
        self.conv2 = nn.Conv2d(hidden_size, 2, 3, padding=1)
        self.relu = nn.ReLU(inplace=True)

    def forward(self, x):
        return self.conv2(self.relu(self.conv1(x)))


class UpdateBlock(nn.Module):
    """The update block which contains the motion encoder, the recurrent block, and the flow head.

    It must expose a ``hidden_state_size`` attribute which is the hidden state size of its recurrent block.
    """

    def __init__(self, *, motion_encoder, recurrent_block, flow_head):
        super().__init__()
        self.motion_encoder = motion_encoder
        self.recurrent_block = recurrent_block
        self.flow_head = flow_head

        self.hidden_state_size = recurrent_block.hidden_size

    def forward(self, hidden_state, context, corr_features, flow):
        motion_features = self.motion_encoder(flow, corr_features)
        x = torch.cat([context, motion_features], dim=1)

        hidden_state = self.recurrent_block(hidden_state, x)
        delta_flow = self.flow_head(hidden_state)
        return hidden_state, delta_flow


class MaskPredictor(nn.Module):
    """Mask predictor to be used when upsampling the predicted flow.

    It takes the hidden state of the recurrent unit as input and outputs the mask.
    This is not used in the raft-small model.
    """

    def __init__(self, *, in_channels, hidden_size, multiplier=0.25):
        super().__init__()
        self.convrelu = Conv2dNormActivation(in_channels, hidden_size, norm_layer=None, kernel_size=3)
        # 8 * 8 * 9 because the predicted flow is downsampled by 8, from the downsampling of the initial FeatureEncoder,
        # and we interpolate with all 9 surrounding neighbors. See paper and appendix B.
        self.conv = nn.Conv2d(hidden_size, 8 * 8 * 9, 1, padding=0)

        # In the original code, they use a factor of 0.25 to "downweight the gradients" of that branch.
        # See e.g. https://github.com/princeton-vl/RAFT/issues/119#issuecomment-953950419
        # or https://github.com/princeton-vl/RAFT/issues/24.
        # It doesn't seem to affect epe significantly and can likely be set to 1.
        self.multiplier = multiplier

    def forward(self, x):
        x = self.convrelu(x)
        x = self.conv(x)
        return self.multiplier * x


class CorrBlock(nn.Module):
    """The correlation block.

    Creates a correlation pyramid with ``num_levels`` levels from the outputs of the feature encoder,
    and then indexes from this pyramid to create correlation features.
    The "indexing" of a given centroid pixel x' is done by concatenating its surrounding neighbors that
    are within a ``radius``, according to the infinity norm (see paper section 3.2).
    Note: typo in the paper, it should be infinity norm, not 1-norm.
    """

    def __init__(self, *, num_levels: int = 4, radius: int = 4):
        super().__init__()
        self.num_levels = num_levels
        self.radius = radius

        self.corr_pyramid: List[Tensor] = [torch.tensor(0)]  # useless, but torchscript is otherwise confused :')

        # The neighborhood of a centroid pixel x' is {x' + delta, ||delta||_inf <= radius}
        # so it's a square surrounding x', and its sides have a length of 2 * radius + 1
        # The paper claims that it's ||.||_1 instead of ||.||_inf but it's a typo:
        # https://github.com/princeton-vl/RAFT/issues/122
        self.out_channels = num_levels * (2 * radius + 1) ** 2

    def build_pyramid(self, fmap1, fmap2):
        """Build the correlation pyramid from two feature maps.

        The correlation volume is first computed as the dot product of each pair (pixel_in_fmap1, pixel_in_fmap2)
        The last 2 dimensions of the correlation volume are then pooled num_levels times at different resolutions
        to build the correlation pyramid.
        """

        if fmap1.shape != fmap2.shape:
            raise ValueError(
                f"Input feature maps should have the same shape, instead got {fmap1.shape} (fmap1.shape) != {fmap2.shape} (fmap2.shape)"
            )

        # Explaining min_fmap_size below: the fmaps are down-sampled (num_levels - 1) times by a factor of 2.
        # The last corr_volume most have at least 2 values (hence the 2* factor), otherwise grid_sample() would
        # produce nans in its output.
        min_fmap_size = 2 * (2 ** (self.num_levels - 1))
        if any(fmap_size < min_fmap_size for fmap_size in fmap1.shape[-2:]):
            raise ValueError(
                "Feature maps are too small to be down-sampled by the correlation pyramid. "
                f"H and W of feature maps should be at least {min_fmap_size}; got: {fmap1.shape[-2:]}. "
                "Remember that input images to the model are downsampled by 8, so that means their "
                f"dimensions should be at least 8 * {min_fmap_size} = {8 * min_fmap_size}."
            )

        corr_volume = self._compute_corr_volume(fmap1, fmap2)

        batch_size, h, w, num_channels, _, _ = corr_volume.shape  # _, _ = h, w
        corr_volume = corr_volume.reshape(batch_size * h * w, num_channels, h, w)
        self.corr_pyramid = [corr_volume]
        for _ in range(self.num_levels - 1):
            corr_volume = F.avg_pool2d(corr_volume, kernel_size=2, stride=2)
            self.corr_pyramid.append(corr_volume)

    def index_pyramid(self, centroids_coords):
        """Return correlation features by indexing from the pyramid."""
        neighborhood_side_len = 2 * self.radius + 1  # see note in __init__ about out_channels
        di = torch.linspace(-self.radius, self.radius, neighborhood_side_len)
        dj = torch.linspace(-self.radius, self.radius, neighborhood_side_len)
        delta = torch.stack(torch.meshgrid(di, dj, indexing="ij"), dim=-1).to(centroids_coords.device)
        delta = delta.view(1, neighborhood_side_len, neighborhood_side_len, 2)

        batch_size, _, h, w = centroids_coords.shape  # _ = 2
        centroids_coords = centroids_coords.permute(0, 2, 3, 1).reshape(batch_size * h * w, 1, 1, 2)

        indexed_pyramid = []
        for corr_volume in self.corr_pyramid:
            sampling_coords = centroids_coords + delta  # end shape is (batch_size * h * w, side_len, side_len, 2)
            indexed_corr_volume = grid_sample(corr_volume, sampling_coords, align_corners=True, mode="bilinear").view(
                batch_size, h, w, -1
            )
            indexed_pyramid.append(indexed_corr_volume)
            centroids_coords = centroids_coords / 2

        corr_features = torch.cat(indexed_pyramid, dim=-1).permute(0, 3, 1, 2).contiguous()

        expected_output_shape = (batch_size, self.out_channels, h, w)
        if corr_features.shape != expected_output_shape:
            raise ValueError(
                f"Output shape of index pyramid is incorrect. Should be {expected_output_shape}, got {corr_features.shape}"
            )

        return corr_features

    def _compute_corr_volume(self, fmap1, fmap2):
        batch_size, num_channels, h, w = fmap1.shape
        fmap1 = fmap1.view(batch_size, num_channels, h * w)
        fmap2 = fmap2.view(batch_size, num_channels, h * w)

        corr = torch.matmul(fmap1.transpose(1, 2), fmap2)
        corr = corr.view(batch_size, h, w, 1, h, w)
        return corr / torch.sqrt(torch.tensor(num_channels))


class RAFT(nn.Module):
    def __init__(self, *, feature_encoder, context_encoder, corr_block, update_block, mask_predictor=None):
        """RAFT model from
        `RAFT: Recurrent All Pairs Field Transforms for Optical Flow <https://arxiv.org/abs/2003.12039>`_.

        args:
            feature_encoder (nn.Module): The feature encoder. It must downsample the input by 8.
                Its input is the concatenation of ``image1`` and ``image2``.
            context_encoder (nn.Module): The context encoder. It must downsample the input by 8.
                Its input is ``image1``. As in the original implementation, its output will be split into 2 parts:

                - one part will be used as the actual "context", passed to the recurrent unit of the ``update_block``
                - one part will be used to initialize the hidden state of the recurrent unit of
                  the ``update_block``

                These 2 parts are split according to the ``hidden_state_size`` of the ``update_block``, so the output
                of the ``context_encoder`` must be strictly greater than ``hidden_state_size``.

            corr_block (nn.Module): The correlation block, which creates a correlation pyramid from the output of the
                ``feature_encoder``, and then indexes from this pyramid to create correlation features. It must expose
                2 methods:

                - a ``build_pyramid`` method that takes ``feature_map_1`` and ``feature_map_2`` as input (these are the
                  output of the ``feature_encoder``).
                - a ``index_pyramid`` method that takes the coordinates of the centroid pixels as input, and returns
                  the correlation features. See paper section 3.2.

                It must expose an ``out_channels`` attribute.

            update_block (nn.Module): The update block, which contains the motion encoder, the recurrent unit, and the
                flow head. It takes as input the hidden state of its recurrent unit, the context, the correlation
                features, and the current predicted flow. It outputs an updated hidden state, and the ``delta_flow``
                prediction (see paper appendix A). It must expose a ``hidden_state_size`` attribute.
            mask_predictor (nn.Module, optional): Predicts the mask that will be used to upsample the predicted flow.
                The output channel must be 8 * 8 * 9 - see paper section 3.3, and Appendix B.
                If ``None`` (default), the flow is upsampled using interpolation.
        """
        super().__init__()
        _log_api_usage_once(self)

        self.feature_encoder = feature_encoder
        self.context_encoder = context_encoder
        self.corr_block = corr_block
        self.update_block = update_block

        self.mask_predictor = mask_predictor

        if not hasattr(self.update_block, "hidden_state_size"):
            raise ValueError("The update_block parameter should expose a 'hidden_state_size' attribute.")

    def forward(self, image1, image2, num_flow_updates: int = 12):

        batch_size, _, h, w = image1.shape
        if (h, w) != image2.shape[-2:]:
            raise ValueError(f"input images should have the same shape, instead got ({h}, {w}) != {image2.shape[-2:]}")
        if not (h % 8 == 0) and (w % 8 == 0):
            raise ValueError(f"input image H and W should be divisible by 8, instead got {h} (h) and {w} (w)")

        fmaps = self.feature_encoder(torch.cat([image1, image2], dim=0))
        fmap1, fmap2 = torch.chunk(fmaps, chunks=2, dim=0)
        if fmap1.shape[-2:] != (h // 8, w // 8):
            raise ValueError("The feature encoder should downsample H and W by 8")

        self.corr_block.build_pyramid(fmap1, fmap2)

        context_out = self.context_encoder(image1)
        if context_out.shape[-2:] != (h // 8, w // 8):
            raise ValueError("The context encoder should downsample H and W by 8")

        # As in the original paper, the actual output of the context encoder is split in 2 parts:
        # - one part is used to initialize the hidden state of the recurent units of the update block
        # - the rest is the "actual" context.
        hidden_state_size = self.update_block.hidden_state_size
        out_channels_context = context_out.shape[1] - hidden_state_size
        if out_channels_context <= 0:
            raise ValueError(
                f"The context encoder outputs {context_out.shape[1]} channels, but it should have at strictly more than hidden_state={hidden_state_size} channels"
            )
        hidden_state, context = torch.split(context_out, [hidden_state_size, out_channels_context], dim=1)
        hidden_state = torch.tanh(hidden_state)
        context = F.relu(context)

        coords0 = make_coords_grid(batch_size, h // 8, w // 8).to(fmap1.device)
        coords1 = make_coords_grid(batch_size, h // 8, w // 8).to(fmap1.device)

        flow_predictions = []
        for _ in range(num_flow_updates):
            coords1 = coords1.detach()  # Don't backpropagate gradients through this branch, see paper
            corr_features = self.corr_block.index_pyramid(centroids_coords=coords1)

            flow = coords1 - coords0
            hidden_state, delta_flow = self.update_block(hidden_state, context, corr_features, flow)

            coords1 = coords1 + delta_flow

            up_mask = None if self.mask_predictor is None else self.mask_predictor(hidden_state)
            upsampled_flow = upsample_flow(flow=(coords1 - coords0), up_mask=up_mask)
            flow_predictions.append(upsampled_flow)

        return flow_predictions


_COMMON_META = {
    "min_size": (128, 128),
}


class Raft_Large_Weights(WeightsEnum):
    """The metrics reported here are as follows.

    ``epe`` is the "end-point-error" and indicates how far (in pixels) the
    predicted flow is from its true value. This is averaged over all pixels
    of all images. ``per_image_epe`` is similar, but the average is different:
    the epe is first computed on each image independently, and then averaged
    over all images. This corresponds to "Fl-epe" (sometimes written "F1-epe")
    in the original paper, and it's only used on Kitti. ``fl-all`` is also a
    Kitti-specific metric, defined by the author of the dataset and used for the
    Kitti leaderboard. It corresponds to the average of pixels whose epe is
    either <3px, or <5% of flow's 2-norm.
    """

    C_T_V1 = Weights(
        # Weights ported from https://github.com/princeton-vl/RAFT
        url="https://download.pytorch.org/models/raft_large_C_T_V1-22a6c225.pth",
        transforms=OpticalFlow,
        meta={
            **_COMMON_META,
            "num_params": 5257536,
            "recipe": "https://github.com/princeton-vl/RAFT",
            "_metrics": {
                "Sintel-Train-Cleanpass": {"epe": 1.4411},
                "Sintel-Train-Finalpass": {"epe": 2.7894},
                "Kitti-Train": {"per_image_epe": 5.0172, "fl_all": 17.4506},
            },
            "_ops": 211.007,
            "_file_size": 20.129,
            "_docs": """These weights were ported from the original paper. They
            are trained on :class:`~torchvision.datasets.FlyingChairs` +
            :class:`~torchvision.datasets.FlyingThings3D`.""",
        },
    )

    C_T_V2 = Weights(
        url="https://download.pytorch.org/models/raft_large_C_T_V2-1bb1363a.pth",
        transforms=OpticalFlow,
        meta={
            **_COMMON_META,
            "num_params": 5257536,
            "recipe": "https://github.com/pytorch/vision/tree/main/references/optical_flow",
            "_metrics": {
                "Sintel-Train-Cleanpass": {"epe": 1.3822},
                "Sintel-Train-Finalpass": {"epe": 2.7161},
                "Kitti-Train": {"per_image_epe": 4.5118, "fl_all": 16.0679},
            },
            "_ops": 211.007,
            "_file_size": 20.129,
            "_docs": """These weights were trained from scratch on
            :class:`~torchvision.datasets.FlyingChairs` +
            :class:`~torchvision.datasets.FlyingThings3D`.""",
        },
    )

    C_T_SKHT_V1 = Weights(
        # Weights ported from https://github.com/princeton-vl/RAFT
        url="https://download.pytorch.org/models/raft_large_C_T_SKHT_V1-0b8c9e55.pth",
        transforms=OpticalFlow,
        meta={
            **_COMMON_META,
            "num_params": 5257536,
            "recipe": "https://github.com/princeton-vl/RAFT",
            "_metrics": {
                "Sintel-Test-Cleanpass": {"epe": 1.94},
                "Sintel-Test-Finalpass": {"epe": 3.18},
            },
            "_ops": 211.007,
            "_file_size": 20.129,
            "_docs": """
                These weights were ported from the original paper. They are
                trained on :class:`~torchvision.datasets.FlyingChairs` +
                :class:`~torchvision.datasets.FlyingThings3D` and fine-tuned on
                Sintel. The Sintel fine-tuning step is a combination of
                :class:`~torchvision.datasets.Sintel`,
                :class:`~torchvision.datasets.KittiFlow`,
                :class:`~torchvision.datasets.HD1K`, and
                :class:`~torchvision.datasets.FlyingThings3D` (clean pass).
            """,
        },
    )

    C_T_SKHT_V2 = Weights(
        url="https://download.pytorch.org/models/raft_large_C_T_SKHT_V2-ff5fadd5.pth",
        transforms=OpticalFlow,
        meta={
            **_COMMON_META,
            "num_params": 5257536,
            "recipe": "https://github.com/pytorch/vision/tree/main/references/optical_flow",
            "_metrics": {
                "Sintel-Test-Cleanpass": {"epe": 1.819},
                "Sintel-Test-Finalpass": {"epe": 3.067},
            },
            "_ops": 211.007,
            "_file_size": 20.129,
            "_docs": """
                These weights were trained from scratch. They are
                pre-trained on :class:`~torchvision.datasets.FlyingChairs` +
                :class:`~torchvision.datasets.FlyingThings3D` and then
                fine-tuned on Sintel. The Sintel fine-tuning step is a
                combination of :class:`~torchvision.datasets.Sintel`,
                :class:`~torchvision.datasets.KittiFlow`,
                :class:`~torchvision.datasets.HD1K`, and
                :class:`~torchvision.datasets.FlyingThings3D` (clean pass).
            """,
        },
    )

    C_T_SKHT_K_V1 = Weights(
        # Weights ported from https://github.com/princeton-vl/RAFT
        url="https://download.pytorch.org/models/raft_large_C_T_SKHT_K_V1-4a6a5039.pth",
        transforms=OpticalFlow,
        meta={
            **_COMMON_META,
            "num_params": 5257536,
            "recipe": "https://github.com/princeton-vl/RAFT",
            "_metrics": {
                "Kitti-Test": {"fl_all": 5.10},
            },
            "_ops": 211.007,
            "_file_size": 20.129,
            "_docs": """
                These weights were ported from the original paper. They are
                pre-trained on :class:`~torchvision.datasets.FlyingChairs` +
                :class:`~torchvision.datasets.FlyingThings3D`,
                fine-tuned on Sintel, and then fine-tuned on
                :class:`~torchvision.datasets.KittiFlow`. The Sintel fine-tuning
                step was described above.
            """,
        },
    )

    C_T_SKHT_K_V2 = Weights(
        url="https://download.pytorch.org/models/raft_large_C_T_SKHT_K_V2-b5c70766.pth",
        transforms=OpticalFlow,
        meta={
            **_COMMON_META,
            "num_params": 5257536,
            "recipe": "https://github.com/pytorch/vision/tree/main/references/optical_flow",
            "_metrics": {
                "Kitti-Test": {"fl_all": 5.19},
            },
            "_ops": 211.007,
            "_file_size": 20.129,
            "_docs": """
                These weights were trained from scratch. They are
                pre-trained on :class:`~torchvision.datasets.FlyingChairs` +
                :class:`~torchvision.datasets.FlyingThings3D`,
                fine-tuned on Sintel, and then fine-tuned on
                :class:`~torchvision.datasets.KittiFlow`. The Sintel fine-tuning
                step was described above.
            """,
        },
    )

    DEFAULT = C_T_SKHT_V2


class Raft_Small_Weights(WeightsEnum):
    """The metrics reported here are as follows.

    ``epe`` is the "end-point-error" and indicates how far (in pixels) the
    predicted flow is from its true value. This is averaged over all pixels
    of all images. ``per_image_epe`` is similar, but the average is different:
    the epe is first computed on each image independently, and then averaged
    over all images. This corresponds to "Fl-epe" (sometimes written "F1-epe")
    in the original paper, and it's only used on Kitti. ``fl-all`` is also a
    Kitti-specific metric, defined by the author of the dataset and used for the
    Kitti leaderboard. It corresponds to the average of pixels whose epe is
    either <3px, or <5% of flow's 2-norm.
    """

    C_T_V1 = Weights(
        # Weights ported from https://github.com/princeton-vl/RAFT
        url="https://download.pytorch.org/models/raft_small_C_T_V1-ad48884c.pth",
        transforms=OpticalFlow,
        meta={
            **_COMMON_META,
            "num_params": 990162,
            "recipe": "https://github.com/princeton-vl/RAFT",
            "_metrics": {
                "Sintel-Train-Cleanpass": {"epe": 2.1231},
                "Sintel-Train-Finalpass": {"epe": 3.2790},
                "Kitti-Train": {"per_image_epe": 7.6557, "fl_all": 25.2801},
            },
            "_ops": 47.655,
            "_file_size": 3.821,
            "_docs": """These weights were ported from the original paper. They
            are trained on :class:`~torchvision.datasets.FlyingChairs` +
            :class:`~torchvision.datasets.FlyingThings3D`.""",
        },
    )
    C_T_V2 = Weights(
        url="https://download.pytorch.org/models/raft_small_C_T_V2-01064c6d.pth",
        transforms=OpticalFlow,
        meta={
            **_COMMON_META,
            "num_params": 990162,
            "recipe": "https://github.com/pytorch/vision/tree/main/references/optical_flow",
            "_metrics": {
                "Sintel-Train-Cleanpass": {"epe": 1.9901},
                "Sintel-Train-Finalpass": {"epe": 3.2831},
                "Kitti-Train": {"per_image_epe": 7.5978, "fl_all": 25.2369},
            },
            "_ops": 47.655,
            "_file_size": 3.821,
            "_docs": """These weights were trained from scratch on
            :class:`~torchvision.datasets.FlyingChairs` +
            :class:`~torchvision.datasets.FlyingThings3D`.""",
        },
    )

    DEFAULT = C_T_V2


def _raft(
    *,
    weights=None,
    progress=False,
    # Feature encoder
    feature_encoder_layers,
    feature_encoder_block,
    feature_encoder_norm_layer,
    # Context encoder
    context_encoder_layers,
    context_encoder_block,
    context_encoder_norm_layer,
    # Correlation block
    corr_block_num_levels,
    corr_block_radius,
    # Motion encoder
    motion_encoder_corr_layers,
    motion_encoder_flow_layers,
    motion_encoder_out_channels,
    # Recurrent block
    recurrent_block_hidden_state_size,
    recurrent_block_kernel_size,
    recurrent_block_padding,
    # Flow Head
    flow_head_hidden_size,
    # Mask predictor
    use_mask_predictor,
    **kwargs,
):
    feature_encoder = kwargs.pop("feature_encoder", None) or FeatureEncoder(
        block=feature_encoder_block, layers=feature_encoder_layers, norm_layer=feature_encoder_norm_layer
    )
    context_encoder = kwargs.pop("context_encoder", None) or FeatureEncoder(
        block=context_encoder_block, layers=context_encoder_layers, norm_layer=context_encoder_norm_layer
    )

    corr_block = kwargs.pop("corr_block", None) or CorrBlock(num_levels=corr_block_num_levels, radius=corr_block_radius)

    update_block = kwargs.pop("update_block", None)
    if update_block is None:
        motion_encoder = MotionEncoder(
            in_channels_corr=corr_block.out_channels,
            corr_layers=motion_encoder_corr_layers,
            flow_layers=motion_encoder_flow_layers,
            out_channels=motion_encoder_out_channels,
        )

        # See comments in forward pass of RAFT class about why we split the output of the context encoder
        out_channels_context = context_encoder_layers[-1] - recurrent_block_hidden_state_size
        recurrent_block = RecurrentBlock(
            input_size=motion_encoder.out_channels + out_channels_context,
            hidden_size=recurrent_block_hidden_state_size,
            kernel_size=recurrent_block_kernel_size,
            padding=recurrent_block_padding,
        )

        flow_head = FlowHead(in_channels=recurrent_block_hidden_state_size, hidden_size=flow_head_hidden_size)

        update_block = UpdateBlock(motion_encoder=motion_encoder, recurrent_block=recurrent_block, flow_head=flow_head)

    mask_predictor = kwargs.pop("mask_predictor", None)
    if mask_predictor is None and use_mask_predictor:
        mask_predictor = MaskPredictor(
            in_channels=recurrent_block_hidden_state_size,
            hidden_size=256,
            multiplier=0.25,  # See comment in MaskPredictor about this
        )

    model = RAFT(
        feature_encoder=feature_encoder,
        context_encoder=context_encoder,
        corr_block=corr_block,
        update_block=update_block,
        mask_predictor=mask_predictor,
        **kwargs,  # not really needed, all params should be consumed by now
    )

    if weights is not None:
        model.load_state_dict(weights.get_state_dict(progress=progress, check_hash=True))

    return model


@register_model()
@handle_legacy_interface(weights=("pretrained", Raft_Large_Weights.C_T_SKHT_V2))
def raft_large(*, weights: Optional[Raft_Large_Weights] = None, progress=True, **kwargs) -> RAFT:
    """RAFT model from
    `RAFT: Recurrent All Pairs Field Transforms for Optical Flow <https://arxiv.org/abs/2003.12039>`_.

    Please see the example below for a tutorial on how to use this model.

    Args:
        weights(:class:`~torchvision.models.optical_flow.Raft_Large_Weights`, optional): The
            pretrained weights to use. See
            :class:`~torchvision.models.optical_flow.Raft_Large_Weights`
            below for more details, and possible values. By default, no
            pre-trained weights are used.
        progress (bool): If True, displays a progress bar of the download to stderr. Default is True.
        **kwargs: parameters passed to the ``torchvision.models.optical_flow.RAFT``
            base class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/optical_flow/raft.py>`_
            for more details about this class.

    .. autoclass:: torchvision.models.optical_flow.Raft_Large_Weights
        :members:
    """

    weights = Raft_Large_Weights.verify(weights)

    return _raft(
        weights=weights,
        progress=progress,
        # Feature encoder
        feature_encoder_layers=(64, 64, 96, 128, 256),
        feature_encoder_block=ResidualBlock,
        feature_encoder_norm_layer=InstanceNorm2d,
        # Context encoder
        context_encoder_layers=(64, 64, 96, 128, 256),
        context_encoder_block=ResidualBlock,
        context_encoder_norm_layer=BatchNorm2d,
        # Correlation block
        corr_block_num_levels=4,
        corr_block_radius=4,
        # Motion encoder
        motion_encoder_corr_layers=(256, 192),
        motion_encoder_flow_layers=(128, 64),
        motion_encoder_out_channels=128,
        # Recurrent block
        recurrent_block_hidden_state_size=128,
        recurrent_block_kernel_size=((1, 5), (5, 1)),
        recurrent_block_padding=((0, 2), (2, 0)),
        # Flow head
        flow_head_hidden_size=256,
        # Mask predictor
        use_mask_predictor=True,
        **kwargs,
    )


@register_model()
@handle_legacy_interface(weights=("pretrained", Raft_Small_Weights.C_T_V2))
def raft_small(*, weights: Optional[Raft_Small_Weights] = None, progress=True, **kwargs) -> RAFT:
    """RAFT "small" model from
    `RAFT: Recurrent All Pairs Field Transforms for Optical Flow <https://arxiv.org/abs/2003.12039>`__.

    Please see the example below for a tutorial on how to use this model.

    Args:
        weights(:class:`~torchvision.models.optical_flow.Raft_Small_Weights`, optional): The
            pretrained weights to use. See
            :class:`~torchvision.models.optical_flow.Raft_Small_Weights`
            below for more details, and possible values. By default, no
            pre-trained weights are used.
        progress (bool): If True, displays a progress bar of the download to stderr. Default is True.
        **kwargs: parameters passed to the ``torchvision.models.optical_flow.RAFT``
            base class. Please refer to the `source code
            <https://github.com/pytorch/vision/blob/main/torchvision/models/optical_flow/raft.py>`_
            for more details about this class.

    .. autoclass:: torchvision.models.optical_flow.Raft_Small_Weights
        :members:
    """
    weights = Raft_Small_Weights.verify(weights)

    return _raft(
        weights=weights,
        progress=progress,
        # Feature encoder
        feature_encoder_layers=(32, 32, 64, 96, 128),
        feature_encoder_block=BottleneckBlock,
        feature_encoder_norm_layer=InstanceNorm2d,
        # Context encoder
        context_encoder_layers=(32, 32, 64, 96, 160),
        context_encoder_block=BottleneckBlock,
        context_encoder_norm_layer=None,
        # Correlation block
        corr_block_num_levels=4,
        corr_block_radius=3,
        # Motion encoder
        motion_encoder_corr_layers=(96,),
        motion_encoder_flow_layers=(64, 32),
        motion_encoder_out_channels=82,
        # Recurrent block
        recurrent_block_hidden_state_size=96,
        recurrent_block_kernel_size=(3,),
        recurrent_block_padding=(1,),
        # Flow head
        flow_head_hidden_size=128,
        # Mask predictor
        use_mask_predictor=False,
        **kwargs,
    )
