import torch
import torch.fx
from torch import nn, Tensor
from torch.nn.modules.utils import _pair
from torchvision.extension import _assert_has_ops

from ..utils import _log_api_usage_once
from ._utils import check_roi_boxes_shape, convert_boxes_to_roi_format


@torch.fx.wrap
def ps_roi_align(
    input: Tensor,
    boxes: Tensor,
    output_size: int,
    spatial_scale: float = 1.0,
    sampling_ratio: int = -1,
) -> Tensor:
    """
    Performs Position-Sensitive Region of Interest (RoI) Align operator
    mentioned in Light-Head R-CNN.

    Args:
        input (Tensor[N, C, H, W]): The input tensor, i.e. a batch with ``N`` elements. Each element
            contains ``C`` feature maps of dimensions ``H x W``.
        boxes (Tensor[K, 5] or List[Tensor[L, 4]]): the box coordinates in (x1, y1, x2, y2)
            format where the regions will be taken from.
            The coordinate must satisfy ``0 <= x1 < x2`` and ``0 <= y1 < y2``.
            If a single Tensor is passed, then the first column should
            contain the index of the corresponding element in the batch, i.e. a number in ``[0, N - 1]``.
            If a list of Tensors is passed, then each Tensor will correspond to the boxes for an element i
            in the batch.
        output_size (int or Tuple[int, int]): the size of the output (in bins or pixels) after the pooling
            is performed, as (height, width).
        spatial_scale (float): a scaling factor that maps the box coordinates to
            the input coordinates. For example, if your boxes are defined on the scale
            of a 224x224 image and your input is a 112x112 feature map (resulting from a 0.5x scaling of
            the original image), you'll want to set this to 0.5. Default: 1.0
        sampling_ratio (int): number of sampling points in the interpolation grid
            used to compute the output value of each pooled output bin. If > 0,
            then exactly ``sampling_ratio x sampling_ratio`` sampling points per bin are used. If
            <= 0, then an adaptive number of grid points are used (computed as
            ``ceil(roi_width / output_width)``, and likewise for height). Default: -1

    Returns:
        Tensor[K, C / (output_size[0] * output_size[1]), output_size[0], output_size[1]]: The pooled RoIs
    """
    if not torch.jit.is_scripting() and not torch.jit.is_tracing():
        _log_api_usage_once(ps_roi_align)
    _assert_has_ops()
    check_roi_boxes_shape(boxes)
    rois = boxes
    output_size = _pair(output_size)
    if not isinstance(rois, torch.Tensor):
        rois = convert_boxes_to_roi_format(rois)
    output, _ = torch.ops.torchvision.ps_roi_align(
        input, rois, spatial_scale, output_size[0], output_size[1], sampling_ratio
    )
    return output


class PSRoIAlign(nn.Module):
    """
    See :func:`ps_roi_align`.
    """

    def __init__(
        self,
        output_size: int,
        spatial_scale: float,
        sampling_ratio: int,
    ):
        super().__init__()
        _log_api_usage_once(self)
        self.output_size = output_size
        self.spatial_scale = spatial_scale
        self.sampling_ratio = sampling_ratio

    def forward(self, input: Tensor, rois: Tensor) -> Tensor:
        return ps_roi_align(input, rois, self.output_size, self.spatial_scale, self.sampling_ratio)

    def __repr__(self) -> str:
        s = (
            f"{self.__class__.__name__}("
            f"output_size={self.output_size}"
            f", spatial_scale={self.spatial_scale}"
            f", sampling_ratio={self.sampling_ratio}"
            f")"
        )
        return s
