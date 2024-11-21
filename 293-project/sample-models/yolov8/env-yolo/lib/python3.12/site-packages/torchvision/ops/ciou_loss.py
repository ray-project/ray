import torch

from ..utils import _log_api_usage_once
from ._utils import _upcast_non_float
from .diou_loss import _diou_iou_loss


def complete_box_iou_loss(
    boxes1: torch.Tensor,
    boxes2: torch.Tensor,
    reduction: str = "none",
    eps: float = 1e-7,
) -> torch.Tensor:

    """
    Gradient-friendly IoU loss with an additional penalty that is non-zero when the
    boxes do not overlap. This loss function considers important geometrical
    factors such as overlap area, normalized central point distance and aspect ratio.
    This loss is symmetric, so the boxes1 and boxes2 arguments are interchangeable.

    Both sets of boxes are expected to be in ``(x1, y1, x2, y2)`` format with
    ``0 <= x1 < x2`` and ``0 <= y1 < y2``, and The two boxes should have the
    same dimensions.

    Args:
        boxes1 : (Tensor[N, 4] or Tensor[4]) first set of boxes
        boxes2 : (Tensor[N, 4] or Tensor[4]) second set of boxes
        reduction : (string, optional) Specifies the reduction to apply to the output:
            ``'none'`` | ``'mean'`` | ``'sum'``. ``'none'``: No reduction will be
            applied to the output. ``'mean'``: The output will be averaged.
            ``'sum'``: The output will be summed. Default: ``'none'``
        eps : (float): small number to prevent division by zero. Default: 1e-7

    Returns:
        Tensor: Loss tensor with the reduction option applied.

    Reference:
        Zhaohui Zheng et al.: Complete Intersection over Union Loss:
        https://arxiv.org/abs/1911.08287

    """

    # Original Implementation from https://github.com/facebookresearch/detectron2/blob/main/detectron2/layers/losses.py

    if not torch.jit.is_scripting() and not torch.jit.is_tracing():
        _log_api_usage_once(complete_box_iou_loss)

    boxes1 = _upcast_non_float(boxes1)
    boxes2 = _upcast_non_float(boxes2)

    diou_loss, iou = _diou_iou_loss(boxes1, boxes2)

    x1, y1, x2, y2 = boxes1.unbind(dim=-1)
    x1g, y1g, x2g, y2g = boxes2.unbind(dim=-1)

    # width and height of boxes
    w_pred = x2 - x1
    h_pred = y2 - y1
    w_gt = x2g - x1g
    h_gt = y2g - y1g
    v = (4 / (torch.pi**2)) * torch.pow((torch.atan(w_gt / h_gt) - torch.atan(w_pred / h_pred)), 2)
    with torch.no_grad():
        alpha = v / (1 - iou + v + eps)

    loss = diou_loss + alpha * v

    # Check reduction option and return loss accordingly
    if reduction == "none":
        pass
    elif reduction == "mean":
        loss = loss.mean() if loss.numel() > 0 else 0.0 * loss.sum()
    elif reduction == "sum":
        loss = loss.sum()
    else:
        raise ValueError(
            f"Invalid Value for arg 'reduction': '{reduction} \n Supported reduction modes: 'none', 'mean', 'sum'"
        )
    return loss
