import sys
import warnings

import torch
from torch.onnx import symbolic_opset11 as opset11
from torch.onnx.symbolic_helper import parse_args

_ONNX_OPSET_VERSION_11 = 11
_ONNX_OPSET_VERSION_16 = 16
BASE_ONNX_OPSET_VERSION = _ONNX_OPSET_VERSION_11


@parse_args("v", "v", "f")
def symbolic_multi_label_nms(g, boxes, scores, iou_threshold):
    boxes = opset11.unsqueeze(g, boxes, 0)
    scores = opset11.unsqueeze(g, opset11.unsqueeze(g, scores, 0), 0)
    max_output_per_class = g.op("Constant", value_t=torch.tensor([sys.maxsize], dtype=torch.long))
    iou_threshold = g.op("Constant", value_t=torch.tensor([iou_threshold], dtype=torch.float))

    # Cast boxes and scores to float32 in case they are float64 inputs
    nms_out = g.op(
        "NonMaxSuppression",
        g.op("Cast", boxes, to_i=torch.onnx.TensorProtoDataType.FLOAT),
        g.op("Cast", scores, to_i=torch.onnx.TensorProtoDataType.FLOAT),
        max_output_per_class,
        iou_threshold,
    )
    return opset11.squeeze(
        g, opset11.select(g, nms_out, 1, g.op("Constant", value_t=torch.tensor([2], dtype=torch.long))), 1
    )


def _process_batch_indices_for_roi_align(g, rois):
    indices = opset11.squeeze(
        g, opset11.select(g, rois, 1, g.op("Constant", value_t=torch.tensor([0], dtype=torch.long))), 1
    )
    return g.op("Cast", indices, to_i=torch.onnx.TensorProtoDataType.INT64)


def _process_rois_for_roi_align(g, rois):
    return opset11.select(g, rois, 1, g.op("Constant", value_t=torch.tensor([1, 2, 3, 4], dtype=torch.long)))


def _process_sampling_ratio_for_roi_align(g, sampling_ratio: int):
    if sampling_ratio < 0:
        warnings.warn(
            "ONNX export for RoIAlign with a non-zero sampling_ratio is not supported. "
            "The model will be exported with a sampling_ratio of 0."
        )
        sampling_ratio = 0
    return sampling_ratio


@parse_args("v", "v", "f", "i", "i", "i", "i")
def roi_align_opset11(g, input, rois, spatial_scale, pooled_height, pooled_width, sampling_ratio, aligned):
    batch_indices = _process_batch_indices_for_roi_align(g, rois)
    rois = _process_rois_for_roi_align(g, rois)
    if aligned:
        warnings.warn(
            "ROIAlign with aligned=True is only supported in opset >= 16. "
            "Please export with opset 16 or higher, or use aligned=False."
        )
    sampling_ratio = _process_sampling_ratio_for_roi_align(g, sampling_ratio)
    return g.op(
        "RoiAlign",
        input,
        rois,
        batch_indices,
        spatial_scale_f=spatial_scale,
        output_height_i=pooled_height,
        output_width_i=pooled_width,
        sampling_ratio_i=sampling_ratio,
    )


@parse_args("v", "v", "f", "i", "i", "i", "i")
def roi_align_opset16(g, input, rois, spatial_scale, pooled_height, pooled_width, sampling_ratio, aligned):
    batch_indices = _process_batch_indices_for_roi_align(g, rois)
    rois = _process_rois_for_roi_align(g, rois)
    coordinate_transformation_mode = "half_pixel" if aligned else "output_half_pixel"
    sampling_ratio = _process_sampling_ratio_for_roi_align(g, sampling_ratio)
    return g.op(
        "RoiAlign",
        input,
        rois,
        batch_indices,
        coordinate_transformation_mode_s=coordinate_transformation_mode,
        spatial_scale_f=spatial_scale,
        output_height_i=pooled_height,
        output_width_i=pooled_width,
        sampling_ratio_i=sampling_ratio,
    )


@parse_args("v", "v", "f", "i", "i")
def roi_pool(g, input, rois, spatial_scale, pooled_height, pooled_width):
    roi_pool = g.op(
        "MaxRoiPool", input, rois, pooled_shape_i=(pooled_height, pooled_width), spatial_scale_f=spatial_scale
    )
    return roi_pool, None


def _register_custom_op():
    torch.onnx.register_custom_op_symbolic("torchvision::nms", symbolic_multi_label_nms, _ONNX_OPSET_VERSION_11)
    torch.onnx.register_custom_op_symbolic("torchvision::roi_align", roi_align_opset11, _ONNX_OPSET_VERSION_11)
    torch.onnx.register_custom_op_symbolic("torchvision::roi_align", roi_align_opset16, _ONNX_OPSET_VERSION_16)
    torch.onnx.register_custom_op_symbolic("torchvision::roi_pool", roi_pool, _ONNX_OPSET_VERSION_11)
