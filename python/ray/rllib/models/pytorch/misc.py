""" Code adapted from https://github.com/ikostrikov/pytorch-a3c"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import torch


def var_to_np(var):
    return var.cpu().detach().numpy()


def normc_initializer(std=1.0):
    def initializer(tensor):
        tensor.data.normal_(0, 1)
        tensor.data *= std / torch.sqrt(
            tensor.data.pow(2).sum(1, keepdim=True))

    return initializer


def valid_padding(in_size, filter_size, stride_size):
    """Note: Padding is added to match TF conv2d `same` padding. See
    www.tensorflow.org/versions/r0.12/api_docs/python/nn/convolution

    Params:
        in_size (tuple): Rows (Height), Column (Width) for input
        stride_size (tuple): Rows (Height), Column (Width) for stride
        filter_size (tuple): Rows (Height), Column (Width) for filter

    Output:
        padding (tuple): For input into torch.nn.ZeroPad2d
        output (tuple): Output shape after padding and convolution
    """
    in_height, in_width = in_size
    filter_height, filter_width = filter_size
    stride_height, stride_width = stride_size

    out_height = np.ceil(float(in_height) / float(stride_height))
    out_width = np.ceil(float(in_width) / float(stride_width))

    pad_along_height = int(
        ((out_height - 1) * stride_height + filter_height - in_height))
    pad_along_width = int(
        ((out_width - 1) * stride_width + filter_width - in_width))
    pad_top = pad_along_height // 2
    pad_bottom = pad_along_height - pad_top
    pad_left = pad_along_width // 2
    pad_right = pad_along_width - pad_left
    padding = (pad_left, pad_right, pad_top, pad_bottom)
    output = (out_height, out_width)
    return padding, output
