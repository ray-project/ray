""" Code adapted from https://github.com/ikostrikov/pytorch-a3c"""
import numpy as np

from ray.rllib.utils import try_import_torch

torch, nn = try_import_torch()


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


class SlimConv2d(nn.Module):
    """Simple mock of tf.slim Conv2d"""

    def __init__(
            self,
            in_channels,
            out_channels,
            kernel,
            stride,
            padding,
            # Defaulting these to nn.[..] will break soft torch import.
            initializer="default",
            activation_fn="default",
            bias_init=0):
        super(SlimConv2d, self).__init__()
        layers = []
        if padding:
            layers.append(nn.ZeroPad2d(padding))
        conv = nn.Conv2d(in_channels, out_channels, kernel, stride)
        if initializer:
            if initializer == "default":
                initializer = nn.init.xavier_uniform_
            initializer(conv.weight)
        nn.init.constant_(conv.bias, bias_init)

        layers.append(conv)
        if activation_fn:
            if activation_fn == "default":
                activation_fn = nn.ReLU
            layers.append(activation_fn())
        self._model = nn.Sequential(*layers)

    def forward(self, x):
        return self._model(x)


class SlimFC(nn.Module):
    """Simple PyTorch version of `linear` function"""

    def __init__(self,
                 in_size,
                 out_size,
                 initializer=None,
                 activation_fn=None,
                 bias_init=0.0):
        super(SlimFC, self).__init__()
        layers = []
        linear = nn.Linear(in_size, out_size)
        if initializer:
            initializer(linear.weight)
        nn.init.constant_(linear.bias, bias_init)
        layers.append(linear)
        if activation_fn:
            layers.append(activation_fn())
        self._model = nn.Sequential(*layers)

    def forward(self, x):
        return self._model(x)


class AppendBiasLayer(nn.Module):
    """Simple bias appending layer for free_log_std."""

    def __init__(self, num_bias_vars):
        super().__init__()
        self.log_std = torch.nn.Parameter(
            torch.as_tensor([0.0] * num_bias_vars))
        self.register_parameter("log_std", self.log_std)

    def forward(self, x):
        out = torch.cat(
            [x, self.log_std.unsqueeze(0).repeat([len(x), 1])], axis=1)
        return out
