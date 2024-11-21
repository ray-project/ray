import warnings

import numpy as np
import torch


def l_prod(in_list):
    """Compute the product of all elements in the input list."""
    res = 1
    for _ in in_list:
        res *= _
    return res


def l_sum(in_list):
    """Calculate the sum of all numerical elements in a list."""
    return sum(in_list)


def calculate_parameters(param_list):
    """Calculate the total number of parameters in a list of tensors using the product of their shapes."""
    return sum(torch.DoubleTensor([p.nelement()]) for p in param_list)


def calculate_zero_ops():
    """Initializes and returns a tensor with all elements set to zero."""
    return torch.DoubleTensor([0])


def calculate_conv2d_flops(input_size: list, output_size: list, kernel_size: list, groups: int, bias: bool = False):
    """Calculate FLOPs for a Conv2D layer using input/output sizes, kernel size, groups, and the bias flag."""
    # n, in_c, ih, iw = input_size
    # out_c, in_c, kh, kw = kernel_size
    in_c = input_size[1]
    g = groups
    return l_prod(output_size) * (in_c // g) * l_prod(kernel_size[2:])


def calculate_conv(bias, kernel_size, output_size, in_channel, group):
    """Calculate FLOPs for convolutional layers given bias, kernel size, output size, in_channels, and groups."""
    warnings.warn("This API is being deprecated.")
    return torch.DoubleTensor([output_size * (in_channel / group * kernel_size + bias)])


def calculate_norm(input_size):
    """Compute the L2 norm of a tensor or array based on its input size."""
    return torch.DoubleTensor([2 * input_size])


def calculate_relu_flops(input_size):
    """Calculates the FLOPs for a ReLU activation function based on the input tensor's dimensions."""
    return 0


def calculate_relu(input_size: torch.Tensor):
    """Convert an input tensor to a DoubleTensor with the same value (deprecated)."""
    warnings.warn("This API is being deprecated")
    return torch.DoubleTensor([int(input_size)])


def calculate_softmax(batch_size, nfeatures):
    """Compute FLOPs for a softmax activation given batch size and feature count."""
    total_exp = nfeatures
    total_add = nfeatures - 1
    total_div = nfeatures
    total_ops = batch_size * (total_exp + total_add + total_div)
    return torch.DoubleTensor([int(total_ops)])


def calculate_avgpool(input_size):
    """Calculate the average pooling size for a given input tensor."""
    return torch.DoubleTensor([int(input_size)])


def calculate_adaptive_avg(kernel_size, output_size):
    """Calculate FLOPs for adaptive average pooling given kernel size and output size."""
    total_div = 1
    kernel_op = kernel_size + total_div
    return torch.DoubleTensor([int(kernel_op * output_size)])


def calculate_upsample(mode: str, output_size):
    """Calculate the operations required for various upsample methods based on mode and output size."""
    total_ops = output_size
    if mode == "bicubic":
        total_ops *= 224 + 35
    elif mode == "bilinear":
        total_ops *= 11
    elif mode == "linear":
        total_ops *= 5
    elif mode == "trilinear":
        total_ops *= 13 * 2 + 5
    return torch.DoubleTensor([int(total_ops)])


def calculate_linear(in_feature, num_elements):
    """Calculate the linear operation count for given input feature and number of elements."""
    return torch.DoubleTensor([int(in_feature * num_elements)])


def counter_matmul(input_size, output_size):
    """Calculate the total number of operations for matrix multiplication given input and output sizes."""
    input_size = np.array(input_size)
    output_size = np.array(output_size)
    return np.prod(input_size) * output_size[-1]


def counter_mul(input_size):
    """Calculate the total number of operations for element-wise multiplication given the input size."""
    return input_size


def counter_pow(input_size):
    """Computes the total scalar multiplications required for power operations based on input size."""
    return input_size


def counter_sqrt(input_size):
    """Calculate the total number of scalar operations required for a square root operation given an input size."""
    return input_size


def counter_div(input_size):
    """Calculate the total number of scalar operations for a division operation given an input size."""
    return input_size
