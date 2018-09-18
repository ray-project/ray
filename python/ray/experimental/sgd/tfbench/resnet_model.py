# Copyright 2017 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""Resnet model configuration.

References:
  Kaiming He, Xiangyu Zhang, Shaoqing Ren, Jian Sun
  Deep Residual Learning for Image Recognition
  arXiv:1512.03385 (2015)

  Kaiming He, Xiangyu Zhang, Shaoqing Ren, Jian Sun
  Identity Mappings in Deep Residual Networks
  arXiv:1603.05027 (2016)

  Liang-Chieh Chen, George Papandreou, Iasonas Kokkinos, Kevin Murphy,
  Alan L. Yuille
  DeepLab: Semantic Image Segmentation with Deep Convolutional Nets,
  Atrous Convolution, and Fully Connected CRFs
  arXiv:1606.00915 (2016)
"""

import numpy as np
from six.moves import xrange  # pylint: disable=redefined-builtin
import tensorflow as tf
from . import model as model_lib


def bottleneck_block_v1(cnn, depth, depth_bottleneck, stride):
    """Bottleneck block with identity short-cut for ResNet v1.

  Args:
    cnn: the network to append bottleneck blocks.
    depth: the number of output filters for this bottleneck block.
    depth_bottleneck: the number of bottleneck filters for this block.
    stride: Stride used in the first layer of the bottleneck block.
  """
    input_layer = cnn.top_layer
    in_size = cnn.top_size
    name_key = 'resnet_v1'
    name = name_key + str(cnn.counts[name_key])
    cnn.counts[name_key] += 1

    with tf.variable_scope(name):
        if depth == in_size:
            if stride == 1:
                shortcut = input_layer
            else:
                shortcut = cnn.apool(
                    1,
                    1,
                    stride,
                    stride,
                    input_layer=input_layer,
                    num_channels_in=in_size)
        else:
            shortcut = cnn.conv(
                depth,
                1,
                1,
                stride,
                stride,
                activation=None,
                use_batch_norm=True,
                input_layer=input_layer,
                num_channels_in=in_size,
                bias=None)
        cnn.conv(
            depth_bottleneck,
            1,
            1,
            stride,
            stride,
            input_layer=input_layer,
            num_channels_in=in_size,
            use_batch_norm=True,
            bias=None)
        cnn.conv(
            depth_bottleneck,
            3,
            3,
            1,
            1,
            mode='SAME_RESNET',
            use_batch_norm=True,
            bias=None)
        res = cnn.conv(
            depth, 1, 1, 1, 1, activation=None, use_batch_norm=True, bias=None)
        output = tf.nn.relu(shortcut + res)
        cnn.top_layer = output
        cnn.top_size = depth


def bottleneck_block_v2(cnn, depth, depth_bottleneck, stride):
    """Bottleneck block with identity short-cut for ResNet v2.

  The main difference from v1 is that a batch norm and relu are done at the
  start of the block, instead of the end. This initial batch norm and relu is
  collectively called a pre-activation.

  Args:
    cnn: the network to append bottleneck blocks.
    depth: the number of output filters for this bottleneck block.
    depth_bottleneck: the number of bottleneck filters for this block.
    stride: Stride used in the first layer of the bottleneck block.
  """
    input_layer = cnn.top_layer
    in_size = cnn.top_size
    name_key = 'resnet_v2'
    name = name_key + str(cnn.counts[name_key])
    cnn.counts[name_key] += 1

    preact = cnn.batch_norm()
    preact = tf.nn.relu(preact)
    with tf.variable_scope(name):
        if depth == in_size:
            if stride == 1:
                shortcut = input_layer
            else:
                shortcut = cnn.apool(
                    1,
                    1,
                    stride,
                    stride,
                    input_layer=input_layer,
                    num_channels_in=in_size)
        else:
            shortcut = cnn.conv(
                depth,
                1,
                1,
                stride,
                stride,
                activation=None,
                use_batch_norm=False,
                input_layer=preact,
                num_channels_in=in_size,
                bias=None)
        cnn.conv(
            depth_bottleneck,
            1,
            1,
            stride,
            stride,
            input_layer=preact,
            num_channels_in=in_size,
            use_batch_norm=True,
            bias=None)
        cnn.conv(
            depth_bottleneck,
            3,
            3,
            1,
            1,
            mode='SAME_RESNET',
            use_batch_norm=True,
            bias=None)
        res = cnn.conv(
            depth,
            1,
            1,
            1,
            1,
            activation=None,
            use_batch_norm=False,
            bias=None)
        output = shortcut + res
        cnn.top_layer = output
        cnn.top_size = depth


def bottleneck_block(cnn, depth, depth_bottleneck, stride, pre_activation):
    """Bottleneck block with identity short-cut.

  Args:
    cnn: the network to append bottleneck blocks.
    depth: the number of output filters for this bottleneck block.
    depth_bottleneck: the number of bottleneck filters for this block.
    stride: Stride used in the first layer of the bottleneck block.
    pre_activation: use pre_activation structure used in v2 or not.
  """
    if pre_activation:
        bottleneck_block_v2(cnn, depth, depth_bottleneck, stride)
    else:
        bottleneck_block_v1(cnn, depth, depth_bottleneck, stride)


def residual_block(cnn, depth, stride, pre_activation):
    """Residual block with identity short-cut.

  Args:
    cnn: the network to append residual blocks.
    depth: the number of output filters for this residual block.
    stride: Stride used in the first layer of the residual block.
    pre_activation: use pre_activation structure or not.
  """
    input_layer = cnn.top_layer
    in_size = cnn.top_size
    if in_size != depth:
        # Plan A of shortcut.
        shortcut = cnn.apool(
            1,
            1,
            stride,
            stride,
            input_layer=input_layer,
            num_channels_in=in_size)
        padding = (depth - in_size) // 2
        if cnn.channel_pos == 'channels_last':
            shortcut = tf.pad(shortcut,
                              [[0, 0], [0, 0], [0, 0], [padding, padding]])
        else:
            shortcut = tf.pad(shortcut,
                              [[0, 0], [padding, padding], [0, 0], [0, 0]])
    else:
        shortcut = input_layer
    if pre_activation:
        res = cnn.batch_norm(input_layer)
        res = tf.nn.relu(res)
    else:
        res = input_layer
    cnn.conv(
        depth,
        3,
        3,
        stride,
        stride,
        input_layer=res,
        num_channels_in=in_size,
        use_batch_norm=True,
        bias=None)
    if pre_activation:
        res = cnn.conv(
            depth,
            3,
            3,
            1,
            1,
            activation=None,
            use_batch_norm=False,
            bias=None)
        output = shortcut + res
    else:
        res = cnn.conv(
            depth, 3, 3, 1, 1, activation=None, use_batch_norm=True, bias=None)
        output = tf.nn.relu(shortcut + res)
    cnn.top_layer = output
    cnn.top_size = depth


class ResnetModel(model_lib.Model):
    """Resnet cnn network configuration."""

    def __init__(self, model, layer_counts):
        default_batch_sizes = {
            'resnet50': 64,
            'resnet101': 32,
            'resnet152': 32,
            'resnet50_v2': 64,
            'resnet101_v2': 32,
            'resnet152_v2': 32,
        }
        batch_size = default_batch_sizes.get(model, 32)
        super(ResnetModel, self).__init__(model, 224, batch_size, 0.005,
                                          layer_counts)
        self.pre_activation = 'v2' in model

    def add_inference(self, cnn):
        if self.layer_counts is None:
            raise ValueError(
                'Layer counts not specified for %s' % self.get_model())
        cnn.use_batch_norm = True
        cnn.batch_norm_config = {
            'decay': 0.997,
            'epsilon': 1e-5,
            'scale': True
        }
        cnn.conv(64, 7, 7, 2, 2, mode='SAME_RESNET', use_batch_norm=True)
        cnn.mpool(3, 3, 2, 2, mode='SAME')
        for _ in xrange(self.layer_counts[0]):
            bottleneck_block(cnn, 256, 64, 1, self.pre_activation)
        for i in xrange(self.layer_counts[1]):
            stride = 2 if i == 0 else 1
            bottleneck_block(cnn, 512, 128, stride, self.pre_activation)
        for i in xrange(self.layer_counts[2]):
            stride = 2 if i == 0 else 1
            bottleneck_block(cnn, 1024, 256, stride, self.pre_activation)
        for i in xrange(self.layer_counts[3]):
            stride = 2 if i == 0 else 1
            bottleneck_block(cnn, 2048, 512, stride, self.pre_activation)
        if self.pre_activation:
            cnn.batch_norm()
            cnn.top_layer = tf.nn.relu(cnn.top_layer)
        cnn.spatial_mean()

    def get_learning_rate(self, global_step, batch_size):
        raise NotImplementedError


def create_resnet50_model():
    return ResnetModel('resnet50', (3, 4, 6, 3))


def create_resnet50_v2_model():
    return ResnetModel('resnet50_v2', (3, 4, 6, 3))


def create_resnet101_model():
    return ResnetModel('resnet101', (3, 4, 23, 3))


def create_resnet101_v2_model():
    return ResnetModel('resnet101_v2', (3, 4, 23, 3))


def create_resnet152_model():
    return ResnetModel('resnet152', (3, 8, 36, 3))


def create_resnet152_v2_model():
    return ResnetModel('resnet152_v2', (3, 8, 36, 3))


class ResnetCifar10Model(model_lib.Model):
    """Resnet cnn network configuration for Cifar 10 dataset.

  V1 model architecture follows the one defined in the paper:
  https://arxiv.org/pdf/1512.03385.pdf.

  V2 model architecture follows the one defined in the paper:
  https://arxiv.org/pdf/1603.05027.pdf.
  """

    def __init__(self, model, layer_counts):
        self.pre_activation = 'v2' in model
        super(ResnetCifar10Model, self).__init__(model, 32, 128, 0.1,
                                                 layer_counts)

    def add_inference(self, cnn):
        if self.layer_counts is None:
            raise ValueError(
                'Layer counts not specified for %s' % self.get_model())

        cnn.use_batch_norm = True
        cnn.batch_norm_config = {'decay': 0.9, 'epsilon': 1e-5, 'scale': True}
        if self.pre_activation:
            cnn.conv(16, 3, 3, 1, 1, use_batch_norm=True)
        else:
            cnn.conv(16, 3, 3, 1, 1, activation=None, use_batch_norm=True)
        for i in xrange(self.layer_counts[0]):
            # reshape to batch_size x 16 x 32 x 32
            residual_block(cnn, 16, 1, self.pre_activation)
        for i in xrange(self.layer_counts[1]):
            stride = 2 if i == 0 else 1
            # reshape to batch_size x 32 x 16 x 16
            residual_block(cnn, 32, stride, self.pre_activation)
        for i in xrange(self.layer_counts[2]):
            stride = 2 if i == 0 else 1
            # reshape to batch_size x 64 x 8 x 8
            residual_block(cnn, 64, stride, self.pre_activation)
        if self.pre_activation:
            cnn.batch_norm()
            cnn.top_layer = tf.nn.relu(cnn.top_layer)
        cnn.spatial_mean()

    def get_learning_rate(self, global_step, batch_size):
        num_batches_per_epoch = int(50000 / batch_size)
        boundaries = num_batches_per_epoch * np.array(
            [82, 123, 300], dtype=np.int64)
        boundaries = [x for x in boundaries]
        values = [0.1, 0.01, 0.001, 0.0002]
        return tf.train.piecewise_constant(global_step, boundaries, values)


def create_resnet20_cifar_model():
    return ResnetCifar10Model('resnet20', (3, 3, 3))


def create_resnet20_v2_cifar_model():
    return ResnetCifar10Model('resnet20_v2', (3, 3, 3))


def create_resnet32_cifar_model():
    return ResnetCifar10Model('resnet32_v2', (5, 5, 5))


def create_resnet32_v2_cifar_model():
    return ResnetCifar10Model('resnet32_v2', (5, 5, 5))


def create_resnet44_cifar_model():
    return ResnetCifar10Model('resnet44', (7, 7, 7))


def create_resnet44_v2_cifar_model():
    return ResnetCifar10Model('resnet44_v2', (7, 7, 7))


def create_resnet56_cifar_model():
    return ResnetCifar10Model('resnet56', (9, 9, 9))


def create_resnet56_v2_cifar_model():
    return ResnetCifar10Model('resnet56_v2', (9, 9, 9))


def create_resnet110_cifar_model():
    return ResnetCifar10Model('resnet110', (18, 18, 18))


def create_resnet110_v2_cifar_model():
    return ResnetCifar10Model('resnet110_v2', (18, 18, 18))
