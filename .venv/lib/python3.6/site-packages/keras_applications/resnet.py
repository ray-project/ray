"""ResNet models for Keras.

# Reference paper

- [Deep Residual Learning for Image Recognition]
  (https://arxiv.org/abs/1512.03385) (CVPR 2016 Best Paper Award)

# Reference implementations

- [TensorNets]
  (https://github.com/taehoonlee/tensornets/blob/master/tensornets/resnets.py)
- [Caffe ResNet]
  (https://github.com/KaimingHe/deep-residual-networks/tree/master/prototxt)

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from . import imagenet_utils
from .imagenet_utils import decode_predictions
from .resnet_common import ResNet50
from .resnet_common import ResNet101
from .resnet_common import ResNet152


def preprocess_input(x, **kwargs):
    """Preprocesses a numpy array encoding a batch of images.

    # Arguments
        x: a 4D numpy array consists of RGB values within [0, 255].
        data_format: data format of the image tensor.

    # Returns
        Preprocessed array.
    """
    return imagenet_utils.preprocess_input(x, mode='caffe', **kwargs)
