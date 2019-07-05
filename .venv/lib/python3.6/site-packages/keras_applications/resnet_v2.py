"""ResNetV2 models for Keras.

# Reference paper

- [Aggregated Residual Transformations for Deep Neural Networks]
  (https://arxiv.org/abs/1611.05431) (CVPR 2017)

# Reference implementations

- [TensorNets]
  (https://github.com/taehoonlee/tensornets/blob/master/tensornets/resnets.py)
- [Torch ResNetV2]
  (https://github.com/facebook/fb.resnet.torch/blob/master/models/preresnet.lua)

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from . import imagenet_utils
from .imagenet_utils import decode_predictions
from .resnet_common import ResNet50V2
from .resnet_common import ResNet101V2
from .resnet_common import ResNet152V2


def preprocess_input(x, **kwargs):
    """Preprocesses a numpy array encoding a batch of images.

    # Arguments
        x: a 4D numpy array consists of RGB values within [0, 255].
        data_format: data format of the image tensor.

    # Returns
        Preprocessed array.
    """
    return imagenet_utils.preprocess_input(x, mode='tf', **kwargs)
