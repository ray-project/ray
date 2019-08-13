from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

import tensorflow as tf
from tensorflow import keras

from ray.experimental.sgd.tests import mnist


def model_creator(config):
    return mnist.get_model()

def optimizer_creator(model, config):
    mnist.get_optimizer()
    return mnist.get_optimizer()

def data_creator(config, batch_size):
    return mnist.get_dataset(batch_size)

