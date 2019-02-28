from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf

from tfbench import model_config
from ray.experimental.sgd.model import Model
import ray.experimental.tf_utils as ray_tf_utils


class MockDataset():
    name = "synthetic"


class TFBenchModel(Model):
    def __init__(self, batch=64, use_cpus=False):

        image_shape = [batch, 224, 224, 3]
        labels_shape = [batch]

        # Synthetic image should be within [0, 255].
        images = tf.truncated_normal(
            image_shape,
            dtype=tf.float32,
            mean=127,
            stddev=60,
            name='synthetic_images')

        # Minor hack to avoid H2D copy when using synthetic data
        inputs = tf.contrib.framework.local_variable(
            images, name='gpu_cached_images')
        labels = tf.random_uniform(
            labels_shape,
            minval=0,
            maxval=999,
            dtype=tf.int32,
            name='synthetic_labels')

        model = model_config.get_model_config("resnet101", MockDataset())
        logits, aux = model.build_network(
            inputs, data_format=use_cpus and "NHWC" or "NCHW")
        loss = tf.nn.sparse_softmax_cross_entropy_with_logits(
            logits=logits, labels=labels)

        # Implement model interface
        self.loss = tf.reduce_mean(loss, name='xentropy-loss')
        self.optimizer = tf.train.GradientDescentOptimizer(1e-6)

        self.variables = ray_tf_utils.TensorFlowVariables(
            self.loss, tf.get_default_session())

    def get_loss(self):
        return self.loss

    def get_optimizer(self):
        return self.optimizer

    def get_feed_dict(self):
        return {}

    def get_weights(self):
        return self.variables.get_flat()

    def set_weights(self, weights):
        self.variables.set_flat(weights)
