from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf
import tensorflow.contrib.slim as slim
from tensorflow.contrib.layers import xavier_initializer

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.misc import normc_initializer
from ray.rllib.models.model import Model


class CarlaModel(Model):
    """Carla model that can process the observation tuple.

    The architecture processes the image using convolutional layers, the
    metrics using fully connected layers, and then combines them with
    further fully connected layers.
    """

    # TODO(ekl): use build_layers_v2 for native dict space support
    def _build_layers(self, inputs, num_outputs, options):
        # Parse options
        image_shape = options["custom_options"]["image_shape"]
        convs = options.get("conv_filters", [
            [16, [8, 8], 4],
            [32, [5, 5], 3],
            [32, [5, 5], 2],
            [512, [10, 10], 1],
        ])
        hiddens = options.get("fcnet_hiddens", [64])
        fcnet_activation = options.get("fcnet_activation", "tanh")
        if fcnet_activation == "tanh":
            activation = tf.nn.tanh
        elif fcnet_activation == "relu":
            activation = tf.nn.relu

        # Sanity checks
        image_size = np.product(image_shape)
        expected_shape = [image_size + 5 + 2]
        assert inputs.shape.as_list()[1:] == expected_shape, \
            (inputs.shape.as_list()[1:], expected_shape)

        # Reshape the input vector back into its components
        vision_in = tf.reshape(inputs[:, :image_size],
                               [tf.shape(inputs)[0]] + image_shape)
        metrics_in = inputs[:, image_size:]
        print("Vision in shape", vision_in)
        print("Metrics in shape", metrics_in)

        # Setup vision layers
        with tf.name_scope("carla_vision"):
            for i, (out_size, kernel, stride) in enumerate(convs[:-1], 1):
                vision_in = slim.conv2d(
                    vision_in,
                    out_size,
                    kernel,
                    stride,
                    scope="conv{}".format(i))
            out_size, kernel, stride = convs[-1]
            vision_in = slim.conv2d(
                vision_in,
                out_size,
                kernel,
                stride,
                padding="VALID",
                scope="conv_out")
            vision_in = tf.squeeze(vision_in, [1, 2])

        # Setup metrics layer
        with tf.name_scope("carla_metrics"):
            metrics_in = slim.fully_connected(
                metrics_in,
                64,
                weights_initializer=xavier_initializer(),
                activation_fn=activation,
                scope="metrics_out")

        print("Shape of vision out is", vision_in.shape)
        print("Shape of metric out is", metrics_in.shape)

        # Combine the metrics and vision inputs
        with tf.name_scope("carla_out"):
            i = 1
            last_layer = tf.concat([vision_in, metrics_in], axis=1)
            print("Shape of concatenated out is", last_layer.shape)
            for size in hiddens:
                last_layer = slim.fully_connected(
                    last_layer,
                    size,
                    weights_initializer=xavier_initializer(),
                    activation_fn=activation,
                    scope="fc{}".format(i))
                i += 1
            output = slim.fully_connected(
                last_layer,
                num_outputs,
                weights_initializer=normc_initializer(0.01),
                activation_fn=None,
                scope="fc_out")

        return output, last_layer


def register_carla_model():
    ModelCatalog.register_custom_model("carla", CarlaModel)
