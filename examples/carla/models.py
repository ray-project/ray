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

from env import COMMANDS_ENUM


class CarlaModel(Model):
    """Carla model that can process the observation tuple.

    The architecture processes the image using convolutional layers, the
    metrics using fully connected layers, and then combines them with
    further fully connected layers.
    """

    def _init(self, inputs, num_outputs, options):
        # Parse options
        image_shape = options["custom_options"]["image_shape"]
        command_mode = options["custom_options"]["command_mode"]
        assert command_mode in ["concat", "switched"]
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
        vision_in = tf.reshape(
            inputs[:, :image_size], [tf.shape(inputs)[0]] + image_shape)
        num_commands = len(COMMANDS_ENUM)
        print("Vision in shape", vision_in)
        if command_mode == "concat":
            metrics_in = inputs[:, image_size:]
            print("Metrics and command in shape", metrics_in)
        else:
            command_in = inputs[:, image_size:image_size + num_commands]
            metrics_in = inputs[:, image_size + num_commands:]
            print("Command in shape", command_in)
            print("Metrics in shape", metrics_in)

        # Setup vision layers
        with tf.name_scope("carla_vision"):
            for i, (out_size, kernel, stride) in enumerate(convs[:-1], 1):
                vision_in = slim.conv2d(
                    vision_in, out_size, kernel, stride,
                    scope="conv{}".format(i))
            out_size, kernel, stride = convs[-1]
            vision_in = slim.conv2d(
                vision_in, out_size, kernel, stride,
                padding="VALID", scope="conv_out")
            vision_in = tf.squeeze(vision_in, [1, 2])

        # Setup metrics layer
        with tf.name_scope("carla_metrics"):
            metrics_in = slim.fully_connected(
                metrics_in, 64,
                weights_initializer=xavier_initializer(),
                activation_fn=activation,
                scope="metrics_out")

        print("Shape of vision out is", vision_in.shape)
        print("Shape of metric out is", metrics_in.shape)

        def build_out(in_tensor, scope=""):
            # Combine the metrics and vision inputs
            with tf.name_scope("carla_out{}".format(scope)):
                i = 1
                last_layer = in_tensor
                print("Shape of concatenated out is", last_layer.shape)
                for size in hiddens:
                    last_layer = slim.fully_connected(
                        last_layer, size,
                        weights_initializer=xavier_initializer(),
                        activation_fn=activation,
                        scope="{}fc{}".format(scope, i))
                    i += 1
                output = slim.fully_connected(
                    last_layer, num_outputs,
                    weights_initializer=normc_initializer(0.01),
                    activation_fn=None, scope="{}fc_out".format(scope))

            return output, last_layer

        in_tensor = tf.concat([vision_in, metrics_in], axis=1)

        if command_mode == "concat":
            output, last_layer = build_out(in_tensor)
        else:
            print("Building command-switched output networks")
            outs = [
                build_out(in_tensor, "cmd_{}_".format(i))
                for i in range(5)]
            output = switch_commands(command_in, [o[0] for o in outs])
            last_layer = switch_commands(command_in, [o[1] for o in outs])

        print("Output action", output)
        print("Last layer", last_layer)
        return output, last_layer


def switch_commands(one_hot_vector, choices):
    return tf.where(
        one_hot_vector[:, 0] == 1,
        choices[0],
        tf.where(
            one_hot_vector[:, 1] == 1,
            choices[1],
            tf.where(
                one_hot_vector[:, 2] == 1,
                choices[2],
                tf.where(
                    one_hot_vector[:, 3] == 1,
                    choices[3],
                    choices[4]))))


def register_carla_model():
    ModelCatalog.register_custom_model("carla", CarlaModel)
