from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import tensorflow.contrib.slim as slim
import numpy as np

from ray.rllib.models.model import Model
from ray.rllib.models.misc import normc_initializer
from ray.rllib.models.fcnet import FullyConnectedNetwork
from ray.rllib.models.action_dist import Reshaper


class MultiAgentFullyConnectedNetwork(Model):
    """Multiagent fully connected network."""

    def _init(self, inputs, num_outputs, options):

        input_shapes = options["custom_options"]["obs_shapes"]
        output_shapes = options["custom_options"]["act_shapes"]
        input_reshaper = Reshaper(input_shapes)
        output_reshaper = Reshaper(output_shapes)
        split_inputs = input_reshaper.split_tensor(inputs)
        num_actions = output_reshaper.split_number(num_outputs)
        # convert the input spaces to shapes that we can use to divide the shapes

        custom_options = options["custom_options"]
        hiddens = custom_options.get("multiagent_fcnet_hiddens", [[256, 256]]*1)

        # check for a shared model
        shared_model = custom_options.get("shared_model", 0)
        reuse = tf.AUTO_REUSE if shared_model else False
        outputs = []
        for i in range(len(hiddens)):
            with tf.variable_scope("multi{}".format(i), reuse=reuse):
                sub_options = options.copy()
                sub_options.update({"fcnet_hiddens": hiddens[i]})

                fcnet = FullyConnectedNetwork(
                    split_inputs[i], int(num_actions[i]), sub_options)
                output, last_layer = fcnet.outputs, fcnet.last_layer
                outputs.append(output)
        overall_output = tf.concat(outputs, axis=1)
        return overall_output, outputs

