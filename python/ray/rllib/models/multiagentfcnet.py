from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf

from ray.rllib.models.model import Model
from ray.rllib.models.fcnet import FullyConnectedNetwork
from ray.rllib.utils.reshaper import Reshaper


class MultiAgentFullyConnectedNetwork(Model):
    """Multiagent fully connected network."""

    def _build_layers(self, inputs, num_outputs, options):
        # Split the input and output tensors
        input_shapes = options["custom_options"]["multiagent_obs_shapes"]
        output_shapes = options["custom_options"]["multiagent_act_shapes"]
        input_reshaper = Reshaper(input_shapes)
        output_reshaper = Reshaper(output_shapes)
        split_inputs = input_reshaper.split_tensor(inputs)
        num_actions = output_reshaper.split_number(num_outputs)

        custom_options = options["custom_options"]
        hiddens = custom_options.get("multiagent_fcnet_hiddens",
                                     [[256, 256]] * 1)

        # check for a shared model
        shared_model = custom_options.get("multiagent_shared_model", 0)
        reuse = tf.AUTO_REUSE if shared_model else False
        outputs = []
        for i in range(len(hiddens)):
            scope = "multi" if shared_model else "multi{}".format(i)
            with tf.variable_scope(scope, reuse=reuse):
                sub_options = options.copy()
                sub_options.update({"fcnet_hiddens": hiddens[i]})
                # TODO(ev) make this support arbitrary networks
                fcnet = FullyConnectedNetwork(split_inputs[i],
                                              int(num_actions[i]), sub_options)
                output = fcnet.outputs
                outputs.append(output)
        overall_output = tf.concat(outputs, axis=1)
        return overall_output, outputs
