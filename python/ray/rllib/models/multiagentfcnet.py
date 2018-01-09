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
        # input_shapes = options["custom_options"]["observation_space"]
        # output_shapes = options["custom_options"]["action_space"]
        # input_reshaper = Reshaper(input_shapes)
        # output_reshaper = Reshaper(output_shapes)
        # split_inputs = input_reshaper.split_tensor(inputs)
        # # FIXME(ev) assumes all actions have the same shape
        # split_outputs = output_reshaper.split_agents(num_outputs)
        # convert the input spaces to shapes that we can use to divide the shapes

        hiddens = options.get("fcnet_hiddens", [[256, 256]]*1)
        custom_options = options["custom_options"]
        shared_model = custom_options.get("shared_model", 0)
        num_agents = len(hiddens)
        # FIXME(this won't work if everything isn't perfectly symmetric)
        num_actions = int(num_outputs/num_agents)
        # FIXME(this won't work if everything isn't perfectly symmetric)
        split_inputs = tf.split(inputs, num_agents, axis=-1)
        outputs = []
        for k in range(len(hiddens)):
            sub_options = options.copy()
            sub_options.update({"fcnet_hiddens": hiddens[k]})
            if not shared_model:
                sub_options["user_data"] = {"fcnet_tag": k}
            else:
                sub_options["user_data"] = {"shared_model": shared_model}
            fcnet = FullyConnectedNetwork(
                split_inputs[k], num_actions, sub_options)
            output, last_layer = fcnet.outputs, fcnet.last_layer
            outputs.append(output)
        overall_output = tf.concat(outputs, axis=1)
        # TODO(cathywu) check that outputs is not used later on because it's
        # a list instead of a layer
        return overall_output, outputs

