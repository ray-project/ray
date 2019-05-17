from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.model import Model
from ray.rllib.models.misc import normc_initializer, get_activation_fn
from ray.rllib.utils.annotations import override
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class FullyConnectedNetwork(Model):
    """Generic fully connected network."""

    @override(Model)
    def _build_layers(self, inputs, num_outputs, options):
        """Process the flattened inputs.

        Note that dict inputs will be flattened into a vector. To define a
        model that processes the components separately, use _build_layers_v2().
        """

        hiddens = options.get("fcnet_hiddens")
        activation = get_activation_fn(options.get("fcnet_activation"))

        with tf.name_scope("fc_net"):
            i = 1
            last_layer = inputs
            for size in hiddens:
                label = "fc{}".format(i)
                last_layer = tf.layers.dense(
                    last_layer,
                    size,
                    kernel_initializer=normc_initializer(1.0),
                    activation=activation,
                    name=label)
                i += 1
            label = "fc_out"
            output = tf.layers.dense(
                last_layer,
                num_outputs,
                kernel_initializer=normc_initializer(0.01),
                activation=None,
                name=label)
            return output, last_layer
