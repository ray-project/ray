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
    def _build_layers_v2(self, input_dict: dict, num_outputs: int, config: dict):
        import tensorflow.contrib.slim as slim

        with tf.name_scope("fc_net"):
            last_layer = input_dict['obs']
            activation = get_activation_fn(config.get("fcnet_activation"))
            for i, size in enumerate(config.get("fcnet_hiddens"), 1):
                last_layer = slim.fully_connected(
                    inputs=last_layer,
                    num_outputs=size,
                    weights_initializer=normc_initializer(1.0),
                    activation_fn=activation,
                    scope="fc{}".format(i),
                )
                last_layer = tf.layers.dropout(
                    inputs=last_layer,
                    rate=config.get("fcnet_dropout_rate"),
                    training=input_dict['is_training'],
                    name="dropout{}".format(i),
                )
            output = slim.fully_connected(
                inputs=last_layer,
                num_outputs=num_outputs,
                weights_initializer=normc_initializer(0.01),
                activation_fn=None,
                scope="fc_out",
            )
            return output, last_layer
