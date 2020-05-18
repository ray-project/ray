from ray.rllib.models.model import Model
from ray.rllib.models.tf.misc import normc_initializer
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.framework import get_activation_fn, try_import_tf

tf = try_import_tf()


# Deprecated: see as an alternative models/tf.fcnet.py
class FullyConnectedNetwork(Model):
    """Generic fully connected network."""

    @override(Model)
    def _build_layers(self, inputs, num_outputs, options):
        """Process the flattened inputs.

        Note that dict inputs will be flattened into a vector. To define a
        model that processes the components separately, use _build_layers_v2().
        """
        # Soft deprecate this class. All Models should use the ModelV2
        # API from here on.
        deprecation_warning(
            "Model->FullyConnectedNetwork",
            "ModelV2->FullyConnectedNetwork",
            error=False)

        hiddens = options.get("fcnet_hiddens")
        activation = get_activation_fn(options.get("fcnet_activation"))

        if len(inputs.shape) > 2:
            inputs = tf.layers.flatten(inputs)

        with tf.name_scope("fc_net"):
            i = 1
            last_layer = inputs
            for size in hiddens:
                # skip final linear layer
                if options.get("no_final_linear") and i == len(hiddens):
                    output = tf.layers.dense(
                        last_layer,
                        num_outputs,
                        kernel_initializer=normc_initializer(1.0),
                        activation=activation,
                        name="fc_out")
                    return output, output

                label = "fc{}".format(i)
                last_layer = tf.layers.dense(
                    last_layer,
                    size,
                    kernel_initializer=normc_initializer(1.0),
                    activation=activation,
                    name=label)
                i += 1

            output = tf.layers.dense(
                last_layer,
                num_outputs,
                kernel_initializer=normc_initializer(0.01),
                activation=None,
                name="fc_out")
            return output, last_layer
