from gym.spaces import Box, Dict, Discrete, MultiDiscrete, Tuple
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2, restore_original_dimensions
from ray.rllib.models.tf.misc import normc_initializer
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.utils import get_filter_config
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.spaces.space_utils import flatten_space
from ray.rllib.utils.tf_utils import one_hot

tf1, tf, tfv = try_import_tf()


# __sphinx_doc_begin__
class ComplexInputNetwork(TFModelV2):
    """TFModelV2 concat'ing CNN outputs to flat input(s), followed by FC(s).

    Note: This model should be used for complex (Dict or Tuple) observation
    spaces that have one or more image components.

    The data flow is as follows:

    `obs` (e.g. Tuple[img0, img1, discrete0]) -> `CNN0 + CNN1 + ONE-HOT`
    `CNN0 + CNN1 + ONE-HOT` -> concat all flat outputs -> `out`
    `out` -> (optional) FC-stack -> `out2`
    `out2` -> action (logits) and vaulue heads.
    """

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        self.original_space = obs_space.original_space if \
            hasattr(obs_space, "original_space") else obs_space
        assert isinstance(self.original_space, (Dict, Tuple)), \
            "`obs_space.original_space` must be [Dict|Tuple]!"

        self.processed_obs_space = self.original_space if \
            model_config.get("_disable_preprocessor_api") else obs_space
        super().__init__(self.original_space, action_space, num_outputs,
                         model_config, name)

        self.flattened_input_space = flatten_space(self.original_space)

        # Build the CNN(s) given obs_space's image components.
        self.cnns = {}
        self.one_hot = {}
        self.flatten = {}
        concat_size = 0
        for i, component in enumerate(self.flattened_input_space):
            # Image space.
            if len(component.shape) == 3:
                config = {
                    "conv_filters": model_config["conv_filters"]
                    if "conv_filters" in model_config else
                    get_filter_config(obs_space.shape),
                    "conv_activation": model_config.get("conv_activation"),
                    "post_fcnet_hiddens": [],
                }
                cnn = ModelCatalog.get_model_v2(
                    component,
                    action_space,
                    num_outputs=None,
                    model_config=config,
                    framework="tf",
                    name="cnn_{}".format(i))
                concat_size += cnn.num_outputs
                self.cnns[i] = cnn
            # Discrete|MultiDiscrete inputs -> One-hot encode.
            elif isinstance(component, Discrete):
                self.one_hot[i] = True
                concat_size += component.n
            elif isinstance(component, MultiDiscrete):
                self.one_hot[i] = True
                concat_size += sum(component.nvec)
            # Everything else (1D Box).
            else:
                self.flatten[i] = int(np.product(component.shape))
                concat_size += self.flatten[i]

        # Optional post-concat FC-stack.
        post_fc_stack_config = {
            "fcnet_hiddens": model_config.get("post_fcnet_hiddens", []),
            "fcnet_activation": model_config.get("post_fcnet_activation",
                                                 "relu")
        }
        self.post_fc_stack = ModelCatalog.get_model_v2(
            Box(float("-inf"),
                float("inf"),
                shape=(concat_size, ),
                dtype=np.float32),
            self.action_space,
            None,
            post_fc_stack_config,
            framework="tf",
            name="post_fc_stack")

        # Actions and value heads.
        self.logits_and_value_model = None
        self._value_out = None
        if num_outputs:
            # Action-distribution head.
            concat_layer = tf.keras.layers.Input(
                (self.post_fc_stack.num_outputs, ))
            logits_layer = tf.keras.layers.Dense(
                num_outputs,
                activation=tf.keras.activations.linear,
                name="logits")(concat_layer)

            # Create the value branch model.
            value_layer = tf.keras.layers.Dense(
                1,
                name="value_out",
                activation=None,
                kernel_initializer=normc_initializer(0.01))(concat_layer)
            self.logits_and_value_model = tf.keras.models.Model(
                concat_layer, [logits_layer, value_layer])
        else:
            self.num_outputs = self.post_fc_stack.num_outputs

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        if SampleBatch.OBS in input_dict and "obs_flat" in input_dict:
            orig_obs = input_dict[SampleBatch.OBS]
        else:
            orig_obs = restore_original_dimensions(
                input_dict[SampleBatch.OBS],
                self.processed_obs_space,
                tensorlib="tf")
        # Push image observations through our CNNs.
        outs = []
        for i, component in enumerate(tree.flatten(orig_obs)):
            if i in self.cnns:
                cnn_out, _ = self.cnns[i]({SampleBatch.OBS: component})
                outs.append(cnn_out)
            elif i in self.one_hot:
                if "int" in component.dtype.name:
                    outs.append(
                        one_hot(component, self.flattened_input_space[i]))
                else:
                    outs.append(component)
            else:
                outs.append(
                    tf.cast(
                        tf.reshape(component, [-1, self.flatten[i]]),
                        dtype=tf.float32,
                    ))
        # Concat all outputs and the non-image inputs.
        out = tf.concat(outs, axis=1)
        # Push through (optional) FC-stack (this may be an empty stack).
        out, _ = self.post_fc_stack({SampleBatch.OBS: out}, [], None)

        # No logits/value branches.
        if not self.logits_and_value_model:
            return out, []

        # Logits- and value branches.
        logits, values = self.logits_and_value_model(out)
        self._value_out = tf.reshape(values, [-1])
        return logits, []

    @override(ModelV2)
    def value_function(self):
        return self._value_out


# __sphinx_doc_end__
