from gym.spaces import Box, Discrete, MultiDiscrete
import numpy as np
import tree  # pip install dm_tree
from typing import Tuple

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2, restore_original_dimensions
from ray.rllib.models.tf.misc import normc_initializer
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.utils import get_filter_config
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.spaces.space_utils import flatten_space
from ray.rllib.utils.tf_utils import one_hot as one_hot_tf

tf1, tf, tfv = try_import_tf()


# __sphinx_doc_begin__
class ComplexInputNetwork(TFModelV2):
    """Model concat'ing outputs of different processors to one flat output.

    The individual components of the input space are treated as follows,
    depending on their type:

    Discrete and MultiDiscrete:
    1) One-hot, 2) FC-stack defined by `fcnet_hiddens` config.

    Image-like Boxes:
    1) CNN defined by `conv2d_filters` config.

    All other Boxes:
    1) Flatten, 2) FC-stack defined by `fcnet_hiddens` config.

    After this "preprocessing" of all individual components, the outputs
    are concatenated to one single (1D) layer and then pushed through
    a FC Stack defined by the `post_fcnet_hiddens` config.

    Examples:
        observation_space: Dict({"a": img0, "b": img1, "c": discrete0})
        `obs` -> `CNN0, CNN1, one-hot->FCStack`
        `CNN0, CNN1, one-hot->FCStack` -> concat all flat outputs -> `out`
        `out` -> (optional) "post" FC-stack -> `out2`
        `out2` -> action (logits) and vaulue heads.

        observation_space: Tuple([Box(2, 4), Discrete(2)])
        `obs` -> `Flatten->FCStack, ONE-HOT+FCStack`
        `Flatten->FCStack, ONE-HOT+FCStack` -> concat all flat outputs -> `out`
        `out` -> (optional) "post" FC-stack -> `out2`
        `out2` -> action (logits) and vaulue heads.
    """

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        self.original_space = obs_space.original_space if \
            hasattr(obs_space, "original_space") else obs_space

        self.processed_obs_space = self.original_space if \
            model_config.get("_disable_preprocessor_api") else obs_space
        super().__init__(self.original_space, action_space, num_outputs,
                         model_config, name)

        self.flattened_input_space = flatten_space(self.original_space)

        self.one_hot, self.cnns, self.flatten, \
            concat_size, self.post_fc_stack = self._build_net()

        # Actions and value heads.
        self.logits_and_value_model = None  # shared models
        self.logits_model = None  # separate logits model
        self.value_model = None  # separate value model
        self._value_out = None
        if num_outputs:
            # Action-distribution head.
            post_fc_out = tf.keras.layers.Input(
                (self.post_fc_stack.num_outputs, ))
            logits_layer = tf.keras.layers.Dense(
                num_outputs,
                activation=None,
                kernel_initializer=normc_initializer(0.01),
                name="logits")(post_fc_out)

            # Shared logits- and value branches.
            if self.model_config["vf_share_layers"]:
                # Create the value branch model.
                value_layer = tf.keras.layers.Dense(
                    1,
                    activation=None,
                    kernel_initializer=normc_initializer(0.01),
                    name="value_out")(post_fc_out)
                self.logits_and_value_model = tf.keras.models.Model(
                    post_fc_out, [logits_layer, value_layer])
            # Create the separate value branch.
            else:
                self.one_hot_value, self.cnns_value, self.flatten_value, \
                    _, self.post_fc_stack_value = \
                    self._build_net()
                self.logits_model = tf.keras.models.Model(
                    post_fc_out, logits_layer)

                post_fc_out_value = tf.keras.layers.Input(
                    (self.post_fc_stack_value.num_outputs, ))
                value_layer = tf.keras.layers.Dense(
                    1,
                    activation=None,
                    kernel_initializer=normc_initializer(0.01),
                    name="value_out")(post_fc_out_value)
                self.value_model = tf.keras.models.Model(
                    post_fc_out_value, value_layer)
        else:
            self.num_outputs = self.post_fc_stack.num_outputs

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        if SampleBatch.OBS in input_dict and "obs_flat" in input_dict:
            self.orig_obs = input_dict[SampleBatch.OBS]
        else:
            self.orig_obs = restore_original_dimensions(
                input_dict[SampleBatch.OBS],
                self.processed_obs_space,
                tensorlib="tf")

        out = self._forward(self.one_hot, self.cnns, self.flatten,
                            self.post_fc_stack)

        # No logits/value branches, just return plain feature vector.
        if not self.logits_and_value_model and not self.logits_model:
            return out, []
        # Shared logits- and value branches.
        elif self.model_config["vf_share_layers"]:
            logits, values = self.logits_and_value_model(out)
            self._value_out = tf.reshape(values, [-1])
        # Separate logits- and value branches.
        else:
            logits = self.logits_model(out)

        return logits, []

    @override(ModelV2)
    def value_function(self):
        if not self.model_config["vf_share_layers"]:
            out = self._forward(self.one_hot_value, self.cnns_value,
                                self.flatten_value, self.post_fc_stack_value)
            values = self.value_model(out)
            self._value_out = tf.reshape(values, [-1])

        return self._value_out

    # __sphinx_doc_end__

    def _build_net(self) -> Tuple[dict, dict, dict, int, ModelV2]:
        """Builds a new complex input stack (one-hot, CNNs, flatten layers).

        Returns:
            Tuple consisting of 1) dict mapping ints (component index) to
            ModelV2 FC stack handling the one-hot output, 2) dict mapping
            ints (component index) to ModelV2 CNN instances, 3) dict mapping
            ints (component index) to ModelV2 FC stack handling the flattening
            output, 4) size of the concat output, and
            5) the "post" FCNet ModelV2 instance (applied to concat output).
        """

        # Build the CNN(s), one-hot, flatten, and post-fc-stacks given
        # obs_space's components.
        cnns = {}
        one_hot = {}
        flatten = {}
        concat_size = 0
        for i, component in enumerate(self.flattened_input_space):
            # Image space.
            if len(component.shape) == 3:
                config = {
                    "conv_filters": self.model_config["conv_filters"]
                    if "conv_filters" in self.model_config else
                    get_filter_config(self.obs_space.shape),
                    "conv_activation": self.model_config.get(
                        "conv_activation"),
                    "post_fcnet_hiddens": [],
                    "vf_share_layers": True,
                }
                cnns[i] = ModelCatalog.get_model_v2(
                    component,
                    self.action_space,
                    num_outputs=None,
                    model_config=config,
                    framework="tf",
                    name="cnn_{}".format(i))
                concat_size += cnns[i].num_outputs
            # Discrete|MultiDiscrete inputs -> One-hot encode.
            elif isinstance(component, (Discrete, MultiDiscrete)):
                if isinstance(component, Discrete):
                    size = component.n
                else:
                    size = sum(component.nvec)
                config = {
                    "fcnet_hiddens": self.model_config["fcnet_hiddens"],
                    "fcnet_activation": self.model_config.get(
                        "fcnet_activation"),
                    "post_fcnet_hiddens": [],
                    "vf_share_layers": True,
                }
                one_hot[i] = ModelCatalog.get_model_v2(
                    Box(-1.0, 1.0, (size, ), np.float32),
                    self.action_space,
                    num_outputs=None,
                    model_config=config,
                    framework="tf",
                    name="one_hot_{}".format(i))
                concat_size += one_hot[i].num_outputs
            # Everything else (1D Box).
            else:
                size = int(np.product(component.shape))
                config = {
                    "fcnet_hiddens": self.model_config["fcnet_hiddens"],
                    "fcnet_activation": self.model_config.get(
                        "fcnet_activation"),
                    "post_fcnet_hiddens": [],
                    "vf_share_layers": True,
                }
                flatten[i] = ModelCatalog.get_model_v2(
                    Box(-1.0, 1.0, (size, ), np.float32),
                    self.action_space,
                    num_outputs=None,
                    model_config=config,
                    framework="tf",
                    name="flatten_{}".format(i))
                concat_size += flatten[i].num_outputs

        # Optional post-concat FC-stack.
        post_fc_stack_config = {
            "fcnet_hiddens": self.model_config.get("post_fcnet_hiddens", []),
            "fcnet_activation": self.model_config.get("post_fcnet_activation",
                                                      "relu"),
            "post_fcnet_hiddens": [],
            "vf_share_layers": True,
        }
        post_fc_stack = ModelCatalog.get_model_v2(
            Box(float("-inf"),
                float("inf"),
                shape=(concat_size, ),
                dtype=np.float32),
            self.action_space,
            None,
            post_fc_stack_config,
            framework="tf",
            name="post_fc_stack")

        return one_hot, cnns, flatten, concat_size, post_fc_stack

    def _forward(self, one_hot, cnns, flatten, post_fc_stack):
        # Push image observations through our CNNs.
        outs = []
        for i, component in enumerate(tree.flatten(self.orig_obs)):
            if i in cnns:
                cnn_out, _ = cnns[i](SampleBatch({SampleBatch.OBS: component}))
                outs.append(cnn_out)
            elif i in one_hot:
                if "int" in component.dtype.name:
                    one_hot_in = {
                        SampleBatch.OBS: one_hot_tf(
                            component, self.flattened_input_space[i])
                    }
                else:
                    one_hot_in = {SampleBatch.OBS: component}
                one_hot_out, _ = one_hot[i](SampleBatch(one_hot_in))
                outs.append(one_hot_out)
            else:
                input_dim = flatten[i].obs_space.shape[0]
                nn_out, _ = flatten[i](SampleBatch({
                    SampleBatch.OBS: tf.cast(
                        tf.reshape(component, [-1, input_dim]), tf.float32)
                }))
                outs.append(nn_out)

        # Concat all outputs and the non-image inputs.
        out = tf.concat(outs, axis=1)
        # Push through (optional) FC-stack (this may be an empty stack).
        out, _ = post_fc_stack(SampleBatch({SampleBatch.OBS: out}))

        return out
