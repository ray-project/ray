from gym.spaces import Box, Discrete, MultiDiscrete
import numpy as np
import tree  # pip install dm_tree
from typing import Tuple

# TODO (sven): add IMPALA-style option.
# from ray.rllib.examples.models.impala_vision_nets import TorchImpalaVisionNet
from ray.rllib.models.torch.misc import normc_initializer as \
    torch_normc_initializer, SlimFC
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2, restore_original_dimensions
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.utils import get_filter_config
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.spaces.space_utils import flatten_space
from ray.rllib.utils.torch_utils import one_hot as one_hot_torch

torch, nn = try_import_torch()


class ComplexInputNetwork(TorchModelV2, nn.Module):
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

        nn.Module.__init__(self)
        TorchModelV2.__init__(self, self.original_space, action_space,
                              num_outputs, model_config, name)

        self.flattened_input_space = flatten_space(self.original_space)

        # Atari type CNNs or IMPALA type CNNs (with residual layers)?
        # self.cnn_type = self.model_config["custom_model_config"].get(
        #     "conv_type", "atari")
        self.one_hot, self.cnns, self.flatten, \
            concat_size, self.post_fc_stack = self._build_net()

        # Actions and value heads.
        self.logits_layer = None
        self.value_layer = None
        self._value_out = None

        # Add action-distribution head and value output.
        if num_outputs:
            self.logits_layer = SlimFC(
                in_size=self.post_fc_stack.num_outputs,
                out_size=num_outputs,
                activation_fn=None,
                initializer=torch_normc_initializer(0.01))

            # Create the separate value branch model, if necessary.
            if not self.model_config["vf_share_layers"]:
                self.one_hot_value, self.cnns_value, self.flatten_value, \
                    _, self.post_fc_stack_value = \
                    self._build_net()

            # Value head (1 output node).
            self.value_layer = SlimFC(
                in_size=self.post_fc_stack.num_outputs,
                out_size=1,
                activation_fn=None,
                initializer=torch_normc_initializer(0.01))

        # No output layers -> Store last layer size in self.num_outputs.
        else:
            self.num_outputs = concat_size

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        if SampleBatch.OBS in input_dict and "obs_flat" in input_dict:
            self.orig_obs = input_dict[SampleBatch.OBS]
        else:
            self.orig_obs = restore_original_dimensions(
                input_dict[SampleBatch.OBS],
                self.processed_obs_space,
                tensorlib="torch")

        out = self._forward(self.one_hot, self.cnns, self.flatten,
                            self.post_fc_stack)

        # No logits/value branches, just return plain feature vector.
        if self.logits_layer is None:
            return out, []
        # Shared logits- and value branches.
        elif self.model_config["vf_share_layers"]:
            values = self.value_layer(out)
            self._value_out = torch.reshape(values, [-1])

        logits = self.logits_layer(out)

        return logits, []

    @override(ModelV2)
    def value_function(self):
        if not self.model_config["vf_share_layers"]:
            out = self._forward(self.one_hot_value, self.cnns_value,
                                self.flatten_value, self.post_fc_stack_value)
            values = self.value_layer(out)
            self._value_out = torch.reshape(values, [-1])

        return self._value_out

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
                # if self.cnn_type == "atari":
                cnns[i] = ModelCatalog.get_model_v2(
                    component,
                    self.action_space,
                    num_outputs=None,
                    model_config=config,
                    framework="torch",
                    name="cnn_{}".format(i))
                # TODO (sven): add IMPALA-style option.
                # else:
                #    cnn = TorchImpalaVisionNet(
                #        component,
                #        action_space,
                #        num_outputs=None,
                #        model_config=config,
                #        name="cnn_{}".format(i))

                concat_size += cnns[i].num_outputs
                self.add_module("cnn_{}".format(i), self.cnns[i])
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
                    framework="torch",
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
                    framework="torch",
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
            framework="torch",
            name="post_fc_stack")

        return one_hot, cnns, flatten, concat_size, post_fc_stack

    def _forward(self, one_hot, cnns, flatten, post_fc_stack):
        """Runs a forward pass through a given stack.

        Stack is defined by one-hot, CNN, and flatten components, as well as
        the post fully-connected (FC) stack.

        Args:
            one_hot:
            cnns:
            flatten:
            post_fc_stack:

        Returns:

        """
        # Push observations through the different components
        # (CNNs, one-hot + FC, etc..).
        outs = []
        for i, component in enumerate(tree.flatten(self.orig_obs)):
            if i in cnns:
                cnn_out, _ = cnns[i](SampleBatch({SampleBatch.OBS: component}))
                outs.append(cnn_out)
            elif i in one_hot:
                if component.dtype in [torch.int32, torch.int64, torch.uint8]:
                    one_hot_in = {
                        SampleBatch.OBS: one_hot_torch(
                            component, self.flattened_input_space[i])
                    }
                else:
                    one_hot_in = {SampleBatch.OBS: component}
                one_hot_out, _ = one_hot[i](SampleBatch(one_hot_in))
                outs.append(one_hot_out)
            else:
                input_dim = component.obs_space.shape[0]
                nn_out, _ = flatten[i](SampleBatch({
                    SampleBatch.OBS: torch.reshape(component, [-1, input_dim])
                }))
                outs.append(nn_out)

        # Concat all outputs and the non-image inputs.
        out = torch.cat(outs, dim=1)
        # Push through (optional) FC-stack (this may be an empty stack).
        out, _ = post_fc_stack(SampleBatch({SampleBatch.OBS: out}))

        return out
