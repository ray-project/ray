import logging
import numpy as np

from ray.rllib.models.jax.jax_modelv2 import JAXModelV2
from ray.rllib.models.jax.modules.fc_stack import FCStack
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_jax

jax, flax = try_import_jax()
jnp = None
if jax:
    import jax.numpy as jnp

logger = logging.getLogger(__name__)


class FullyConnectedNetwork(JAXModelV2):
    """Generic fully connected network."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super().__init__(obs_space, action_space, num_outputs, model_config,
                         name)

        activation = model_config.get("fcnet_activation")
        hiddens = model_config.get("fcnet_hiddens", [])
        no_final_linear = model_config.get("no_final_linear")
        in_features = int(np.product(obs_space.shape))
        self.vf_share_layers = model_config.get("vf_share_layers")
        self.free_log_std = model_config.get("free_log_std")

        if self.free_log_std:
            raise ValueError("`free_log_std` not supported for JAX yet!")

        self._logits = None
        self._logits_params = None
        self._hidden_layers = None

        # The last layer is adjusted to be of size num_outputs, but it's a
        # layer with activation.
        if no_final_linear and num_outputs:
            self._hidden_layers = FCStack(
                in_features=in_features,
                layers=hiddens + [num_outputs],
                activation=activation,
                prng_key=self.prng_key,
            )

        # Finish the layers with the provided sizes (`hiddens`), plus -
        # iff num_outputs > 0 - a last linear layer of size num_outputs.
        else:
            prev_layer_size = in_features
            if len(hiddens) > 0:
                self._hidden_layers = FCStack(
                    in_features=prev_layer_size,
                    layers=hiddens,
                    activation=activation,
                    prng_key=self.prng_key,
                )
                prev_layer_size = hiddens[-1]
            if num_outputs:
                self._logits = FCStack(
                    in_features=prev_layer_size,
                    layers=[num_outputs],
                    activation=None,
                    prng_key=self.prng_key,
                )
                self._logits_params = self._logits.init(
                    self.prng_key, jnp.zeros((1, prev_layer_size)))
            else:
                self.num_outputs = (
                    [int(np.product(obs_space.shape))] + hiddens[-1:])[-1]

        # Init hidden layers.
        self._hidden_layers_params = None
        if self._hidden_layers:
            in_ = jnp.zeros((1, in_features))
            self._hidden_layers_params = self._hidden_layers.init(
                self.prng_key, in_)

        self._value_branch_separate = None
        if not self.vf_share_layers:
            # Build a parallel set of hidden layers for the value net.
            self._value_branch_separate = FCStack(
                in_features=in_features,
                layers=hiddens,
                activation=activation,
                prng_key=self.prng_key,
            )
            in_ = jnp.zeros((1, in_features))
            self._value_branch_separate_params = \
                self._value_branch_separate.init(self.prng_key, in_)

        self._value_branch = FCStack(
            in_features=prev_layer_size,
            layers=[1],
            prng_key=self.prng_key,
        )
        in_ = jnp.zeros((1, prev_layer_size))
        self._value_branch_params = self._value_branch.init(self.prng_key, in_)
        # Holds the current "base" output (before logits layer).
        self._features = None
        # Holds the last input, in case value branch is separate.
        self._last_flat_in = None

    @override(JAXModelV2)
    def forward(self, input_dict, state, seq_lens):
        #self.jit_forward = jax.jit(lambda i, s, sl: self.forward_(i, s, sl))

        if not hasattr(self, "forward_"):
            def forward_(flat_in):
                self._last_flat_in = self._features = flat_in#input_dict["obs_flat"]#flat_in
                if self._hidden_layers:
                    self._features = self._hidden_layers.apply(
                        self._hidden_layers_params, self._last_flat_in)
                logits = self._logits.apply(self._logits_params, self._features) if \
                    self._logits else self._features

                if self._value_branch_separate:
                    x = self._value_branch_separate.apply(
                        self._value_branch_separate_params, self._last_flat_in)
                    value_out = self._value_branch.apply(self._value_branch_params,
                                                    x).squeeze(1)
                else:
                    value_out = self._value_branch.apply(self._value_branch_params,
                                                         self._features).squeeze(1)

                return logits, value_out, state

            self.forward_ = forward_
            self.jit_forward = jax.jit(forward_)

        return self.jit_forward(input_dict["obs_flat"])

    #@override(JAXModelV2)
    #def value_function(self):
    #    assert self._features is not None, "must call forward() first"

    #    if not hasattr(self, "value_function_"):
    #        def value_function_():
    #            if self._value_branch_separate:
    #                x = self._value_branch_separate.apply(
    #                    self._value_branch_separate_params, self._last_flat_in)
    #                return self._value_branch.apply(self._value_branch_params,
    #                                                x).squeeze(1)
    #            else:
    #                return self._value_branch.apply(self._value_branch_params,
    #                                                self._features).squeeze(1)

    #        self.value_function_ = jax.jit(value_function_)

    #    return self.value_function_()
