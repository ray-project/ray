import logging
import numpy as np
import time
from typing import Callable, Optional, Union

from ray.rllib.models.jax.misc import SlimFC
from ray.rllib.models.utils import get_activation_fn, get_initializer
from ray.rllib.utils.framework import try_import_jax

jax, flax = try_import_jax()
nn = None
if flax:
    nn = flax.linen

logger = logging.getLogger(__name__)


class FCStack(nn.Module if nn else object):
    """Generic fully connected FLAX module.

    Properties:
        in_features (int): Number of input features (input dim).
        layers (List[int]): List of Dense layer sizes.
        activation (Optional[Union[Callable, str]]): An optional activation
            function or activation function specifier (str), such as
            "relu". Use None or "linear" for no activation.
        initializer ():
    """

    in_features: int
    layers: []
    activation: Optional[Union[Callable, str]] = None
    initializer: Optional[Union[Callable, str]] = None
    use_bias: bool = True
    prng_key: Optional[jax.random.PRNGKey] = jax.random.PRNGKey(int(time.time()))

    def setup(self):
        self.initializer = get_initializer(self.initializer, framework="jax")

        # Create all layers.
        hidden_layers = []
        prev_layer_size = self.in_features
        for i, size in enumerate(self.layers):
            #setattr(self, "fc_{}".format(i), slim_fc)
            hidden_layers.append(SlimFC(
                in_size=prev_layer_size,
                out_size=size,
                use_bias=self.use_bias,
                initializer=self.initializer,
                activation=self.activation,
            ))
            prev_layer_size = size
        self.hidden_layers = hidden_layers

    def __call__(self, inputs):
        x = inputs
        for layer in self.hidden_layers:
            x = layer(x)
        return x
