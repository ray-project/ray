import logging
import numpy as np
import time

from ray.rllib.models.jax.misc import get_activation_fn, SlimFC
from ray.rllib.utils.framework import try_import_jax

jax, flax = try_import_jax()
nn = None
if flax:
    nn = flax.linen

logger = logging.getLogger(__name__)


class FCStack(nn.Module if nn else object):
    """Generic fully connected FLAX module."""

    def __init__(self, in_features, layers, activation=None, prng_key=None):
        """Initializes a FCStack instance.

        Args:
            in_features (int): Number of input features (input dim).
            layers (List[int]): List of Dense layer sizes.
            activation (Optional[Union[Callable, str]]): An optional activation
                function or activation function specifier (str), such as
                "relu". Use None or "linear" for no activation.
        """
        super().__init__()

        self.prng_key = prng_key or jax.random.PRNGKey(int(time.time()))
        activation_fn = get_activation_fn(activation, framework="jax")

        # Create all layers.
        self._layers = []
        prev_layer_size = in_features
        for size in layers:
            self._hidden_layers.append(
                SlimFC(
                    in_size=prev_layer_size,
                    out_size=size,
                    use_bias=self.use_bias,
                    initializer=self.initializer,
                    activation_fn=activation_fn,
                    prng_key=self.prng_key,
                ))
            prev_layer_size = size

    def __call__(self, inputs):
        x = inputs
        for layer in self._hidden_layers:
            x = layer(x)
        return x
