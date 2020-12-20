import time
from typing import Callable, Optional, Union

from ray.rllib.models.utils import get_activation_fn, get_initializer
from ray.rllib.utils.framework import try_import_jax

jax, flax = try_import_jax()
nn = jnp = None
if flax:
    import flax.linen as nn
    import jax.numpy as jnp


class SlimFC(nn.Module if nn else object):
    """Simple JAX version of a fully connected layer.

    Properties:
        in_size (int): The input size of the input data that will be passed
            into this layer.
        out_size (int): The number of nodes in this FC layer.
        initializer (flax.:
        activation (str): An activation string specifier, e.g. "relu".
        use_bias (bool): Whether to add biases to the dot product or not.
        #bias_init (float):
    """

    in_size: int
    out_size: int
    initializer: Optional[Union[Callable, str]] = None
    activation: Optional[Union[Callable, str]] = None
    use_bias: bool = True

    def setup(self):
        # By default, use Glorot unform initializer.
        if self.initializer is None:
            self.initializer = "xavier_uniform"

        # Activation function (if any; default=None (linear)).
        self.initializer_fn = get_initializer(self.initializer, framework="jax")
        self.activation_fn = get_activation_fn(self.activation, framework="jax")

        # Create the flax dense layer.
        self.dense = nn.Dense(
            self.out_size,
            use_bias=self.use_bias,
            kernel_init=self.initializer_fn,
        )

    def __call__(self, x):
        out = self.dense(x)
        if self.activation_fn:
            out = self.activation_fn(out)
        return out
