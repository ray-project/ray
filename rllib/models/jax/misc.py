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
        prng_key (Optional[jax.random.PRNGKey]): An optional PRNG key to
            use for initialization. If None, create a new random one.
    """

    in_size: int
    out_size: int
    initializer: Optional[Union[Callable, str]] = None
    activation: Optional[Union[Callable, str]] = None
    use_bias: bool = True
    prng_key: Optional[jax.random.PRNGKey] = None

    def setup(self):
        # By default, use Glorot unform initializer.
        if self.initializer is None:
            self.initializer = "xavier_uniform"

        if self.prng_key is None:
            self.prng_key = jax.random.PRNGKey(int(time.time()))
        #_, self.prng_key = jax.random.split(self.prng_key)

        # Activation function (if any; default=None (linear)).
        self.initializer_fn = get_initializer(self.initializer, framework="jax")
        self.activation_fn = get_activation_fn(self.activation, framework="jax")

        # Create the flax dense layer.
        self._dense = nn.Dense(
            self.out_size,
            use_bias=self.use_bias,
            kernel_init=self.initializer_fn,
        )
        # Initialize it.
        in_ = jnp.zeros((self.in_size, ))
        #_, self.prng_key = jax.random.split(self.prng_key)
        self._params = self._dense.init(self.prng_key, in_)

    def __call__(self, x):
        out = self._dense.apply(self._params, x)
        if self.activation_fn:
            out = self.activation_fn(out)
        return out
