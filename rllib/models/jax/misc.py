import time
from typing import Callable, Optional

from ray.rllib.models.utils import get_activation_fn
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.framework import try_import_jax
from ray.util import log_once

jax, flax = try_import_jax()
nn = np = None
if flax:
    import flax.linen as nn
    import jax.numpy as np


class SlimFC:
    """Simple JAX version of a fully connected layer."""

    def __init__(
        self,
        in_size,
        out_size,
        initializer: Optional[Callable] = None,
        activation_fn: Optional[str] = None,
        use_bias: bool = True,
        prng_key: Optional["jax.random.PRNGKey"] = None,
        name: Optional[str] = None,
    ):
        """Initializes a SlimFC instance.

        Args:
            in_size: The input size of the input data that will be passed
                into this layer.
            out_size: The number of nodes in this FC layer.
            initializer (flax.:
            activation_fn: An activation string specifier, e.g. "relu".
            use_bias: Whether to add biases to the dot product or not.
            #bias_init (float):
            prng_key (Optional[jax.random.PRNGKey]): An optional PRNG key to
                use for initialization. If None, create a new random one.
            name (Optional[str]): An optional name for this layer.
        """
        if log_once("jax_slim_fc_deprecation_warning"):
            deprecation_warning(old=("ray.rllib.models.jax.misc.SlimFC"))

        # By default, use Glorot uniform initializer.
        if initializer is None:
            initializer = nn.initializers.xavier_uniform()

        self.prng_key = prng_key or jax.random.PRNGKey(int(time.time()))
        _, self.prng_key = jax.random.split(self.prng_key)
        # Create the flax dense layer.
        self._dense = nn.Dense(
            out_size,
            use_bias=use_bias,
            kernel_init=initializer,
            name=name,
        )
        # Initialize it.
        dummy_in = jax.random.normal(self.prng_key, (in_size,), dtype=np.float32)
        _, self.prng_key = jax.random.split(self.prng_key)
        self._params = self._dense.init(self.prng_key, dummy_in)

        # Activation function (if any; default=None (linear)).
        self.activation_fn = get_activation_fn(activation_fn, "jax")

    def __call__(self, x):
        out = self._dense.apply(self._params, x)
        if self.activation_fn:
            out = self.activation_fn(out)
        return out
