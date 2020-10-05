import numpy as np

from ray.rllib.utils.framework import get_activation_fn, try_import_jax

jax = try_import_jax()


def normc_initializer(std=1.0):
    def initializer(tensor):
        tensor.data.normal_(0, 1)
        tensor.data *= std / torch.sqrt(
            tensor.data.pow(2).sum(1, keepdim=True))

    return initializer


class SlimFC:
    """Simple JAX version of `linear` function"""

    def __init__(self,
                 in_size,
                 out_size,
                 initializer=None,
                 activation_fn=None,
                 use_bias=True,
                 bias_init=0.0,
                 key=None):
        weights, biases = jax.random.normal(), jax.random.normal
        # Actual Conv2D layer (including correct initialization logic).
        linear = nn.Linear(in_size, out_size, bias=use_bias)
        if initializer:
            initializer(linear.weight)
        if use_bias is True:
            nn.init.constant_(linear.bias, bias_init)
        layers.append(linear)
        # Activation function (if any; default=None (linear)).
        if isinstance(activation_fn, str):
            activation_fn = get_activation_fn(activation_fn, "torch")
        if activation_fn is not None:
            layers.append(activation_fn())
        # Put everything in sequence.
        self._model = nn.Sequential(*layers)

    def forward(self, x):
        return self._model(x)


class AppendBiasLayer(nn.Module):
    """Simple bias appending layer for free_log_std."""

    def __init__(self, num_bias_vars):
        super().__init__()
        self.log_std = torch.nn.Parameter(
            torch.as_tensor([0.0] * num_bias_vars))
        self.register_parameter("log_std", self.log_std)

    def forward(self, x):
        out = torch.cat(
            [x, self.log_std.unsqueeze(0).repeat([len(x), 1])], axis=1)
        return out
