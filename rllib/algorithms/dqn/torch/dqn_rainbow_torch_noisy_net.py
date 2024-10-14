from typing import Callable, Dict, List, Optional, Union

from ray.rllib.algorithms.dqn.dqn_rainbow_noisy_net_configs import (
    NoisyMLPEncoderConfig,
    NoisyMLPHeadConfig,
)
from ray.rllib.algorithms.dqn.torch.torch_noisy_linear import NoisyLinear
from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import Encoder, ENCODER_OUT, Model
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.models.utils import get_activation_fn, get_initializer_fn
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class TorchNoisyMLPEncoder(TorchModel, Encoder):
    def __init__(self, config: NoisyMLPEncoderConfig) -> None:
        TorchModel.__init__(self, config)
        Encoder.__init__(self, config)

        # Create the noisy network.
        self.net = TorchNoisyMLP(
            input_dim=config.input_dims[0],
            hidden_layer_dims=config.hidden_layer_dims,
            hidden_layer_activation=config.hidden_layer_activation,
            hidden_layer_use_layernorm=config.hidden_layer_use_layernorm,
            hidden_layer_use_bias=config.hidden_layer_use_bias,
            hidden_layer_weights_initializer=config.hidden_layer_weights_initializer,
            hidden_layer_weights_initializer_config=(
                config.hidden_layer_weights_initializer_config
            ),
            hidden_layer_bias_initializer=config.hidden_layer_bias_initializer,
            hidden_layer_bias_initializer_config=(
                config.hidden_layer_bias_initializer_config
            ),
            output_dim=config.output_layer_dim,
            output_activation=config.output_layer_activation,
            output_use_bias=config.output_layer_use_bias,
            output_weights_initializer=config.output_layer_weights_initializer,
            output_weights_initializer_config=(
                config.output_layer_weights_initializer_config
            ),
            output_bias_initializer=config.output_layer_bias_initializer,
            output_bias_initializer_config=config.output_layer_bias_initializer_config,
            # Note, this is the only additional parameter in regard to a regular MLP.
            std_init=config.std_init,
        )

    @override(Model)
    def _forward(self, inputs: dict, **kwargs) -> dict:
        return {ENCODER_OUT: self.net(inputs[Columns.OBS])}

    def _reset_noise(self):
        # Reset the noise in the complete network.
        self.net._reset_noise()


class TorchNoisyMLPHead(TorchModel):
    def __init__(self, config: NoisyMLPHeadConfig) -> None:
        super().__init__(config)

        self.net = TorchNoisyMLP(
            input_dim=config.input_dims[0],
            hidden_layer_dims=config.hidden_layer_dims,
            hidden_layer_activation=config.hidden_layer_activation,
            hidden_layer_use_layernorm=config.hidden_layer_use_layernorm,
            hidden_layer_use_bias=config.hidden_layer_use_bias,
            hidden_layer_weights_initializer=config.hidden_layer_weights_initializer,
            hidden_layer_weights_initializer_config=(
                config.hidden_layer_weights_initializer_config
            ),
            hidden_layer_bias_initializer=config.hidden_layer_bias_initializer,
            hidden_layer_bias_initializer_config=(
                config.hidden_layer_bias_initializer_config
            ),
            output_dim=config.output_layer_dim,
            output_activation=config.output_layer_activation,
            output_use_bias=config.output_layer_use_bias,
            output_weights_initializer=config.output_layer_weights_initializer,
            output_weights_initializer_config=(
                config.output_layer_weights_initializer_config
            ),
            output_bias_initializer=config.output_layer_bias_initializer,
            output_bias_initializer_config=config.output_layer_bias_initializer_config,
            # Note, this is the only additional parameter in regard to a regular MLP.
            std_init=config.std_init,
        )

    @override(Model)
    def _forward(self, inputs: torch.Tensor, **kwargs) -> torch.Tensor:
        return self.net(inputs)

    def _reset_noise(self) -> None:
        # Reset the noise in the complete network.
        self.net._reset_noise()


class TorchNoisyMLP(nn.Module):
    """A multi-layer perceptron with N dense layers.

    All layers (except for an optional additional extra output layer) share the same
    activation function, bias setup (use bias or not), and LayerNorm setup
    (use layer normalization or not).

    If `output_dim` (int) is not None, an additional, extra output dense layer is added,
    which might have its own activation function (e.g. "linear"). However, the output
    layer does NOT use layer normalization.
    """

    def __init__(
        self,
        *,
        input_dim: int,
        hidden_layer_dims: List[int],
        hidden_layer_activation: Union[str, Callable] = "relu",
        hidden_layer_use_bias: bool = True,
        hidden_layer_use_layernorm: bool = False,
        hidden_layer_weights_initializer: Optional[Union[str, Callable]] = None,
        hidden_layer_weights_initializer_config: Optional[Union[str, Callable]] = None,
        hidden_layer_bias_initializer: Optional[Union[str, Callable]] = None,
        hidden_layer_bias_initializer_config: Optional[Dict] = None,
        output_dim: Optional[int] = None,
        output_use_bias: bool = True,
        output_activation: Union[str, Callable] = "linear",
        output_weights_initializer: Optional[Union[str, Callable]] = None,
        output_weights_initializer_config: Optional[Dict] = None,
        output_bias_initializer: Optional[Union[str, Callable]] = None,
        output_bias_initializer_config: Optional[Dict] = None,
        std_init: Optional[float] = 0.1,
    ):
        """Initialize a TorchMLP object.

        Args:
            input_dim: The input dimension of the network. Must not be None.
            hidden_layer_dims: The sizes of the hidden layers. If an empty list, only a
                single layer will be built of size `output_dim`.
            hidden_layer_use_layernorm: Whether to insert a LayerNormalization
                functionality in between each hidden layer's output and its activation.
            hidden_layer_use_bias: Whether to use bias on all dense layers (excluding
                the possible separate output layer).
            hidden_layer_activation: The activation function to use after each layer
                (except for the output). Either a torch.nn.[activation fn] callable or
                the name thereof, or an RLlib recognized activation name,
                e.g. "ReLU", "relu", "tanh", "SiLU", or "linear".
            hidden_layer_weights_initializer: The initializer function or class to use
                forweights initialization in the hidden layers. If `None` the default
                initializer of the respective dense layer is used. Note, only the
                in-place initializers, i.e. ending with an underscore "_" are allowed.
            hidden_layer_weights_initializer_config: Configuration to pass into the
                initializer defined in `hidden_layer_weights_initializer`.
            hidden_layer_bias_initializer: The initializer function or class to use for
                bias initialization in the hidden layers. If `None` the default
                initializer of the respective dense layer is used. Note, only the
                in-place initializers, i.e. ending with an underscore "_" are allowed.
            hidden_layer_bias_initializer_config: Configuration to pass into the
                initializer defined in `hidden_layer_bias_initializer`.
            output_dim: The output dimension of the network. If None, no specific output
                layer will be added and the last layer in the stack will have
                size=`hidden_layer_dims[-1]`.
            output_use_bias: Whether to use bias on the separate output layer,
                if any.
            output_activation: The activation function to use for the output layer
                (if any). Either a torch.nn.[activation fn] callable or
                the name thereof, or an RLlib recognized activation name,
                e.g. "ReLU", "relu", "tanh", "SiLU", or "linear".
            output_layer_weights_initializer: The initializer function or class to use
                for weights initialization in the output layers. If `None` the default
                initializer of the respective dense layer is used. Note, only the
                in-place initializers, i.e. ending with an underscore "_" are allowed.
            output_layer_weights_initializer_config: Configuration to pass into the
                initializer defined in `output_layer_weights_initializer`.
            output_layer_bias_initializer: The initializer function or class to use for
                bias initialization in the output layers. If `None` the default
                initializer of the respective dense layer is used. Note, only the
                in-place initializers, i.e. ending with an underscore "_" are allowed.
            output_layer_bias_initializer_config: Configuration to pass into the
                initializer defined in `output_layer_bias_initializer`.
            std_init: Initial value of the Gaussian standard deviation before
                optimization. Defaults to `0.1`.
        """
        super().__init__()
        assert input_dim > 0

        self.input_dim = input_dim

        hidden_activation = get_activation_fn(
            hidden_layer_activation, framework="torch"
        )
        hidden_weights_initializer = get_initializer_fn(
            hidden_layer_weights_initializer, framework="torch"
        )
        hidden_bias_initializer = get_initializer_fn(
            hidden_layer_bias_initializer, framework="torch"
        )
        output_weights_initializer = get_initializer_fn(
            output_weights_initializer, framework="torch"
        )
        output_bias_initializer = get_initializer_fn(
            output_bias_initializer, framework="torch"
        )

        layers = []

        dims = (
            [self.input_dim]
            + list(hidden_layer_dims)
            + ([output_dim] if output_dim else [])
        )
        for i in range(0, len(dims) - 1):
            # Whether we are already processing the last (special) output layer.
            is_output_layer = output_dim is not None and i == len(dims) - 2

            layer = NoisyLinear(
                dims[i],
                dims[i + 1],
                bias=output_use_bias if is_output_layer else hidden_layer_use_bias,
                std_init=std_init,
            )

            # Initialize layers, if necessary.
            if is_output_layer:
                # Initialize output layer weigths if necessary.
                if output_weights_initializer:
                    output_weights_initializer(
                        layer.weight, **output_weights_initializer_config or {}
                    )
                # Initialize output layer bias if necessary.
                if output_bias_initializer:
                    output_bias_initializer(
                        layer.bias, **output_bias_initializer_config or {}
                    )
            # Must be hidden.
            else:
                # Initialize hidden layer weights if necessary.
                if hidden_layer_weights_initializer:
                    hidden_weights_initializer(
                        layer.weight, **hidden_layer_weights_initializer_config or {}
                    )
                # Initialize hidden layer bias if necessary.
                if hidden_layer_bias_initializer:
                    hidden_bias_initializer(
                        layer.bias, **hidden_layer_bias_initializer_config or {}
                    )

            layers.append(layer)

            # We are still in the hidden layer section: Possibly add layernorm and
            # hidden activation.
            if not is_output_layer:
                # Insert a layer normalization in between layer's output and
                # the activation.
                if hidden_layer_use_layernorm:
                    layers.append(nn.LayerNorm(dims[i + 1]))
                # Add the activation function.
                if hidden_activation is not None:
                    layers.append(hidden_activation())

        # Add output layer's (if any) activation.
        output_activation = get_activation_fn(output_activation, framework="torch")
        if output_dim is not None and output_activation is not None:
            layers.append(output_activation())

        self.mlp = nn.Sequential(*layers)

        self.expected_input_dtype = torch.float32

    def forward(self, x):
        return self.mlp(x.type(self.expected_input_dtype))

    def _reset_noise(self):
        # Reset the noise for all modules (layers).
        for module in self.modules():
            if hasattr(module, "reset_noise"):
                module.reset_noise()
