from typing import List, Optional
from typing import Tuple

from ray.rllib.models.experimental.base import Model
from ray.rllib.models.specs.checker import (
    is_input_decorated,
    is_output_decorated,
)
from ray.rllib.models.temp_spec_classes import TensorDict
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType
from ray.rllib.models.experimental.base import ModelConfig
from ray.rllib.models.utils import get_activation_fn
from ray.rllib.models.specs.checker import (
    check_input_specs,
    check_output_specs,
)

torch, nn = try_import_torch()


class TorchModel(nn.Module, Model):
    """Base class for torch models.

    This class is used to define the general interface for torch models and checks
    whether inputs and outputs are checked with `check_input_specs()` and
    `check_output_specs()` respectively.
    """

    def __init__(self, config: ModelConfig):
        nn.Module.__init__(self)
        Model.__init__(self, config)
        # automatically apply spec checking
        if not is_input_decorated(self.forward):
            self.forward = check_input_specs("input_spec", cache=True)(self.forward)
        if not is_output_decorated(self.forward):
            self.forward = check_output_specs("output_spec", cache=True)(self.forward)

    @check_input_specs("input_spec", cache=True)
    @check_output_specs("output_spec", cache=True)
    def forward(self, input_dict: TensorDict) -> Tuple[TensorDict, List[TensorType]]:
        """Returns the output of this model for the given input.

        Args:
            input_dict: The input tensors.

        Returns:
            Tuple[TensorDict, List[TensorType]]: The output tensors.
        """
        raise NotImplementedError


class TorchMLP(nn.Module):
    """A multi-layer perceptron.

    Attributes:
        input_dim: The input dimension of the network. It cannot be None.
        hidden_layer_dims: The sizes of the hidden layers.
        output_dim: The output dimension of the network. if None, the last layer would
            be the last hidden layer.
        hidden_layer_activation: The activation function to use after each layer.
        output_activation: The activation function to use for the output layer.
    """

    def __init__(
        self,
        input_dim: int,
        hidden_layer_dims: List[int],
        output_dim: Optional[int] = None,
        hidden_layer_activation: str = "linear",
        output_activation: str = "linear",
    ):
        super().__init__()
        self.input_dim = input_dim
        hidden_layer_dims = hidden_layer_dims

        activation_class = getattr(nn, hidden_layer_activation, lambda: None)()
        layers = []
        layers.append(nn.Linear(input_dim, hidden_layer_dims[0]))
        for i in range(len(hidden_layer_dims) - 1):
            if hidden_layer_activation != "linear":
                layers.append(activation_class)
            layers.append(nn.Linear(hidden_layer_dims[i], hidden_layer_dims[i + 1]))

        if output_dim is not None:
            if hidden_layer_activation != "linear":
                layers.append(activation_class)
            layers.append(nn.Linear(hidden_layer_dims[-1], output_dim))
            self.output_dim = output_dim
        else:
            self.output_dim = hidden_layer_dims[-1]

        if output_activation != "linear":
            layers.append(get_activation_fn(output_activation, framework="torch"))

        self.mlp = nn.Sequential(*layers)

    def forward(self, x):
        return self.mlp(x)
