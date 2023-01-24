from typing import List, Optional
from typing import Tuple

from ray.rllib.models.experimental.base import Model
from ray.rllib.models.specs.checker import (
    input_is_decorated,
    is_output_decorated,
)
from ray.rllib.models.temp_spec_classes import TensorDict
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType
from ray.rllib.models.experimental.base import ModelConfig

torch, nn = try_import_torch()


def _forward_not_decorated(input_or_output):
    return (
        f"forward not decorated with {input_or_output} specification. Decorate "
        f"with @check_{input_or_output}_specs() to define a specification. See "
        f"BaseModel for examples."
    )


class TorchModel(nn.Module, Model):
    """Base class for torch models.

    This class is used to define the general interface for torch models and checks
    whether inputs and outputs are checked with `check_input_specs()` and
    `check_output_specs()` respectively.
    """

    def __init__(self, config: ModelConfig):
        nn.Module.__init__(self)
        Model.__init__(self, config)
        assert input_is_decorated(self.forward), _forward_not_decorated("input")
        assert is_output_decorated(self.forward), _forward_not_decorated("output")

    def forward(self, input_dict: TensorDict) -> Tuple[TensorDict, List[TensorType]]:
        """Returns the output of this model for the given input.

        Args:
            input_dict: The input tensors.

        Returns:
            Tuple[TensorDict, List[TensorType]]: The output tensors.
        """
        raise NotImplementedError


class FCNet(nn.Module):
    """A simple fully connected network.

    Attributes:
        input_dim: The input dimension of the network. It cannot be None.
        output_dim: The output dimension of the network. if None, the last layer would
            be the last hidden layer.
        hidden_layers: The sizes of the hidden layers.
        activation: The activation function to use after each layer.
    """

    def __init__(
        self,
        input_dim: int,
        hidden_layers: List[int],
        output_dim: Optional[int] = None,
        activation: str = "linear",
    ):
        super().__init__()
        self.input_dim = input_dim
        self.hidden_layers = hidden_layers

        activation_class = getattr(nn, activation, lambda: None)()
        self.layers = []
        self.layers.append(nn.Linear(self.input_dim, self.hidden_layers[0]))
        for i in range(len(self.hidden_layers) - 1):
            if activation != "linear":
                self.layers.append(activation_class)
            self.layers.append(
                nn.Linear(self.hidden_layers[i], self.hidden_layers[i + 1])
            )

        if output_dim is not None:
            if activation != "linear":
                self.layers.append(activation_class)
            self.layers.append(nn.Linear(self.hidden_layers[-1], output_dim))

        if output_dim is None:
            self.output_dim = hidden_layers[-1]
        else:
            self.output_dim = output_dim

        self.layers = nn.Sequential(*self.layers)

    def forward(self, x):
        return self.layers(x)
