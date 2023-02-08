import abc
from typing import List, Optional

from ray.rllib.models.experimental.base import Model, ModelConfig
from ray.rllib.models.specs.checker import (
    check_input_specs,
    check_output_specs,
)
from ray.rllib.models.specs.checker import (
    is_input_decorated,
    is_output_decorated,
)
from ray.rllib.models.utils import get_activation_fn
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.nested_dict import NestedDict

torch, nn = try_import_torch()


def _raise_not_decorated_exception(input_or_output):
    raise ValueError(
        f"`TorchModel.forward()` not decorated with {input_or_output} specification. "
        f"Decorate it with @check_{input_or_output}_specs() to define a specification."
    )


class TorchModel(nn.Module, Model, abc.ABC):
    """Base class for RLlib's PyTorch models.

    This class defines the interface for RLlib's PyTorch models and checks
    whether inputs and outputs of forward are checked with `check_input_specs()` and
    `check_output_specs()` respectively.
    """

    def __init__(self, config: ModelConfig):
        nn.Module.__init__(self)
        Model.__init__(self, config)

        # automatically apply spec checking
        if not is_input_decorated(self.forward):
            _raise_not_decorated_exception("input")
        if not is_output_decorated(self.forward):
            _raise_not_decorated_exception("output")

    @check_input_specs("input_spec", cache=True)
    @check_output_specs("output_spec", cache=True)
    def forward(self, input_dict: NestedDict, **kwargs) -> NestedDict:
        """Returns the output of this model for the given input.

        This method only makes sure that we have a spec-checked _forward() method.

        Args:
            input_dict: The input tensors.
            **kwargs: Forward compatibility kwargs.

        Returns:
            NestedDict: The output tensors.
        """
        return self._forward(input_dict, **kwargs)


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

        hidden_activation_class = get_activation_fn(
            hidden_layer_activation, framework="torch"
        )

        output_activation_class = get_activation_fn(output_activation,
                                                    framework="torch")

        layers = []
        dims = [input_dim] + hidden_layer_dims + [output_dim]
        layers.append(nn.Linear(dims[0], dims[1]))
        for i in range(1, len(dims) - 1):
            if hidden_layer_activation != "linear":
                layers.append(hidden_activation_class())
            layers.append(nn.Linear(dims[i], dims[i + 1]))

        self.output_dim = dims[-1]

        if output_activation != "linear":
            layers.append(output_activation_class())

        self.mlp = nn.Sequential(*layers)

    def forward(self, x):
        return self.mlp(x)
