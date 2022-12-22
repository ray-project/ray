from typing import TYPE_CHECKING
from ray.rllib.models.specs.specs_torch import TorchTensorSpec

import torch
from torch import nn

from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.torch.model import TorchModel
from ray.rllib.models.utils import get_activation_fn
from ray.rllib.utils.nested_dict import NestedDict

from ray.rllib.models.utils import input_to_output_spec

if TYPE_CHECKING:
    from ray.rllib.models.configs.encoder import VectorEncoderConfig


class TorchVectorEncoder(TorchModel):
    """A torch implementation of an MLP encoder.

    This encoder concatenates inputs along the last dimension,
    then pushes them through a series of linear layers and nonlinear activations.
    """

    @property
    def input_spec(self) -> SpecDict:
        return self._input_spec

    @property
    def output_spec(self) -> SpecDict:
        return self._output_spec

    def __init__(
        self,
        input_spec: SpecDict,
        config: "VectorEncoderConfig",
    ):
        super().__init__(config=config)
        # Setup input and output specs
        self._input_spec = input_spec
        self._output_spec = input_to_output_spec(
            input_spec=input_spec,
            num_input_feature_dims=1,
            output_key=config.output_key,
            output_feature_spec=TorchTensorSpec("f", f=config.hidden_layer_sizes[-1]),
        )
        # Returns the size of the feature dimension for the input tensors
        prev_size = sum(v.shape[-1] for v in input_spec.values())

        # Construct layers
        layers = []
        activation = (
            None
            if config.activation == "linear"
            else get_activation_fn(config.activation, framework=config.framework_str)()
        )
        for size in config.hidden_layer_sizes[:-1]:
            layers += [nn.Linear(prev_size, size)]
            layers += [activation] if activation is not None else []
            prev_size = size

        # Final layer
        layers += [
            nn.Linear(config.hidden_layer_sizes[-2], config.hidden_layer_sizes[-1])
        ]
        if config.final_activation != "linear":
            layers += [
                get_activation_fn(
                    config.final_activation, framework=config.framework_str
                )()
            ]

        self.net = nn.Sequential(*layers)

    def _forward(self, inputs: NestedDict) -> NestedDict:
        """Runs the forward pass of the MLP. Call this via unroll().

        Args:
            inputs: The nested dictionary of inputs

        Returns:
            The nested dictionary of outputs
        """
        # Ensure all inputs have matching dims before concat
        # so we can emit an informative error message
        first_key, first_tensor = list(inputs.items())[0]
        for k, tensor in inputs.items():
            assert tensor.shape[:-1] == first_tensor.shape[:-1], (
                "Inputs have mismatching dimensions, all dims but the last should "
                f"be equal: {first_key}: {first_tensor.shape} != {k}: {tensor.shape}"
            )

        # Concatenate all input along the feature dim
        x = torch.cat(list(inputs.values()), dim=-1)
        [out_key] = self.output_spec.keys()
        inputs[out_key] = self.net(x)
        return inputs
